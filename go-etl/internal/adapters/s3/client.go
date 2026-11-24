package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	DefaultBucket = "openalex"
	DefaultRegion = "us-east-1"

	ParquetPrefix = "data/parquet"
	JSONLPrefix   = "data/jsonl"
)

type Client struct {
	api    *awss3.Client
	bucket string
}

type Option func(*clientConfig)

type clientConfig struct {
	bucket     string
	region     string
	httpClient *http.Client
}

func WithBucket(bucket string) Option {
	return func(c *clientConfig) {
		if bucket != "" {
			c.bucket = bucket
		}
	}
}

func WithRegion(region string) Option {
	return func(c *clientConfig) {
		if region != "" {
			c.region = region
		}
	}
}

func WithHTTPClient(hc *http.Client) Option {
	return func(c *clientConfig) {
		if hc != nil {
			c.httpClient = hc
		}
	}
}

func NewAnonymousClient(ctx context.Context, opts ...Option) (*Client, error) {
	cc := clientConfig{
		bucket:     DefaultBucket,
		region:     DefaultRegion,
		httpClient: newTunedHTTPClient(),
	}
	for _, opt := range opts {
		opt(&cc)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cc.region),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
		config.WithSharedConfigFiles([]string{}),
		config.WithSharedCredentialsFiles([]string{}),
		config.WithHTTPClient(cc.httpClient),
		config.WithRetryMode(aws.RetryModeStandard),
		config.WithRetryMaxAttempts(5),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	)
	if err != nil {
		return nil, fmt.Errorf("load anonymous aws config: %w", err)
	}

	return &Client{
		api:    awss3.NewFromConfig(awsCfg),
		bucket: cc.bucket,
	}, nil
}

func newTunedHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	}
	return &http.Client{Transport: transport}
}

func (c *Client) Bucket() string {
	return c.bucket
}

func (c *Client) API() *awss3.Client {
	return c.api
}

func (c *Client) GetObject(ctx context.Context, bucket, key string) (*awss3.GetObjectOutput, error) {
	if bucket == "" {
		bucket = c.bucket
	}
	out, err := c.api.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get s3://%s/%s: %w", bucket, key, err)
	}
	return out, nil
}

func (c *Client) ReadAll(ctx context.Context, bucket, key string, maxBytes int64) ([]byte, error) {
	if bucket == "" {
		bucket = c.bucket
	}
	out, err := c.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = out.Body.Close()
	}()

	limit := int64(math.MaxInt64)
	if maxBytes > 0 {
		if out.ContentLength != nil && *out.ContentLength > maxBytes {
			return nil, fmt.Errorf("object s3://%s/%s size %d exceeds limit %d bytes", bucket, key, *out.ContentLength, maxBytes)
		}
		limit = maxBytes
	}

	var buf bytes.Buffer
	if out.ContentLength != nil && *out.ContentLength > 0 {
		capHint := min(*out.ContentLength, limit)
		buf.Grow(int(capHint) + bytes.MinRead)
	}

	r := io.Reader(out.Body)
	if maxBytes > 0 {
		r = io.LimitReader(out.Body, maxBytes+1)
	}

	n, err := buf.ReadFrom(r)
	if err != nil {
		return nil, fmt.Errorf("read s3://%s/%s: %w", bucket, key, err)
	}
	if maxBytes > 0 && n > maxBytes {
		return nil, fmt.Errorf("object s3://%s/%s exceeds limit %d bytes", bucket, key, maxBytes)
	}

	return buf.Bytes(), nil
}

func (c *Client) ListKeys(ctx context.Context, prefix string, maxKeys int32) ([]string, error) {
	if maxKeys <= 0 {
		maxKeys = 20
	}
	out, err := c.api.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:  aws.String(c.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(maxKeys),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 list s3://%s/%s: %w", c.bucket, prefix, err)
	}

	keys := make([]string, 0, len(out.Contents))
	for _, obj := range out.Contents {
		if obj.Key != nil {
			keys = append(keys, *obj.Key)
		}
	}
	return keys, nil
}

func ParseS3URL(raw string) (bucket, key string, err error) {
	rest, ok := strings.CutPrefix(raw, "s3://")
	if !ok {
		return "", "", fmt.Errorf("invalid s3 url %q: missing s3:// prefix", raw)
	}
	bucket, key, ok = strings.Cut(rest, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", errors.New("invalid s3 url: expected s3://<bucket>/<key>")
	}
	key = strings.TrimLeft(key, "/")
	if key == "" {
		return "", "", errors.New("invalid s3 url: empty object key")
	}
	return bucket, key, nil
}
