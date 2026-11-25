package s3

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

func BuildPrefetchPlan(md *format.FileMetaData, neededPaths []string) (*PrefetchPlan, error) {
	if md == nil {
		return nil, fmt.Errorf("prefetch plan: nil file metadata")
	}
	if len(neededPaths) == 0 {
		return nil, fmt.Errorf("prefetch plan: empty projection")
	}

	neededByPath := make(map[string]string, len(neededPaths))
	neededByNorm := make(map[string]string, len(neededPaths))
	for _, p := range neededPaths {
		neededByPath[p] = p
		if _, ok := neededByNorm[normalizeLeafPath(p)]; !ok {
			neededByNorm[normalizeLeafPath(p)] = p
		}
	}
	matched := make(map[string]struct{}, len(neededPaths))

	groups := make([]PrefetchGroup, 0, len(md.RowGroups))
	for rgi := range md.RowGroups {
		rg := &md.RowGroups[rgi]
		spans := make([]Span, 0, len(neededPaths))
		for ci := range rg.Columns {
			cmd := &rg.Columns[ci].MetaData
			path := strings.Join(cmd.PathInSchema, ".")

			key, ok := neededByPath[path]
			if !ok {
				if nk, found := neededByNorm[normalizeLeafPath(path)]; found {
					key, ok = nk, true
				}
			}
			if !ok {
				continue
			}
			matched[key] = struct{}{}

			sp := columnChunkSpan(cmd)
			if sp.Len <= 0 {
				return nil, fmt.Errorf("prefetch plan: column %q has no data pages", path)
			}
			spans = append(spans, sp)
		}
		if len(spans) == 0 {
			continue
		}
		groups = append(groups, PrefetchGroup{Spans: spans})
	}

	if len(matched) != len(neededPaths) {
		missing := make([]string, 0, len(neededPaths)-len(matched))
		for _, p := range neededPaths {
			if _, ok := matched[p]; !ok {
				missing = append(missing, p)
			}
		}
		return nil, fmt.Errorf("prefetch plan: %d projected column(s) not found in file: %s",
			len(missing), strings.Join(missing, ", "))
	}
	return NewPrefetchPlan(groups)
}

func columnChunkSpan(md *format.ColumnMetaData) Span {
	start := md.DataPageOffset
	if md.DictionaryPageOffset > 0 && md.DictionaryPageOffset < start {
		start = md.DictionaryPageOffset
	}
	return Span{Off: start, Len: md.TotalCompressedSize}
}

func normalizeLeafPath(path string) string {
	if !strings.Contains(path, ".list.") && !strings.Contains(path, ".element.") {
		return path
	}
	parts := strings.Split(path, ".")
	out := parts[:0]
	for _, p := range parts {
		if p == "list" || p == "element" {
			continue
		}
		out = append(out, p)
	}
	return strings.Join(out, ".")
}

func leafPathsOf(model any) []string {
	t := reflect.TypeOf(model)
	if v, ok := leafPathCache.Load(t); ok {
		return v.([]string)
	}
	schema := parquet.SchemaOf(model)
	cols := schema.Columns()
	paths := make([]string, 0, len(cols))
	for _, c := range cols {
		paths = append(paths, strings.Join(c, "."))
	}
	leafPathCache.Store(t, paths)
	return paths
}

func BuildPrefetchPlanFor(md *format.FileMetaData, model any) (*PrefetchPlan, error) {
	return BuildPrefetchPlan(md, leafPathsOf(model))
}

var leafPathCache sync.Map
