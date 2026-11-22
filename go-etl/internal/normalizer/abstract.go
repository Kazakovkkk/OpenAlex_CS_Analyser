package normalizer

import (
	"bytes"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/bytedance/sonic"
)

const (
	MaxAbstractBytes     = 32 << 10
	maxAbstractPositions = MaxAbstractBytes / 2

	defaultWordsPoolCap = 512
	maxPooledWordsCap   = 4096
	maxPooledMapKeys    = 1024
)

var (
	abstractWordsPool = sync.Pool{
		New: func() any {
			s := make([]string, 0, defaultWordsPoolCap)
			return &s
		},
	}

	abstractMapPool = sync.Pool{
		New: func() any {
			m := make(map[string][]int, 256)
			return &m
		},
	}
)

func ReconstructAbstractJSON(raw []byte) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) <= 2 || bytes.Equal(raw, []byte("null")) {
		return ""
	}

	mapPtr := abstractMapPool.Get().(*map[string][]int)
	inv := *mapPtr
	defer func() {
		keyCount := len(inv)
		clear(inv)
		if keyCount <= maxPooledMapKeys {
			abstractMapPool.Put(mapPtr)
		}
	}()

	if err := sonic.Unmarshal(raw, &inv); err != nil {
		return ""
	}
	return ReconstructAbstract(inv)
}

func ReconstructAbstract(inv map[string][]int) string {
	if len(inv) == 0 {
		return ""
	}

	maxPos := -1
	totalBytes := 0
	for word, positions := range inv {
		if len(word) == 0 {
			continue
		}
		wordLen := len(word)
		for _, pos := range positions {
			if pos < 0 || pos >= maxAbstractPositions {
				continue
			}
			if pos > maxPos {
				maxPos = pos
			}
			totalBytes += wordLen + 1
		}
	}
	if maxPos < 0 {
		return ""
	}

	needed := maxPos + 1

	wordsPtr := abstractWordsPool.Get().(*[]string)
	words := *wordsPtr
	if cap(words) < needed {
		words = make([]string, needed)
	} else {
		words = words[:needed]
	}
	defer func() {
		clear(words)
		if cap(words) <= maxPooledWordsCap {
			*wordsPtr = words[:0]
			abstractWordsPool.Put(wordsPtr)
		}
	}()

	for word, positions := range inv {
		if len(word) == 0 {
			continue
		}
		for _, pos := range positions {
			if pos >= 0 && pos < needed {
				words[pos] = word
			}
		}
	}

	var sb strings.Builder
	sb.Grow(min(totalBytes, MaxAbstractBytes))

	for _, w := range words {
		if len(w) == 0 {
			continue
		}
		needSpace := sb.Len() > 0
		addLen := len(w)
		if needSpace {
			addLen++
		}
		if sb.Len()+addLen > MaxAbstractBytes {
			if needSpace {
				if sb.Len()+1 >= MaxAbstractBytes {
					break
				}
				sb.WriteByte(' ')
			}
			rem := MaxAbstractBytes - sb.Len()
			sb.WriteString(truncateUTF8Bytes(w, rem))
			break
		}
		if needSpace {
			sb.WriteByte(' ')
		}
		sb.WriteString(w)
	}

	return sb.String()
}

func truncateUTF8Bytes(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
