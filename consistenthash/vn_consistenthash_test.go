package consistenthash

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"strconv"
	"testing"
)

const (
	keySize     = 20
	requestSize = 1000
)

func TestVNConsistentHash(t *testing.T) {
	ch := NewCustomVNConsistentHash(0, nil)
	val, ok := ch.Get("any")
	assert.False(t, ok)
	assert.Empty(t, val)

	for i := 0; i < keySize; i++ {
		ch.AddWithReplicas("localhost:"+strconv.Itoa(i), minReplicas<<1)
	}

	keys := make(map[string]int)
	for i := 0; i < requestSize; i++ {
		key, ok := ch.Get(fmt.Sprintf(`%d`, requestSize+i))
		assert.True(t, ok)
		keys[key]++
	}

	mi := make(map[string]int, len(keys))
	for k, v := range keys {
		mi[k] = v
	}
	entropy := CalcEntropy(mi)
	assert.True(t, entropy > .95)
}

func TestConsistentHash_Remove(t *testing.T) {
	ch := NewVNConsistentHash()
	ch.Add("first")
	ch.Add("second")
	ch.Remove("first")
	for i := 0; i < 100; i++ {
		val, ok := ch.Get(strconv.Itoa(i))
		assert.True(t, ok)
		assert.Equal(t, "second", val)
	}
}

const epsilon = 1e-6

// CalcEntropy calculates the entropy of m.
func CalcEntropy(m map[string]int) float64 {
	if len(m) == 0 || len(m) == 1 {
		return 1
	}

	var entropy float64
	var total int
	for _, v := range m {
		total += v
	}

	for _, v := range m {
		proba := float64(v) / float64(total)
		if proba < epsilon {
			proba = epsilon
		}
		entropy -= proba * math.Log2(proba)
	}

	return entropy / math.Log2(float64(len(m)))
}
