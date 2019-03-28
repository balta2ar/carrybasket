package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBlockCache_GetAndSet(t *testing.T) {
	cache := NewBlockCache()
	block1 := NewContentBlock(1, 2, []byte("content"))
	block2 := NewHashedBlock(3, 4, []byte("hash"))

	assert.Equal(t, cache.Len(), 0)

	cache.Set([]byte("first"), block1)
	assert.Equal(t, cache.Len(), 1)

	cache.Set([]byte("second"), block2)
	assert.Equal(t, cache.Len(), 2)

	first, ok := cache.Get([]byte("first"))
	assert.True(t, ok)
	assert.Equal(t, first.Offset(), block1.Offset())
	assert.Equal(t, first.Size(), block1.Size())
	assert.Equal(t, first.(ContentBlock).Content(), ContentBlock(block1).Content())

	second, ok := cache.Get([]byte("second"))
	assert.True(t, ok)
	assert.Equal(t, second.Offset(), block2.Offset())
	assert.Equal(t, second.Size(), block2.Size())
	assert.Equal(t, second.(HashedBlock).HashSum(), HashedBlock(block2).HashSum())

	third, ok := cache.Get([]byte("third"))
	assert.False(t, ok)
	assert.Equal(t, third, nil)
}
