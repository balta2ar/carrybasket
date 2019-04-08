package carrybasket

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

func TestBlockCache_AddHashes(t *testing.T) {
	cache := NewBlockCache()

	cacheBlock1, ok := cache.Get([]byte("hash"))
	assert.Equal(t, cacheBlock1, nil)
	assert.False(t, ok)

	hashedBlock := NewHashedBlock(0, 4, []byte("hash"))
	cache.AddHashes([]Block{hashedBlock})
	cacheBlock2, ok := cache.Get([]byte("hash"))
	assert.Equal(t, 1, cache.Len())
	assert.Equal(t, cacheBlock2, hashedBlock)
	assert.True(t, ok)
}

func TestBlockCache_AddContents(t *testing.T) {
	cache := NewBlockCache()

	cacheBlock1, ok := cache.Get([]byte("hash"))
	assert.Equal(t, cacheBlock1, nil)
	assert.False(t, ok)

	hashedBlock := NewHashedBlock(0, 7, []byte("hash"))
	contentBlock := NewContentBlock(0, 7, []byte("content"))
	cache.AddContents([]Block{hashedBlock}, []Block{contentBlock})
	cacheBlock2, ok := cache.Get([]byte("hash"))
	assert.Equal(t, 1, cache.Len())
	assert.Equal(t, cacheBlock2, contentBlock)
	assert.True(t, ok)
}
