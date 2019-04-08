package carrybasket

import (
	"crypto/md5"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestHashGenerator_Smoke(t *testing.T) {
	blockSize := 4
	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()
	generator := NewHashGenerator(blockSize, fastHasher, strongHasher)
	generator.Reset()
}

func TestHashGenerator_Empty(t *testing.T) {
	blockSize := 4
	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()
	generator := NewHashGenerator(blockSize, fastHasher, strongHasher)

	result := generator.Scan(strings.NewReader(""))
	assert.Empty(t, result.fastHashes)
	assert.Empty(t, result.strongHashes)
	assert.Empty(t, result.contentBlocks)
}

func TestHashGenerator_OneFullBlock(t *testing.T) {
	blockSize := 4
	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()
	generator := NewHashGenerator(blockSize, fastHasher, strongHasher)
	result := generator.Scan(strings.NewReader("1234"))
	assert.Len(t, result.fastHashes, 1)
	assert.Len(t, result.strongHashes, 1)
	assert.Len(t, result.contentBlocks, 1)
	assert.Equal(t, uint64(0), result.contentBlocks[0].Offset())
	assert.Equal(t, uint64(4), result.contentBlocks[0].Size())
	assert.Len(t, result.contentBlocks[0].(ContentBlock).Content(), 4)
}

func TestHashGenerator_OneIncompleteBlock(t *testing.T) {
	blockSize := 4
	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()
	generator := NewHashGenerator(4, fastHasher, strongHasher)
	result := generator.Scan(strings.NewReader("12"))
	assert.Len(t, result.fastHashes, 1)
	assert.Len(t, result.strongHashes, 1)
	assert.Len(t, result.contentBlocks, 1)
	assert.Equal(t, uint64(0), result.contentBlocks[0].Offset())
	assert.Equal(t, uint64(2), result.contentBlocks[0].Size())
	assert.Len(t, result.contentBlocks[0].(ContentBlock).Content(), 2)
}

func TestHashGenerator_TwoCompleteBlocks(t *testing.T) {
	blockSize := 4
	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()
	generator := NewHashGenerator(4, fastHasher, strongHasher)
	result := generator.Scan(strings.NewReader("1234abcd"))
	assert.Len(t, result.fastHashes, 2)
	assert.Len(t, result.strongHashes, 2)
	assert.Len(t, result.contentBlocks, 2)
	assert.Equal(t, uint64(0), result.contentBlocks[0].Offset())
	assert.Equal(t, uint64(4), result.contentBlocks[0].Size())
	assert.Len(t, result.contentBlocks[0].(ContentBlock).Content(), 4)
	assert.Equal(t, uint64(4), result.contentBlocks[1].Offset())
	assert.Equal(t, uint64(4), result.contentBlocks[1].Size())
	assert.Len(t, result.contentBlocks[1].(ContentBlock).Content(), 4)
}

func TestHashGenerator_TwoBlocksLastIncomplete(t *testing.T) {
	blockSize := 4
	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()
	generator := NewHashGenerator(4, fastHasher, strongHasher)
	result := generator.Scan(strings.NewReader("1234ab"))
	assert.Len(t, result.fastHashes, 2)
	assert.Len(t, result.strongHashes, 2)
	assert.Len(t, result.contentBlocks, 2)
	assert.Equal(t, uint64(0), result.contentBlocks[0].Offset())
	assert.Equal(t, uint64(4), result.contentBlocks[0].Size())
	assert.Len(t, result.contentBlocks[0].(ContentBlock).Content(), 4)
	assert.Equal(t, uint64(4), result.contentBlocks[1].Offset())
	assert.Equal(t, uint64(2), result.contentBlocks[1].Size())
	assert.Len(t, result.contentBlocks[1].(ContentBlock).Content(), 2)
}
