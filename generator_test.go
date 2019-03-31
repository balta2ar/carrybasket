package main

import (
	"crypto/md5"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestHashGenerator_Smoke(t *testing.T) {
	blockSize := 4
	strongHasher := md5.New()
	generator := NewHashGenerator(blockSize, strongHasher)
	generator.Reset()
}

func TestHashGenerator_Empty(t *testing.T) {
	strongHasher := md5.New()
	generator := NewHashGenerator(4, strongHasher)
	blocks := generator.Scan(strings.NewReader(""))
	assert.Empty(t, blocks)
}

func TestHashGenerator_OneFullBlock(t *testing.T) {
	strongHasher := md5.New()
	generator := NewHashGenerator(4, strongHasher)
	blocks := generator.Scan(strings.NewReader("1234"))
	assert.Len(t, blocks, 1)
	assert.Equal(t, uint64(0), blocks[0].Offset())
	assert.Equal(t, uint64(4), blocks[0].Size())
}

func TestHashGenerator_OneIncompleteBlock(t *testing.T) {
	strongHasher := md5.New()
	generator := NewHashGenerator(4, strongHasher)
	blocks := generator.Scan(strings.NewReader("12"))
	assert.Len(t, blocks, 1)
	assert.Equal(t, uint64(0), blocks[0].Offset())
	assert.Equal(t, uint64(2), blocks[0].Size())
}

func TestHashGenerator_TwoCompleteBlocks(t *testing.T) {
	strongHasher := md5.New()
	generator := NewHashGenerator(4, strongHasher)
	blocks := generator.Scan(strings.NewReader("1234abcd"))
	assert.Len(t, blocks, 2)
	assert.Equal(t, uint64(0), blocks[0].Offset())
	assert.Equal(t, uint64(4), blocks[0].Size())
	assert.Equal(t, uint64(4), blocks[1].Offset())
	assert.Equal(t, uint64(4), blocks[1].Size())
}

func TestHashGenerator_TwoBlocksLastIncomplete(t *testing.T) {
	strongHasher := md5.New()
	generator := NewHashGenerator(4, strongHasher)
	blocks := generator.Scan(strings.NewReader("1234ab"))
	assert.Len(t, blocks, 2)
	assert.Equal(t, uint64(0), blocks[0].Offset())
	assert.Equal(t, uint64(4), blocks[0].Size())
	assert.Equal(t, uint64(4), blocks[1].Offset())
	assert.Equal(t, uint64(2), blocks[1].Size())
}
