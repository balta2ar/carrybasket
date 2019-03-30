package main

import (
	"crypto/md5"
	"github.com/stretchr/testify/assert"
	"hash/adler32"
	"strings"
	"testing"
)

func makeEmptyBlockProducer(blockSize int) BlockProducer {
	fastHash := NewMackerras(blockSize)
	strongHash := md5.New()
	fastCache := NewBlockCache()
	strongCache := NewBlockCache()
	return NewBlockProducer(blockSize, fastHash, strongHash, fastCache, strongCache)
}

func TestBlockProducer_Smoke(t *testing.T) {
	blockSize := 1
	fastHash := adler32.New()
	strongHash := md5.New()
	fastCache := NewBlockCache()
	strongCache := NewBlockCache()
	producer := NewBlockProducer(blockSize, fastHash, strongHash, fastCache, strongCache)

	r1 := NewStackedReadSeeker(strings.NewReader("abc"))
	result := producer.Scan(r1)
	assert.NotEmpty(t, result)

	r2 := NewStackedReadSeeker(strings.NewReader(""))
	producer.Reset()
	result = producer.Scan(r2)
	assert.Empty(t, result)
}

func TestBlockProducer_EmitOneByteOfContent(t *testing.T) {
	producer := makeEmptyBlockProducer(1)
	r := NewStackedReadSeeker(strings.NewReader("a"))
	result := producer.Scan(r)
	assert.Len(t, result, 1)
	block := result[0].(ContentBlock)
	assert.Equal(t, []byte("a"), block.Content())
}

func TestBlockProducer_EmitSeveralBytesOfContentBlockOfOneByte(t *testing.T) {
	producer := makeEmptyBlockProducer(1)
	r := NewStackedReadSeeker(strings.NewReader("abc"))
	result := producer.Scan(r)
	assert.Len(t, result, 1)
	block := result[0].(ContentBlock)
	assert.Equal(t, []byte("abc"), block.Content())
}

func TestBlockProducer_EmitSeveralBytesOfContentBlockOfFourBytes(t *testing.T) {
	producer := makeEmptyBlockProducer(4)
	r := NewStackedReadSeeker(strings.NewReader("abcdefgh"))
	result := producer.Scan(r)
	assert.Len(t, result, 1)
	block := result[0].(ContentBlock)
	assert.Equal(t, []byte("abcdefgh"), block.Content())
}

func TestBlockProducer_EmitOneHashed(t *testing.T) {
	blockSize := 4
	fastHash := NewMackerras(blockSize)
	strongHash := md5.New()
	fastCache := NewBlockCache()
	strongCache := NewBlockCache()
	producer := NewBlockProducer(blockSize, fastHash, strongHash, fastCache, strongCache)

	// prepare caches
	_, _ = fastHash.Write([]byte("abcd"))
	fastChecksum := fastHash.Sum(nil)
	// the offset for the server differs from the offset for the client, but the size should match
	fastCache.Set(fastChecksum, NewHashedBlock(100, 4, fastChecksum))
	strongHash.Write([]byte("abcd"))
	strongChecksum := strongHash.Sum(nil)
	strongCache.Set(strongChecksum, NewHashedBlock(100, 4, strongChecksum))

	// reset hashing devices
	fastHash.Reset()
	strongHash.Reset()

	// scan
	r := NewStackedReadSeeker(strings.NewReader("abcd"))
	result := producer.Scan(r)
	assert.Len(t, result, 1)
	block := result[0].(HashedBlock)
	assert.Equal(t, strongChecksum, block.HashSum())
	assert.Equal(t, uint64(0), block.Offset())
	assert.Equal(t, uint64(4), block.Size())
}

func TestBlockProducer_EmitTwoHashedSameSize(t *testing.T) {
	blockSize := 4
	fastHash := NewMackerras(blockSize)
	strongHash := md5.New()
	fastCache := NewBlockCache()
	strongCache := NewBlockCache()
	producer := NewBlockProducer(blockSize, fastHash, strongHash, fastCache, strongCache)

	// prepare caches
	_, _ = fastHash.Write([]byte("abcd"))
	fastCache.Set(fastHash.Sum(nil), NewHashedBlock(100, 4, fastHash.Sum(nil)))
	_, _ = fastHash.Write([]byte("1234"))
	fastCache.Set(fastHash.Sum(nil), NewHashedBlock(200, 4, fastHash.Sum(nil)))

	strongHash.Write([]byte("abcd"))
	strongChecksum1 := strongHash.Sum(nil)
	strongCache.Set(strongChecksum1, NewHashedBlock(300, 4, strongChecksum1))
	strongHash.Write([]byte("1234"))
	strongChecksum2 := strongHash.Sum(nil)
	strongCache.Set(strongChecksum2, NewHashedBlock(400, 4, strongChecksum2))

	// reset hashing devices
	fastHash.Reset()
	strongHash.Reset()

	// scan
	r := NewStackedReadSeeker(strings.NewReader("abcd1234"))
	result := producer.Scan(r)
	assert.Len(t, result, 2)
	block1 := result[0].(HashedBlock)
	block2 := result[1].(HashedBlock)
	assert.Equal(t, strongChecksum1, block1.HashSum())
	assert.Equal(t, uint64(0), block1.Offset())
	assert.Equal(t, uint64(4), block1.Size())
	assert.Equal(t, strongChecksum2, block2.HashSum())
	assert.Equal(t, uint64(4), block2.Offset())
	assert.Equal(t, uint64(4), block2.Size())
}

func TestBlockProducer_EmitTwoHashedSecondIsSmaller(t *testing.T) {
	blockSize := 4
	fastHash := NewMackerras(blockSize)
	strongHash := md5.New()
	fastCache := NewBlockCache()
	strongCache := NewBlockCache()
	producer := NewBlockProducer(blockSize, fastHash, strongHash, fastCache, strongCache)

	// prepare caches
	_, _ = fastHash.Write([]byte("abcd"))
	fastCache.Set(fastHash.Sum(nil), NewHashedBlock(100, 4, fastHash.Sum(nil)))
	_, _ = fastHash.Write([]byte("123"))
	fastCache.Set(fastHash.Sum(nil), NewHashedBlock(200, 4, fastHash.Sum(nil)))

	strongHash.Write([]byte("abcd"))
	strongChecksum1 := strongHash.Sum(nil)
	strongCache.Set(strongChecksum1, NewHashedBlock(300, 4, strongChecksum1))
	strongHash.Write([]byte("123"))
	strongChecksum2 := strongHash.Sum(nil)
	strongCache.Set(strongChecksum2, NewHashedBlock(400, 4, strongChecksum2))

	// reset hashing devices
	fastHash.Reset()
	strongHash.Reset()

	// scan
	r := NewStackedReadSeeker(strings.NewReader("abcd123"))
	result := producer.Scan(r)
	assert.Len(t, result, 2)
	block1 := result[0].(HashedBlock)
	block2 := result[1].(HashedBlock)
	assert.Equal(t, strongChecksum1, block1.HashSum())
	assert.Equal(t, uint64(0), block1.Offset())
	assert.Equal(t, uint64(4), block1.Size())
	assert.Equal(t, strongChecksum2, block2.HashSum())
	assert.Equal(t, uint64(4), block2.Offset())
	assert.Equal(t, uint64(3), block2.Size())
}