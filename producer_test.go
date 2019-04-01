package main

import (
	"crypto/md5"
	"fmt"
	"github.com/stretchr/testify/assert"
	"hash"
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
	fastHash := NewMackerras(blockSize)
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
	assert.Equal(t, uint64(3), block.Size())
	assert.Equal(t, []byte("abc"), block.Content())
}

func TestBlockProducer_EmitSeveralBytesOfContentBlockOfFourBytes(t *testing.T) {
	producer := makeEmptyBlockProducer(4)
	r := NewStackedReadSeeker(strings.NewReader("abcdefgh"))
	result := producer.Scan(r)
	assert.Len(t, result, 1)
	block := result[0].(ContentBlock)
	assert.Equal(t, uint64(8), block.Size())
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
	strongHash.Reset()
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
	fastHash.Reset()
	_, _ = fastHash.Write([]byte("123"))
	fastCache.Set(fastHash.Sum(nil), NewHashedBlock(200, 3, fastHash.Sum(nil)))

	strongHash.Write([]byte("abcd"))
	strongChecksum1 := strongHash.Sum(nil)
	strongCache.Set(strongChecksum1, NewHashedBlock(300, 4, strongChecksum1))
	strongHash.Reset()
	strongHash.Write([]byte("123"))
	strongChecksum2 := strongHash.Sum(nil)
	strongCache.Set(strongChecksum2, NewHashedBlock(400, 3, strongChecksum2))

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

type blockProducerTestStand struct {
	fastHash    hash.Hash32
	strongHash  hash.Hash
	fastCache   BlockCache
	strongCache BlockCache

	inputBlocks []Block
	offset      uint64
}

type chunk struct {
	fn    func(string)
	value string
}

func (ts *blockProducerTestStand) reset(blockSize int) {
	ts.fastHash = NewMackerras(blockSize)
	ts.strongHash = md5.New()
	ts.fastCache = NewBlockCache()
	ts.strongCache = NewBlockCache()

	ts.inputBlocks = make([]Block, 0)
	ts.offset = 0
}

func (ts *blockProducerTestStand) resetHashes() {
	ts.fastHash.Reset()
	ts.strongHash.Reset()
}

func (ts *blockProducerTestStand) addHash(value string) {
	O, L := ts.offset, uint64(len(value))

	ts.fastHash.Reset()
	_, _ = ts.fastHash.Write([]byte(value))
	ts.fastCache.Set(ts.fastHash.Sum(nil), NewHashedBlock(O, L, ts.fastHash.Sum(nil)))

	ts.strongHash.Reset()
	_, _ = ts.strongHash.Write([]byte(value))
	ts.strongCache.Set(ts.strongHash.Sum(nil), NewHashedBlock(O, L, ts.strongHash.Sum(nil)))

	ts.inputBlocks = append(ts.inputBlocks, NewHashedBlock(O, L, ts.strongHash.Sum(nil)))
	ts.offset += L
}

func (ts *blockProducerTestStand) addContent(value string) {
	O, L := ts.offset, uint64(len(value))

	ts.inputBlocks = append(ts.inputBlocks, NewContentBlock(O, L, []byte(value)))
	ts.offset += L
}

func (ts *blockProducerTestStand) verify(t *testing.T, blocks []Block) {
	assert.Len(t, blocks, len(ts.inputBlocks))
	for i, block := range ts.inputBlocks {
		assert.Equal(t, block.Offset(), blocks[i].Offset())
		assert.Equal(t, block.Size(), blocks[i].Size())
		assert.IsType(t, block, blocks[i])
		switch inputBlock := block.(type) {
		case HashedBlock:
			actualBlock := blocks[i].(HashedBlock)
			assert.Equal(t, inputBlock.HashSum(), actualBlock.HashSum())
		case ContentBlock:
			actualBlock := blocks[i].(ContentBlock)
			assert.Equal(t, inputBlock.Content(), actualBlock.Content())
		}
	}
}

func TestBlockProducer_MultipleTestCases(t *testing.T) {
	stand := blockProducerTestStand{
		nil, nil, nil, nil,

		make([]Block, 0),
		0,
	}

	testCases := []struct {
		blockSize int
		input     string
		chunks    []chunk
	}{
		{
			4, "",
			[]chunk{},
		},
		{
			4, "1234",
			[]chunk{
				{stand.addContent, "1234"},
			},
		},
		{
			4, "1234",
			[]chunk{
				{stand.addHash, "1234"},
			},
		},
		{
			4, "123abcd",
			[]chunk{
				{stand.addContent, "123"},
				{stand.addHash, "abcd"},
			},
		},
		{
			4, "abcd123",
			[]chunk{
				{stand.addHash, "abcd"},
				{stand.addContent, "123"},
			},
		},
		{
			4, "123abcd987",
			[]chunk{
				{stand.addContent, "123"},
				{stand.addHash, "abcd"},
				{stand.addContent, "987"},
			},
		},
		{
			4, "1234abcd987",
			[]chunk{
				{stand.addHash, "1234"},
				{stand.addHash, "abcd"},
				{stand.addHash, "987"},
			},
		},
		{
			4, "1234*abcd",
			[]chunk{
				{stand.addHash, "1234"},
				{stand.addContent, "*"},
				{stand.addHash, "abcd"},
			},
		},
		{
			4, "1234****abcd",
			[]chunk{
				{stand.addContent, "1234"},
				{stand.addHash, "****"},
				{stand.addContent, "abcd"},
			},
		},
	}

	for i, tt := range testCases {
		fmt.Printf("\n***** Running test case %v *****\n", i)

		stand.reset(tt.blockSize)
		producer := NewBlockProducer(
			tt.blockSize,
			stand.fastHash,
			stand.strongHash,
			stand.fastCache,
			stand.strongCache,
		)

		for _, chunk := range tt.chunks {
			chunk.fn(chunk.value)
		}
		stand.resetHashes()

		r := NewStackedReadSeeker(strings.NewReader(tt.input))
		blocks := producer.Scan(r)
		stand.verify(t, blocks)
	}
}

func TestProducerFactory_Smoke(t *testing.T) {
	blockSize := 4
	factory := NewProducerFactory(blockSize)
	producer := factory.MakeProducer(nil, nil)
	assert.NotNil(t, producer)
}

func TestProducerFactory_WithoutCaches(t *testing.T) {
	blockSize := 4
	factory := NewProducerFactory(blockSize)
	producer := factory.MakeProducer(nil, nil)
	assert.NotNil(t, producer)

	r := NewStackedReadSeeker(strings.NewReader("abcd"))
	blocks := producer.Scan(r)
	// the factory was not given blocks so the caches were empty,
	// thus it produced content block
	assert.Len(t, blocks, 1)
	assert.Equal(t, []byte("abcd"), blocks[0].(ContentBlock).Content())
}

func TestProducerFactory_WithCaches(t *testing.T) {
	blockSize := 4

	fastHash := NewMackerras(blockSize)
	_, _ = fastHash.Write([]byte("abcd"))
	fastChecksum := fastHash.Sum(nil)

	strongHash := md5.New()
	strongHash.Write([]byte("abcd"))
	strongChecksum := strongHash.Sum(nil)

	factory := NewProducerFactory(blockSize)
	producer := factory.MakeProducer(
		[]Block{NewHashedBlock(0, 4, fastChecksum)},
		[]Block{NewHashedBlock(0, 4, strongChecksum)},
	)
	assert.NotNil(t, producer)

	r := NewStackedReadSeeker(strings.NewReader("abcd"))
	blocks := producer.Scan(r)
	// caches were pre-filled, so producer made a hash block
	assert.Len(t, blocks, 1)
	assert.Equal(t, strongChecksum, blocks[0].(HashedBlock).HashSum())
}
