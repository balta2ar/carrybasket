package main

import (
	"crypto/md5"
	"fmt"
	"github.com/stretchr/testify/assert"
	"hash"
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

//type thash struct{ value string }
//type tcontent struct{ value string }
//type toutput struct {
//	offset  uint64
//	size    uint64
//	tcontent string
//	//hash    []byte
//}
//
//type tinputs []interface{}

type blockProducerTestStand struct {
	fastHash    hash.Hash
	strongHash  hash.Hash
	fastCache   BlockCache
	strongCache BlockCache

	inputBlocks []Block
	offset      uint64
	//fastChecksums   [][]byte
	//strongChecksums [][]byte
}

func (ts *blockProducerTestStand) addHash(value string) {
	O, L := ts.offset, uint64(len(value))

	ts.fastHash.Reset()
	_, _ = ts.fastHash.Write([]byte(value))
	ts.fastCache.Set(ts.fastHash.Sum(nil), NewHashedBlock(O, L, ts.fastHash.Sum(nil)))
	//ts.fastChecksums = append(ts.fastChecksums, ts.fastHash.Sum(nil))

	ts.strongHash.Reset()
	_, _ = ts.strongHash.Write([]byte(value))
	ts.strongCache.Set(ts.strongHash.Sum(nil), NewHashedBlock(O, L, ts.strongHash.Sum(nil)))
	//ts.strongChecksums = append(ts.strongChecksums, ts.strongHash.Sum(nil))

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
			fmt.Printf("hashed block %v\n", actualBlock)
			assert.Equal(t, inputBlock.HashSum(), actualBlock.HashSum())
		case ContentBlock:
			actualBlock := blocks[i].(ContentBlock)
			fmt.Printf("content block %v\n", actualBlock)
			assert.Equal(t, inputBlock.Content(), actualBlock.Content())
		}
	}
}

func TestBlockProducer_MultipleTestCases(t *testing.T) {
	blockSize := 4
	fastHash := NewMackerras(blockSize)
	strongHash := md5.New()
	fastCache := NewBlockCache()
	strongCache := NewBlockCache()
	producer := NewBlockProducer(blockSize, fastHash, strongHash, fastCache, strongCache)

	stand := blockProducerTestStand{
		fastHash,
		strongHash,
		fastCache,
		strongCache,

		make([]Block, 0),
		0,
		//make([][]byte, 0),
		//make([][]byte, 0),
	}

	//
	//// describe input tests
	//var testInputs = []struct {
	//	blocks tinputs // []interface{}
	//}{
	//	{tinputs{thash{"abcd"}, thash{"1234"}}},
	//	{tinputs{thash{"abcd"}, thash{"123"}}},
	//}
	//
	//var expectedOutputs [][]toutput
	//var fastHashes [][]byte
	//var strongHashes[][]byte
	//
	//// convert input tests to expected toutput
	//var offset uint64 = 0
	//for _, testInput := range testInputs {
	//	var testOutput []toutput
	//	for _, input := range testInput.blocks {
	//		switch testBlock := input.(type) {
	//		case thash:
	//			O, L := uint64(offset), uint64(len(testBlock.value))
	//			testOutput = append(testOutput, toutput{
	//				O, L, testBlock.value,
	//			})
	//			fastHash.Reset()
	//			_,_ = fastHash.Write([]byte(testBlock.value))
	//			fastCache.Set(fastHash.Sum(nil), NewHashedBlock(O, L, fastHash.Sum(nil)))
	//			fastHashes = append(fastHashes, fastHash.Sum(nil))
	//
	//			strongHash.Reset()
	//			_,_ = strongHash.Write([]byte(testBlock.value))
	//			strongCache.Set(strongHash.Sum(nil), NewHashedBlock(O, L, strongHash.Sum(nil)))
	//			strongHashes = append(strongHashes, strongHash.Sum(nil))
	//			fmt.Println("hash")
	//		case tcontent:
	//			O, L := uint64(offset), uint64(len(testBlock.value))
	//			testOutput = append(testOutput, toutput{
	//				O, L, testBlock.value,
	//			})
	//			fmt.Println("content")
	//		default:
	//			panic("unknown test input type")
	//		}
	//	}
	//	expectedOutputs = append(expectedOutputs, testOutput)
	//}

	stand.addHash("abcd")
	stand.addContent("123")

	fastHash.Reset()
	strongHash.Reset()

	r := NewStackedReadSeeker(strings.NewReader("abcd123"))
	blocks := producer.Scan(r)
	stand.verify(t, blocks)
}
