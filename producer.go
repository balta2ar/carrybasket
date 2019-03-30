package main

import (
	"fmt"
	"hash"
	"io"
)

// --- Block producer -----------------------------------------------

// scan single byte of input as a time
// if fast checksum at this point does not match,
//   emit byte
//   advance one byte in the input
// if fast checksum matches,
//   compute strong checksum
//   if strong matches,
//     emit HashedBlock
//     advance block size in the input

/// This abstraction can scan input and produce blocks. Either:
/// - hashed blocks that can be reused by the receiver side, or
/// - content blocks that contain data that should be put into file by the
///   receiver
/// To operate, it requires:
/// + input readseaker (what to scan)
/// - block size (sizes of blocks to scan)
/// - fast hasher (can read input one byte at a time)
/// - strong hasher (can read blocks of input)
/// + fast hash lookuper
/// + strong hash lookuper
type BlockProducer interface {
	Scan(r StackedReadSeeker) []Block
	Reset()
}

// ctor: blockSize, fast hasher, strong hasher, 2 caches
// runtime: reader

type blockProducer struct {
	blockSize       int
	fastHasher      hash.Hash32
	strongHasher    hash.Hash
	fastHashCache   BlockCache
	strongHashCache BlockCache

	offset      int    // current reading offset
	bytesRemain int    // how many bytes remain in the input
	buffer      []byte // buffer into which we read data from the input
	content     []byte // accumulated content so far that has not been emitted
	fastHash    []byte // current fast hash value
}

func NewBlockProducer(
	blockSize int,
	fastHasher hash.Hash32,
	strongHasher hash.Hash,
	fastHashCache BlockCache,
	strongHashCache BlockCache,
) *blockProducer {
	producer := &blockProducer{
		blockSize:       blockSize,
		fastHasher:      fastHasher,
		strongHasher:    strongHasher,
		fastHashCache:   fastHashCache,
		strongHashCache: strongHashCache,

		offset:      0,
		bytesRemain: 0,
		buffer:      nil,
		content:     nil,
		fastHash:    nil,
	}
	producer.Reset()
	return producer
}

func (bp *blockProducer) Reset() {
	bp.offset = 0
	bp.buffer = make([]byte, 1)
	bp.content = make([]byte, 0)
	bp.fastHash = make([]byte, 4)
}

// how many bytes in the input is left
func inputBytesRemain(r StackedReadSeeker) int {
	r.Push()
	defer r.Pop()

	current, _ := r.Seek(0, io.SeekCurrent)
	size, _ := r.Seek(0, io.SeekEnd)
	return int(size - current)
}

// take the min of what remains and our regular block size
func (bp *blockProducer) windowSize() int {
	if bp.bytesRemain < bp.blockSize {
		return bp.bytesRemain
	}
	return bp.blockSize
}

func (bp *blockProducer) tryEmitContent(blocks []Block) []Block {
	if len(bp.content) > 0 {
		contentBlock := NewContentBlock(uint64(bp.offset), uint64(len(bp.content)), bp.content)
		blocks = append(blocks, contentBlock)
		bp.content = make([]byte, 0)
	}
	return blocks
}

func (bp *blockProducer) tryEmitHash(blocks []Block, r StackedReadSeeker) ([]Block, bool) {
	if cachedBlock, ok := bp.findFastAndStrongHash(r); ok {
		// fast & strong hashes have been found
		blocks = bp.emitHash(blocks, cachedBlock.(HashedBlock))
		// hash has been emitted, we need to clear current content
		bp.content = make([]byte, 0)
		return blocks, true
	}

	// remember current content
	bp.content = append(bp.content, bp.buffer...)
	return blocks, false
}

func (bp *blockProducer) emitHash(blocks []Block, hashedBlock HashedBlock) []Block {
	offset := uint64(bp.offset) - hashedBlock.Size()
	block := NewHashedBlock(offset, hashedBlock.Size(), hashedBlock.HashSum())
	return append(blocks, block)
}

func (bp *blockProducer) readCurrentWindow(r StackedReadSeeker) []byte {
	r.Push()
	defer r.Pop()

	bytesToRewind := bp.blockSize
	if bp.offset < bytesToRewind {
		bytesToRewind = bp.offset
	}
	if _, err := r.Seek(int64(-bytesToRewind), io.SeekCurrent); err != nil {
		panic("unexpected seek error")
	}

	buffer := make([]byte, bytesToRewind)
	if _, err := r.Read(buffer); err != nil {
		panic("unexpected read error")
	}
	return buffer
}

// check whether fast & strong hashes of the current window
// are available in caches
func (bp *blockProducer) findFastAndStrongHash(r StackedReadSeeker) (Block, bool) {
	// first, check fast hash
	_, _ = bp.fastHasher.Write(bp.buffer)
	//binary.LittleEndian.PutUint32(bp.fastHash, bp.fastHasher.Sum32())
	if _, ok := bp.fastHashCache.Get(bp.fastHasher.Sum(nil)); !ok {
		return nil, false
	}

	// fast hash matched, compute and check strong hash
	windowContent := bp.readCurrentWindow(r)
	bp.strongHasher.Write(windowContent)
	block, ok := bp.strongHashCache.Get(bp.strongHasher.Sum(nil))
	return block, ok
}

/// Scan the input and emit a slice of blocks with contents or hashes.
/// Blocks with content will be written by the server into the file.
/// Blocks with hashes indicate that server already has this block on
/// its side so it can reuse it.
func (bp *blockProducer) Scan(r StackedReadSeeker) []Block {
	blocks := make([]Block, 0)
	bp.bytesRemain = inputBytesRemain(r)

	// the first step is to read the window of size equals to the block size
	advancedBy, err := bp.advance(r, bp.windowSize())
	if err != nil {
		return blocks
	}
	fmt.Printf("start advanced by %v\n", advancedBy)

	for {
		blocks, _ = bp.tryEmitHash(blocks, r)
		advancedBy, err := bp.advance(r, 1)
		if err != nil {
			break
		}
		fmt.Printf("loop advanced by %v\n", advancedBy)
	}

	blocks = bp.tryEmitContent(blocks)
	return blocks
}

// Try reading n bytes from reader r
func (bp *blockProducer) advance(r StackedReadSeeker, n int) (int, error) {
	bp.buffer = make([]byte, n)
	n, err := r.Read(bp.buffer)
	if err != nil {
		//return errors.Wrap(err, "Cant read from reader")
		fmt.Printf("cannot read: %s\n", err)
		return n, err
	}
	bp.offset += n
	bp.bytesRemain -= n
	return n, nil
}
