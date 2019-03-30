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

type blockProducer struct {
	blockSize       int
	fastHasher      hash.Hash32
	strongHasher    hash.Hash
	fastHashCache   BlockCache
	strongHashCache BlockCache

	offset      int    // current reading offset
	cutoff      int    // can't rewind backwards earlier than this cut-off offset
	bytesRemain int    // how many bytes remain in the input
	//buffer      []byte // buffer into which we read data from the input
	content     []byte // accumulated content so far that has not been emitted
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
		cutoff:      0,
		bytesRemain: 0,
		//buffer:      nil,
		content:     nil,
	}
	producer.Reset()
	return producer
}

func (bp *blockProducer) Reset() {
	bp.offset = 0
	bp.cutoff = 0
	//bp.buffer = make([]byte, 1)
	bp.content = make([]byte, 0)
}

// how many bytes in the input is left
func inputBytesRemain(r StackedReadSeeker) int {
	r.Push()
	defer r.Pop()

	current, _ := r.Seek(0, io.SeekCurrent)
	size, _ := r.Seek(0, io.SeekEnd)
	return int(size - current)
}

// Take the min of what remains and our regular block size. This is
// used to advance position in the beginning of the scan when there
// could be less data than our block size.
func (bp *blockProducer) windowSizeForward() int {
	if bp.bytesRemain < bp.blockSize {
		return bp.bytesRemain
	}
	return bp.blockSize
}

// Calculate how far back we can go from the current position. This is
// used when fast checksum has matched, and we need to rewind backwards
// to read data to calculate strong checksum. Cutoff barrier is respected.
// For example, emitting a strong hash updates the cutoff position since
// after we have emitted strong hash, we can't rewind back past it to
// overlap it.
func (bp *blockProducer) windowSizeBackward() int {
	// Ideally we'd like to rewind back bp.blockSize bytes.
	// But there are edge cases:
	// 1) We're at the beginning of the input, and our offset < blockSize,
	//    we can't go back past offset (which is zero at the moment).
	// 2) We're past cut-off, updated by emitHash. We can't go back past
	//    that barrier point at which we emitted strong hash.
	leftBarrier := min(0, bp.offset-bp.blockSize)
	leftBarrier = max(leftBarrier, bp.cutoff)
	return bp.offset - leftBarrier
}

func (bp *blockProducer) tryEmitContent(blocks []Block) []Block {
	if len(bp.content) > 0 {
		//contentBlock := NewContentBlock(uint64(bp.offset), uint64(len(bp.content)), bp.content)
		//blocks = append(blocks, contentBlock)
		fmt.Println("trying to emit content, content found")
		// TODO: CHECK THIS DIFFERENCE offset-len(content)
		blocks = bp.emitContent(blocks, bp.offset-len(bp.content), bp.content)
		//fmt.Printf("content (%v) has been emitted (len %v), resetting\n", string(bp.content), len(bp.content))
		bp.content = make([]byte, 0)
	}
	return blocks
}

func (bp *blockProducer) emitContent(blocks []Block, offset int, content []byte) []Block {
	contentBlock := NewContentBlock(uint64(offset), uint64(len(content)), content)
	blocks = append(blocks, contentBlock)
	fmt.Printf("content (%v) has been emitted (len %v), resetting\n", string(content), len(content))
	return blocks
}

func (bp *blockProducer) tryEmitHash(blocks []Block, r StackedReadSeeker) ([]Block, bool) {
	if cachedBlock, ok := bp.findFastAndStrongHash(r); ok {
		// Fast & strong hashes have been found.
		// But before we proceed, there could be content before this
		// hashed block which we haven't emitted yet. We can check current
		// content size whether it's bigger than our backward lookup
		// window.
		partialContentSize := len(bp.content) - bp.windowSizeBackward()
		if partialContentSize > 0 {
			partialContent := bp.content[0:partialContentSize]
			blocks = bp.emitContent(blocks, bp.offset-len(bp.content), partialContent)
		}

		blocks = bp.emitHash(blocks, cachedBlock.(HashedBlock))
		// hash has been emitted, we need to clear current content and hashes
		bp.cutoff = bp.offset
		bp.content = make([]byte, 0)
		bp.fastHasher.Reset()
		bp.strongHasher.Reset()

		fmt.Println("hash has been emitted")
		return blocks, true
	}

	fmt.Println("no hash found, remember content")
	// remember current content
	//bp.content = append(bp.content, bp.buffer...)
	//bp.content = make([]byte, 0)
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

	bytesToRewind := bp.windowSizeBackward()
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
	if _, ok := bp.fastHashCache.Get(bp.fastHasher.Sum(nil)); !ok {
		fmt.Println("no match for fast hash")
		return nil, false
	}

	// fast hash matched, compute and check strong hash
	fmt.Println("fast has matched, trying strong hash")
	windowContent := bp.readCurrentWindow(r)
	fmt.Printf("current window size for strong hash: %v (window value '%v')\n", len(windowContent), string(windowContent))
	bp.strongHasher.Reset()
	bp.strongHasher.Write(windowContent)
	block, ok := bp.strongHashCache.Get(bp.strongHasher.Sum(nil))
	fmt.Printf("strong hash lookup result: %v\n", ok)
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
	advancedBy, err := bp.advance(r, bp.windowSizeForward())
	if err != nil {
		return blocks
	}
	fmt.Printf("start advanced by %v\n", advancedBy)

	for {
		fmt.Println("LOOP START")
		blocks, _ = bp.tryEmitHash(blocks, r)
		advancedBy, err := bp.advance(r, 1)
		if err != nil {
			break
		}
		fmt.Printf("LOOP END advanced by %v\n", advancedBy)
	}

	fmt.Printf("LOOP FINISHED; content len %v\n", len(bp.content))
	fmt.Println("trying to emit final hash")
	fmt.Printf("LEN BEFORE EMIT HASH: %v\n", len(bp.content))
	blocks, _ = bp.tryEmitHash(blocks, r)
	fmt.Printf("LEN AFTER EMIT HASH: %v\n", len(bp.content))
	fmt.Println("trying to emit final content")
	// this should not have effect -- TODO: remove it
	blocks = bp.tryEmitContent(blocks)
	return blocks
}

// Try reading n bytes from reader r
func (bp *blockProducer) advance(r StackedReadSeeker, n int) (int, error) {
	buffer := make([]byte, n)
	n, err := r.Read(buffer)
	if err != nil {
		//return errors.Wrap(err, "Cant read from reader")
		fmt.Printf("cannot read: %s\n", err)
		return n, err
	}
	_, _ = bp.fastHasher.Write(buffer)
	bp.offset += n
	bp.bytesRemain -= n

	bp.content = append(bp.content, buffer...)
	//bp.content = make([]byte, 0)

	return n, nil
}
