package main

import (
	"hash"
	"io"
)

/// BlockProducer runs on the client and scans the input file to detect
/// which parts of it are present on the server using fast & strong
/// hashes from the server. The missing parts are produced as content
/// blocks and should be sent to the server to apply changes using
/// ContentReconstructor.
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
		content:     nil,
	}
	producer.Reset()
	return producer
}

func (bp *blockProducer) Reset() {
	bp.offset = 0
	bp.cutoff = 0
	bp.content = make([]byte, 0)
}

// How many bytes in the input is left
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
	leftBarrier := max(bp.cutoff, bp.offset-bp.blockSize)
	return bp.offset - leftBarrier
}

func (bp *blockProducer) tryEmitContent(blocks []Block) []Block {
	if len(bp.content) > 0 {
		blocks = bp.emitContent(blocks, bp.offset-len(bp.content), bp.content)
		bp.content = make([]byte, 0)
	}
	return blocks
}

func (bp *blockProducer) emitContent(blocks []Block, offset int, content []byte) []Block {
	contentBlock := NewContentBlock(uint64(offset), uint64(len(content)), content)
	blocks = append(blocks, contentBlock)
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

		return blocks, true
	}

	return blocks, false
}

func (bp *blockProducer) emitHash(blocks []Block, hashedBlock HashedBlock) []Block {
	offset := uint64(bp.offset) - hashedBlock.Size()
	block := NewHashedBlock(offset, hashedBlock.Size(), hashedBlock.HashSum())
	return append(blocks, block)
}

// Read current window backwards. This method is used when we found fast
// match and we check whether strong hash matches as well. Normally strong
// hash is calculated over blockSize bytes, but it may be smaller in the
// beginning and in the end of the input.
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

// Check whether fast & strong hashes of the current window
// are available in caches
func (bp *blockProducer) findFastAndStrongHash(r StackedReadSeeker) (Block, bool) {
	// first, check fast hash
	if _, ok := bp.fastHashCache.Get(bp.fastHasher.Sum(nil)); !ok {
		return nil, false
	}

	// fast hash matched, compute and check strong hash
	windowContent := bp.readCurrentWindow(r)
	bp.strongHasher.Reset()
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
	err := bp.advance(r, bp.windowSizeForward())
	if err != nil {
		return blocks
	}

	for {
		blocks, _ = bp.tryEmitHash(blocks, r)
		if err := bp.advance(r, 1); err != nil {
			break
		}
	}

	blocks, _ = bp.tryEmitHash(blocks, r)
	blocks = bp.tryEmitContent(blocks)
	return blocks
}

// Try reading n bytes from reader r
func (bp *blockProducer) advance(r StackedReadSeeker, n int) (error) {
	buffer := make([]byte, n)
	n, err := r.Read(buffer)
	if err != nil {
		return err
	}
	_, _ = bp.fastHasher.Write(buffer)
	bp.offset += n
	bp.bytesRemain -= n
	bp.content = append(bp.content, buffer...)

	return nil
}
