package carrybasket

import (
	"fmt"
	"hash"
	"io"
)

/// BlockProducer runs on the client and scans the input file to detect
/// which parts of it are present on the server using fast & strong
/// hashes from the server. The missing parts are produced as content
/// blocks and should be sent to the server to apply changes using
/// ContentReconstructor.
type BlockProducer interface {
	Scan(r io.Reader) []Block
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
	content     []byte // accumulated content so far that has not been emitted
	contentSize int
	buffer      []byte
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
		content:     nil,
		contentSize: 0,
		buffer:      nil,
	}
	producer.Reset()
	return producer
}

func (bp *blockProducer) Reset() {
	bp.offset = 0
	bp.cutoff = 0
	bp.content = make([]byte, bp.blockSize)
	bp.contentSize = 0
	bp.buffer = make([]byte, 1)
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
	if bp.contentSize > 0 {
		blocks = bp.emitContent(blocks, bp.offset-bp.contentSize, bp.content[0:bp.contentSize])
		bp.contentSize = 0
	}
	return blocks
}

func (bp *blockProducer) emitContent(blocks []Block, offset int, content []byte) []Block {
	contentBlock := NewContentBlock(uint64(offset), uint64(len(content)), content)
	bp.updateBothCachesWithContent(contentBlock)
	blocks = append(blocks, contentBlock)
	return blocks
}

// Update fast & strong caches with content from client. This is necessary
// to make sure we don't send the same blocks of content from the client
// twice and more. Each content block is sent once, then cached, then
// its strong hash is sent.
func (bp *blockProducer) updateBothCachesWithContent(contentBlock ContentBlock) {
	bp.fastHasher.Reset()
	_, _ = bp.fastHasher.Write(contentBlock.Content())
	fastHash := bp.fastHasher.Sum(nil)

	bp.strongHasher.Reset()
	bp.strongHasher.Write(contentBlock.Content())
	strongHash := bp.strongHasher.Sum(nil)

	fastHashedBlock := NewHashedBlock(contentBlock.Offset(), contentBlock.Size(), fastHash)
	strongHashedBlock := NewHashedBlock(contentBlock.Offset(), contentBlock.Size(), strongHash)

	bp.fastHashCache.Set(fastHash, fastHashedBlock)
	bp.strongHashCache.Set(strongHash, strongHashedBlock)
}

func (bp *blockProducer) tryEmitHash(blocks []Block, r io.Reader) ([]Block, bool) {
	if cachedBlock, ok := bp.findFastAndStrongHash(r); ok {
		// Fast & strong hashes have been found.
		// But before we proceed, there could be content before this
		// hashed block which we haven't emitted yet. We can check current
		// content size whether it's bigger than our backward lookup
		// window.
		partialContentSize := bp.contentSize - bp.windowSizeBackward()
		if partialContentSize > 0 {
			partialContent := bp.content[0:partialContentSize]
			blocks = bp.emitContent(blocks, bp.offset-bp.contentSize, partialContent)
		}

		blocks = bp.emitHash(blocks, cachedBlock.(HashedBlock))
		// hash has been emitted, we need to clear current content and hashes
		bp.cutoff = bp.offset
		bp.contentSize = 0
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
func (bp *blockProducer) readCurrentWindow(r io.Reader) []byte {
	bytesToRewind := bp.windowSizeBackward()
	return bp.content[bp.contentSize-bytesToRewind:bp.contentSize]
}

// Check whether fast & strong hashes of the current window
// are available in caches
func (bp *blockProducer) findFastAndStrongHash(r io.Reader) (Block, bool) {
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
func (bp *blockProducer) Scan(r io.Reader) []Block {
	blocks := make([]Block, 0)
	for {
		blocks, _ = bp.tryEmitHash(blocks, r)
		if err := bp.advance(r); err != nil {
			break
		}
	}

	blocks, _ = bp.tryEmitHash(blocks, r)
	blocks = bp.tryEmitContent(blocks)
	return blocks
}

// Try reading 1 byte from reader r
func (bp *blockProducer) advance(r io.Reader) error {

	n, err := r.Read(bp.buffer)
	if err != nil {
		return err
	}
	if n != 1 {
		panic(fmt.Sprintf("unexpected number of bytes read: %v\n", n))
	}
	_, _ = bp.fastHasher.Write(bp.buffer)
	bp.offset += n
	if bp.contentSize+1 == cap(bp.content) {
		newContent := make([]byte, 2*cap(bp.content))
		copy(newContent, bp.content[0:bp.contentSize])
		bp.content = newContent
	}
	bp.content[bp.contentSize] = bp.buffer[0]
	bp.contentSize++


	return nil
}

/// Some parts of the system need to have producer at hand, but
/// they don't need to know the block size. This abstraction hides
/// the details about the block size.
type ProducerFactory interface {
	MakeProducer(fastHashBlocks []Block, strongHashBlocks []Block) BlockProducer
	MakeProducerWithCache(fastCache BlockCache, strongCache BlockCache) BlockProducer
}

type MakeFastHash func() hash.Hash32
type MakeStrongHash func() hash.Hash

type producerFactory struct {
	blockSize   int
	hashFactory HashFactory
}

func NewProducerFactory(
	blockSize int,
	hashFactory HashFactory,
) *producerFactory {
	return &producerFactory{
		blockSize:   blockSize,
		hashFactory: hashFactory,
	}
}

func (pf *producerFactory) MakeProducer(fastHashBlocks []Block, strongHashBlocks []Block) BlockProducer {
	fastCache := NewBlockCache()
	strongCache := NewBlockCache()

	if fastHashBlocks != nil {
		fastCache.AddHashes(fastHashBlocks)
	}

	if strongHashBlocks != nil {
		strongCache.AddHashes(strongHashBlocks)
	}

	return pf.MakeProducerWithCache(fastCache, strongCache)
}

func (pf *producerFactory) MakeProducerWithCache(fastCache BlockCache, strongCache BlockCache) BlockProducer {
	return NewBlockProducer(
		pf.blockSize,
		pf.hashFactory.MakeFastHash(),
		pf.hashFactory.MakeStrongHash(),
		fastCache,
		strongCache,
	)
}
