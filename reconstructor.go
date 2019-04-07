package main

import (
	"hash"
	"io"
	"sort"
)

/// ContentReconstructor rebuilds a file from the given list of hashed
/// and content blocks. Content blocks are inserted into the file as is,
/// hashed blocks are looked up in a cache where actual content is stored.
/// This abstraction is supposed to be used by the receiving (server)
/// side.
type ContentReconstructor interface {
	Reconstruct(blocks []Block, w io.Writer) uint64
}

type contentReconstructor struct {
	strongHasher    hash.Hash
	strongHashCache BlockCache
}

func NewContentReconstructor(strongHasher hash.Hash, strongHashCache BlockCache) *contentReconstructor {
	return &contentReconstructor{
		strongHasher,
		strongHashCache,
	}
}

type byOffset []Block

func (b byOffset) Len() int           { return len(b) }
func (b byOffset) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byOffset) Less(i, j int) bool { return b[i].Offset() < b[j].Offset() }

/// Reconstruct the file from the given blocks into the given writer w.
/// Return the final offset, which is equal to file size.
func (cr *contentReconstructor) Reconstruct(blocks []Block, w io.Writer) uint64 {
	var offset uint64

	// Sort blocks in the increasing offset order
	sort.Sort(byOffset(blocks))

	for _, abstractBlock := range blocks {
		if offset != abstractBlock.Offset() {
			panic("current offset does not match another block offset")
		}

		switch block := abstractBlock.(type) {
		case ContentBlock:
			n, _ := w.Write(block.Content())
			offset += uint64(n)
			cr.updateStrongCacheWithContent(block)

		case HashedBlock:
			hashedBlock, ok := cr.strongHashCache.Get(block.HashSum())
			if !ok {
				panic("could not find hashed block in the cache")
			}
			contentBlock := hashedBlock.(ContentBlock)
			if contentBlock.Size() != uint64(len(contentBlock.Content())) {
				panic("content block size does not match actual content length")
			}
			n, _ := w.Write(contentBlock.Content())
			offset += uint64(n)
		}
	}

	return offset
}

// Client will not send the same content block twice. Instead, it will reuse already
// sent blocks. Thus we need to hash new content blocks that we see because they
// maybe come as strong hashed blocks later in the stream.
func (cr *contentReconstructor) updateStrongCacheWithContent(contentBlock ContentBlock) {
	cr.strongHasher.Reset()
	cr.strongHasher.Write(contentBlock.Content())
	strongHash := cr.strongHasher.Sum(nil)
	cr.strongHashCache.Set(strongHash, contentBlock)
}
