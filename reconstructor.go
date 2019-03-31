package main

import (
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
	strongHashCache BlockCache
}

func NewContentReconstructor(strongHashCache BlockCache) *contentReconstructor {
	return &contentReconstructor{
		strongHashCache,
	}
}

type byOffset []Block

func (b byOffset) Len() int           { return len(b) }
func (b byOffset) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byOffset) Less(i, j int) bool { return b[i].Offset() < b[j].Offset() }

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
		case HashedBlock:
			contentBlock, ok := cr.strongHashCache.Get(block.HashSum())
			if !ok {
				panic("could not find hashed block in the cache")
			}
			n, _ := w.Write(contentBlock.(ContentBlock).Content())
			offset += uint64(n)
		}
	}

	return offset
}
