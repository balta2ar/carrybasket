package main

import (
	"hash"
	"io"
)

/// The goal of HashGenerator is to scan the input and produce
/// strong hashes of blocks of the given size.
type HashGenerator interface {
	Scan(r io.Reader) []Block
	Reset()
}

type hashGenerator struct {
	blockSize    int
	strongHasher hash.Hash
}

func NewHashGenerator(blockSize int, strongHasher hash.Hash) *hashGenerator {
	return &hashGenerator{
		blockSize:    blockSize,
		strongHasher: strongHasher,
	}
}

func (hg *hashGenerator) Scan(r io.Reader) []Block {
	var blocks []Block
	var offset uint64
	var buffer = make([]byte, hg.blockSize)

	for {
		n, err := r.Read(buffer)
		if err != nil {
			break
		}
		hg.strongHasher.Reset()
		hg.strongHasher.Write(buffer[:n])
		blocks = append(blocks, NewHashedBlock(offset, uint64(n), hg.strongHasher.Sum(nil)))
		offset += uint64(n)
	}

	return blocks
}

func (hg *hashGenerator) Reset() {}
