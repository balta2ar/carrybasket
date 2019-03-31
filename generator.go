package main

import (
	"hash"
	"io"
)

/// The goal of HashGenerator is to scan the input and produce
/// the following:
/// 1) A list of fast hashes that will be sent to the client
/// 2) A list of strong hashes that will be sent to the client
/// 3) A list of content to be used later at reconstruction stage
/// User by the server to prepare its content for comparison by the client
/// and upcoming reconstruction.
type HashGenerator interface {
	Scan(r io.Reader) HashGeneratorResult
	Reset()
}

type HashGeneratorResult struct {
	fastHashes    []Block
	strongHashes  []Block
	contentBlocks []Block
}

type hashGenerator struct {
	blockSize    int
	fastHasher   hash.Hash32
	strongHasher hash.Hash
}

func NewHashGenerator(
	blockSize int,
	fastHasher hash.Hash32,
	strongHasher hash.Hash,
) *hashGenerator {
	return &hashGenerator{
		blockSize:    blockSize,
		fastHasher:   fastHasher,
		strongHasher: strongHasher,
	}
}

func (hg *hashGenerator) Scan(r io.Reader) HashGeneratorResult {
	var offset uint64
	var buffer = make([]byte, hg.blockSize)
	var result = HashGeneratorResult{
		make([]Block, 0),
		make([]Block, 0),
		make([]Block, 0),
	}

	for {
		n, err := r.Read(buffer)
		if err != nil {
			break
		}
		hg.strongHasher.Reset()
		hg.strongHasher.Write(buffer[:n])
		strongHash := hg.strongHasher.Sum(nil)

		hg.fastHasher.Reset()
		_, _ = hg.fastHasher.Write(buffer[:n])
		fastHash := hg.fastHasher.Sum(nil)

		result.fastHashes = append(result.fastHashes,
			NewHashedBlock(offset, uint64(n), fastHash))
		result.strongHashes = append(result.strongHashes,
			NewHashedBlock(offset, uint64(n), strongHash))
		result.contentBlocks = append(result.contentBlocks,
			NewContentBlock(offset, uint64(n), buffer[:n]))

		offset += uint64(n)
	}

	return result
}

func (hg *hashGenerator) Reset() {}
