package main

import (
	"encoding/binary"
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
	Scan(r io.ReadSeeker) []Block
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

	offset   uint64
	smallBuf []byte
	fastHash []byte
}

func NewBlockProducer(
	blockSize int,
	fastHasher hash.Hash32,
	strongHasher hash.Hash,
	fastHashCache BlockCache,
	strongHashCache BlockCache,
) *blockProducer {
	producer := &blockProducer{
		blockSize,
		fastHasher,
		strongHasher,
		fastHashCache,
		strongHashCache,

		0,
		nil,
		nil,
	}
	producer.Reset()
	return producer
}

func (bp *blockProducer) Reset() {
	bp.offset = 0
	bp.smallBuf = make([]byte, 1)
	bp.fastHash = make([]byte, 4)
}

func (bp *blockProducer) Scan(r io.ReadSeeker) []Block {
	blocks := make([]Block, 0)

	for {
		err := bp.advance(r)
		if err != nil {
			break
		}

		_, _ = bp.fastHasher.Write(bp.smallBuf)
		binary.LittleEndian.PutUint32(bp.fastHash, bp.fastHasher.Sum32())

		_, ok := bp.fastHashCache.Get(bp.fastHash)
		if ok {
			// fast has been found

			// compute strong
			// check strong
			// ? emit HashedBlock
			fmt.Println("Emitting hashed block")
			block := NewHashedBlock(bp.offset, 4, []byte("hash"))
			blocks = append(blocks, block)
		} else {
			// emit raw content
			fmt.Println("Emitting raw content")
			block := NewContentBlock(bp.offset, 256, []byte("content"))
			blocks = append(blocks, block)
		}
	}

	return blocks
}

func (bp *blockProducer) advance(r io.ReadSeeker) error {
	n, err := r.Read(bp.smallBuf)
	if err != nil {
		//return errors.Wrap(err, "Cant read from reader")
		fmt.Printf("cannot read: %s\n", err)
		return err
	}
	if n == 0 {
		return nil
	}
	bp.offset += uint64(n)
	return nil
}
