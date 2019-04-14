package carrybasket

import (
	"crypto/md5"
	"hash"
)

const (
	M    = 2 << 16
	Size = 4
	Init = 0
)

type digest struct {
	blockSize int    // size of the block we were initialized with
	digest    uint32 // the hash value itself
	circle    []byte // circular buffer to keep values of our rolling window
	index     int    // current position in the circular buffer
	sum       []byte // slice that is returned on Sum call (for optimization)
}

/// Reset checksum state to initial
func (d *digest) Reset() {
	d.digest = Init
	d.circle = make([]byte, d.blockSize)
	d.index = 0
	d.sum = make([]byte, Size)
}

/// Rolling checksum taken from rsync thesis, inspired by Adler-32.
/// The expected way to use this checksum is:
/// 1. Initialize it with some block using NewMackerras
/// 2. Feed it with one byte a time using Write
func NewMackerras(blockSize int) hash.Hash32 {
	d := &digest{
		blockSize: blockSize,
		digest:    Init,
		circle:    make([]byte, blockSize),
		index:     0,
		sum:       make([]byte, Size),
	}
	return d
}

func (d *digest) Size() int { return Size }

func (d *digest) BlockSize() int { return d.blockSize }

func update(d *digest, p []byte) {
	r1, r2 := uint32(d.digest&0xffff), uint32(d.digest>>16)
	L := d.blockSize

	for i := 0; i < len(p); i++ {
		// remove first value from the start of the window (ak)
		// and add value of the new end of the window (p[i])
		// variable names follow the notation from rsync paper.
		ak := uint32(d.circle[d.index])
		r1 = (r1 - ak + uint32(p[i])) % M
		r2 = (r2 - uint32(L)*ak + r1) % M
		d.circle[d.index] = p[i]
		d.index = (d.index + 1) % L
	}

	d.digest = (r1 & 0xffff) | (r2 << 16)

	s := uint32(d.digest)
	d.sum[0] = byte(s>>24)
	d.sum[1] = byte(s>>16)
	d.sum[2] = byte(s>>8)
	d.sum[3] = byte(s)
}

/// Perform rolling update
func (d *digest) Write(p []byte) (nn int, err error) {
	nn = len(p)
	update(d, p)
	return
}

func (d *digest) Sum32() uint32 { return uint32(d.digest) }

func (d *digest) Sum(in []byte) []byte {
	return d.sum
}

type HashFactory interface {
	MakeFastHash() hash.Hash32
	MakeStrongHash() hash.Hash
}

type hashFactory struct {
	blockSize int
}

func NewHashFactory(blockSize int) *hashFactory {
	return &hashFactory{
		blockSize: blockSize,
	}
}

func (hf *hashFactory) MakeFastHash() hash.Hash32 {
	return NewMackerras(hf.blockSize)
}

func (hf *hashFactory) MakeStrongHash() hash.Hash {
	return md5.New()
}
