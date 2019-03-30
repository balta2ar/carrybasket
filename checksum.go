package main

import (
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
}

/// Reset is not actually supported because of the nature of rolling
/// checksum. First we initialize it with some block and then we roll,
/// however, while we roll, we loose the data we were initialized with.
/// We could separately store we were initially given, but it's easier
/// to just create new hasher and init again.
func (d *digest) Reset() {}

/// Rolling checksum taken from rsync thesis, inspired by Adler-32.
/// The expected way to use this checksum is:
/// 1. Initialize it with some block using NewMackerras
/// 2. Feed it with one byte a time using Write
func NewMackerras(blockSize int, p []byte) hash.Hash32 {
	d := &digest{
		blockSize: blockSize,
		digest:    Init,
		circle:    nil,
		index:     0,
	}
	d.init(p)
	return d
}

func (d *digest) Size() int { return Size }

func (d *digest) BlockSize() int { return d.blockSize }

// Calculate digest of the initial block
func (d *digest) init(p []byte) {
	L := len(p)
	if L == 0 {
		panic("cannot initialize on an empty block")
	}

	var r1, r2 uint32
	for i := 0; i < L; i++ {
		r1 = (r1 + uint32(p[i])) % M
		r2 = (r2 + uint32(L-i)*uint32(p[i])) % M
	}
	d.digest = (r1 & 0xffff) | (r2 << 16)
	d.circle = append(p[:0:0], p...)
	d.index = 0

}

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
}

/// Perform rolling update
func (d *digest) Write(p []byte) (nn int, err error) {
	update(d, p)
	return len(p), nil
}

func (d *digest) Sum32() uint32 { return uint32(d.digest) }

func (d *digest) Sum(in []byte) []byte {
	s := uint32(d.digest)
	return append(in, byte(s>>24), byte(s>>16), byte(s>>8), byte(s))
}

//func Checksum(data []byte) uint32 {
//	d := NewMackerras()
//	return uint32(update(Init, data))
//}
