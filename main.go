package main

import (
	"fmt"
	"io"
)

// --- Rolling checksum ---------------------------------------------

/// This type is able to calculate rolling checksum
type RollingChecksummer interface {
	Consume() bool
	Checksum() uint32
}

/// Fast signature implementation referred in rsync thesis and attributed
/// to Paul Mackerras.
type MackerrasChecksum struct {
	r         io.Reader
	blockSize int
	checksum  uint32
}

func NewMackerrasChecksum(r io.Reader, blockSize int) *MackerrasChecksum {
	return &MackerrasChecksum{
		r,
		blockSize,
		0,
	}
}

func (mc *MackerrasChecksum) Consume() bool {
	buf := make([]byte, 1)
	_, err := mc.r.Read(buf)
	if err != nil {
		return false
	}
	mc.checksum += 1
	return true
}

func (mc *MackerrasChecksum) Checksum() uint32 {
	return mc.checksum
}


// --- Main ---------------------------------------------------------

func main() {
	fmt.Println("hello")
}
