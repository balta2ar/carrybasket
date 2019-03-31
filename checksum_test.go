package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMackerras_Trivial(t *testing.T) {
	data := []byte("0123")
	blockSize := 2
	fixed := NewMackerras(blockSize)
	_, _ = fixed.Write(data[2:4])
	assert.Equal(t, fixed.BlockSize(), blockSize)
	assert.Equal(t, fixed.Size(), 4)
}

func TestMackerras_Reset(t *testing.T) {
	data := []byte("0123")
	blockSize := 2
	fixed := NewMackerras(blockSize)
	_, _ = fixed.Write(data[2:4])
	checksum := fixed.Sum32()

	fixed.Reset()
	_, _ = fixed.Write(data[2:4])
	assert.Equal(t, checksum, fixed.Sum32())
}

func TestMackerras_RollMatchesInTheMiddle(t *testing.T) {
	data := []byte("0123")
	blockSize := 2
	// start this checksum in the middle and leave it as is
	fixed := NewMackerras(blockSize)
	_, _ = fixed.Write(data[2:4]) // initial call
	// start this checksum in the beginning and roll it to the middle
	rolling := NewMackerras(blockSize)
	_, _ = rolling.Write(data[0:2]) // initial call
	_, _ = rolling.Write(data[2:3])
	_, _ = rolling.Write(data[3:4])
	assert.Equal(t, fixed.Sum32(), rolling.Sum32())
	assert.Equal(t, fixed.Sum(nil), rolling.Sum(nil))
}

func TestMackerras_RollMatchesOverlapping(t *testing.T) {
	data := []byte("01234")
	blockSize := 3
	// start this checksum in the middle and leave it as is
	fixed := NewMackerras(blockSize)
	_, _ = fixed.Write(data[2:5]) // initial call
	// start this checksum in the beginning and roll it to the middle
	rolling := NewMackerras(blockSize)
	_, _ = rolling.Write(data[0:3]) // initial call
	_, _ = rolling.Write(data[3:4])
	_, _ = rolling.Write(data[4:5])
	assert.Equal(t, fixed.Sum32(), rolling.Sum32())
	assert.Equal(t, fixed.Sum(nil), rolling.Sum(nil))
}

func TestMackerras_RollMatchesUpdateBothMultipleCalls(t *testing.T) {
	data := []byte("01234")
	blockSize := 2
	// initialize at different positions and roll both until they meet
	rolling1 := NewMackerras(blockSize)
	_, _ = rolling1.Write(data[2:4]) // initial call
	rolling2 := NewMackerras(blockSize)
	_, _ = rolling2.Write(data[0:2]) // initial call
	_, _ = rolling1.Write(data[4:5])
	_, _ = rolling2.Write(data[2:3])
	_, _ = rolling2.Write(data[3:4])
	_, _ = rolling2.Write(data[4:5])
	assert.Equal(t, rolling1.Sum32(), rolling2.Sum32())
	assert.Equal(t, rolling1.Sum(nil), rolling2.Sum(nil))
}

func TestMackerras_RollMatchesUpdateBothOneCall(t *testing.T) {
	data := []byte("01234")
	blockSize := 2
	// initialize at different positions and roll both until they meet
	rolling1 := NewMackerras(blockSize)
	_, _ = rolling1.Write(data[2:4]) // initial call
	rolling2 := NewMackerras(blockSize)
	_, _ = rolling2.Write(data[0:2]) // initial call
	_, _ = rolling1.Write(data[4:5])
	_, _ = rolling2.Write(data[2:5])
	assert.Equal(t, rolling1.Sum32(), rolling2.Sum32())
	assert.Equal(t, rolling1.Sum(nil), rolling2.Sum(nil))
}
