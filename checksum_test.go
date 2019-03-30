package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMackerras_RollMatchesInTheMiddle(t *testing.T) {
	data := []byte("0123")
	blockSize := 2
	// start this checksum in the middle and leave it as is
	fixed := NewMackerras(blockSize, data[2:4])
	// start this checksum in the beginning and roll it to the middle
	rolling := NewMackerras(blockSize, data[0:2])
	_, _ = rolling.Write(data[2:3])
	_, _ = rolling.Write(data[3:4])
	assert.Equal(t, fixed.Sum32(), rolling.Sum32())
}

func TestMackerras_RollMatchesOverlapping(t *testing.T) {
	data := []byte("01234")
	blockSize := 3
	// start this checksum in the middle and leave it as is
	fixed := NewMackerras(blockSize, data[2:5])
	// start this checksum in the beginning and roll it to the middle
	rolling := NewMackerras(blockSize, data[0:3])
	_, _ = rolling.Write(data[3:4])
	_, _ = rolling.Write(data[4:5])
	assert.Equal(t, fixed.Sum32(), rolling.Sum32())
}

func TestMackerras_RollMatchesUpdateBothMultipleCalls(t *testing.T) {
	data := []byte("01234")
	blockSize := 2
	// initialize at different positions and roll both until they meet
	rolling1 := NewMackerras(blockSize, data[2:4])
	rolling2 := NewMackerras(blockSize, data[0:2])
	_, _ = rolling1.Write(data[4:5])
	_, _ = rolling2.Write(data[2:3])
	_, _ = rolling2.Write(data[3:4])
	_, _ = rolling2.Write(data[4:5])
	assert.Equal(t, rolling1.Sum32(), rolling2.Sum32())
}

func TestMackerras_RollMatchesUpdateBothOneCall(t *testing.T) {
	data := []byte("01234")
	blockSize := 2
	// initialize at different positions and roll both until they meet
	rolling1 := NewMackerras(blockSize, data[2:4])
	rolling2 := NewMackerras(blockSize, data[0:2])
	_, _ = rolling1.Write(data[4:5])
	_, _ = rolling2.Write(data[2:5])
	assert.Equal(t, rolling1.Sum32(), rolling2.Sum32())
}
