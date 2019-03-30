package main

import (
	"io"
)

/// This type allows for convenient jumping back to the previously
/// remembered reading position.
type StackedReadSeeker interface {
	io.ReadSeeker
	Push() /// remember current position
	Pop()  /// restore previous position
}

type stackedReadSeeker struct {
	io.ReadSeeker
	offsets []int64
}

func NewStackedReadSeeker(r io.ReadSeeker) *stackedReadSeeker {
	return &stackedReadSeeker{
		ReadSeeker: r,
		offsets:    make([]int64, 0),
	}
}

func (srs *stackedReadSeeker) Push() {
	offset, err := srs.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("unexpected error during Seek")
	}

	srs.offsets = append(srs.offsets, offset)
	return
}

func (srs *stackedReadSeeker) Pop() {
	if len(srs.offsets) == 0 {
		panic("popping from an empty stack")
	}

	n := len(srs.offsets)
	_, err := srs.Seek(srs.offsets[n-1], io.SeekStart)
	if err != nil {
		panic("unexpected error during Seek")
	}
	srs.offsets = srs.offsets[:n-1]
}
