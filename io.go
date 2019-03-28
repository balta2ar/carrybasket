package main

import (
	"github.com/pkg/errors"
	"io"
)

/// This type allows for convenient jumping back to the previously
/// remembered reading position.
type StackedReadSeeker interface {
	io.ReadSeeker
	Push() error /// remember current position
	Pop() error  /// restore previous position
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

func (srs *stackedReadSeeker) Push() error {
	offset, err := srs.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	srs.offsets = append(srs.offsets, offset)
	return nil
}

func (srs *stackedReadSeeker) Pop() error {
	if len(srs.offsets) == 0 {
		return errors.New("popping from an empty stack")
	}

	n := len(srs.offsets)
	_, err := srs.Seek(srs.offsets[n-1], io.SeekStart)
	if err != nil {
		return errors.Wrap(err, "Cannot seek")

	}
	srs.offsets = srs.offsets[:n-1]
	return nil
}
