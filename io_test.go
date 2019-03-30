package main

import (
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"testing"
)

func TestStackedReadSeeker_PushPop(t *testing.T) {
	r := strings.NewReader("abc")
	srs := NewStackedReadSeeker(r)
	assert.NotPanics(t, srs.Push)
	assert.NotPanics(t, srs.Pop)
}

func TestStackedReadSeeker_DoubleRead(t *testing.T) {
	r := strings.NewReader("abc")
	var srs StackedReadSeeker = NewStackedReadSeeker(r)
	bufSmall := make([]byte, 1)
	bufLarge := make([]byte, 5)

	srs.Push()
	srs.Read(bufSmall)
	srs.Read(bufSmall)
	srs.Pop()
	srs.Read(bufLarge)
	assert.Equal(t, bufLarge[:3], []byte("abc"))
}

func TestStackedReadSeeker_PopCorrectPosition(t *testing.T) {
	r := strings.NewReader("abc")
	var srs StackedReadSeeker = NewStackedReadSeeker(r)
	bufSmall := make([]byte, 1)

	// deplete input, but keep bookmarks
	srs.Push()
	srs.Read(bufSmall)
	srs.Push()
	srs.Read(bufSmall)
	srs.Push()
	srs.Read(bufSmall)

	_, err := srs.Read(bufSmall)
	assert.Equal(t, err, io.EOF)

	for _, expected := range []string{"c", "b", "a"} {
		srs.Pop()
		srs.Read(bufSmall)
		assert.Equal(t, bufSmall, []byte(expected))

	}
}

func TestStackedReadSeeker_PopMisuse(t *testing.T) {
	r := strings.NewReader("abc")
	var srs StackedReadSeeker = NewStackedReadSeeker(r)

	assert.Panics(t, srs.Pop)
	srs.Push()
	assert.NotPanics(t, srs.Pop)
	assert.Panics(t, srs.Pop)
}
