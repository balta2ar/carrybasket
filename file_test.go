package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLoggingFilesystem_Everything(t *testing.T) {
	fs := NewLoggingFilesystem()
	assert.Nil(t, fs.Move("a", "b"))
	assert.Nil(t, fs.Delete("c"))
	handle, err := fs.Open("d")
	assert.Nil(t, handle)
	assert.Nil(t, err)
	assert.Equal(t, []string{"move a b", "delete c", "open d"}, fs.Actions)
}
