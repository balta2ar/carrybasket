package main

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestLoggingFilesystem_Everything(t *testing.T) {
	fs := NewLoggingFilesystem()

	handle, err := fs.Open("d")
	assert.NotNil(t, handle)
	assert.Nil(t, err)

	_, err = handle.Write([]byte("abc"))
	assert.Nil(t, err)

	handle, err = fs.Open("x")
	assert.NotNil(t, handle)
	assert.Nil(t, err)

	_, err = handle.Write([]byte("123"))
	assert.Nil(t, err)

	assert.Error(t, fs.Move("a", "b"))
	assert.Error(t, fs.Move("x", "d"))
	assert.Nil(t, fs.Move("d", "b"))

	handle, err = fs.Open("b")
	assert.Nil(t, err)
	result, err := ioutil.ReadAll(handle)
	assert.Equal(t, "abc", string(result))

	assert.Nil(t, fs.Delete("b"))
	assert.Error(t, fs.Delete("b"))
	assert.Error(t, fs.Delete("a"))

	assert.Equal(t, []string{
		"open d",
		"open x",
		"move a b",
		"move x d",
		"move d b",
		"open b",
		"delete b",
		"delete b",
		"delete a",
	}, fs.Actions)
}

func TestLoggingFilesystem_OpenWriteOpenReadOpenRead(t *testing.T) {
	fs := NewLoggingFilesystem()

	handle, err := fs.Open("a")
	assert.NotNil(t, handle)
	assert.Nil(t, err)

	_, err = handle.Write([]byte("abc"))
	assert.Nil(t, err)

	handle, err = fs.Open("a")
	assert.NotNil(t, handle)
	assert.Nil(t, err)

	result, err := ioutil.ReadAll(handle)
	assert.Nil(t, err)
	assert.Equal(t, "abc", string(result))

	handle, err = fs.Open("a")
	assert.NotNil(t, handle)
	assert.Nil(t, err)

	result, err = ioutil.ReadAll(handle)
	assert.Nil(t, err)
	assert.Equal(t, "abc", string(result))
}
