package main

import (
	"crypto/md5"
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

	filenames, err := fs.ListAll()
	assert.Nil(t, err)
	assert.Equal(t, []string{"x"}, filenames)

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
		"listall",
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

	filenames, err := fs.ListAll()
	assert.Nil(t, err)
	assert.Equal(t, []string{"a"}, filenames)
}

func TestLoggingFilesystem_IsPath(t *testing.T) {
	fs := NewLoggingFilesystem()

	assert.False(t, fs.IsPath("a"))

	handle, err := fs.Open("a")
	assert.NotNil(t, handle)
	assert.Nil(t, err)

	assert.True(t, fs.IsPath("a"))

	assert.Equal(t, []string{
		"ispath a",
		"open a",
		"ispath a",
	}, fs.Actions)
}

func TestLoggingFilesystem_IsDirMkDir(t *testing.T) {
	fs := NewLoggingFilesystem()

	assert.False(t, fs.IsDir("a"))

	handle, err := fs.Open("a")
	assert.NotNil(t, handle)
	assert.Nil(t, err)

	assert.False(t, fs.IsDir("a"))
	assert.Error(t, fs.Mkdir("a"))
	assert.Nil(t, fs.Mkdir("b"))

	handle, err = fs.Open("b")
	assert.Nil(t, nil)
	assert.Error(t, err)

	filenames, err := fs.ListAll()
	assert.Nil(t, err)
	assert.Equal(t, []string{"a", "b"}, filenames)

	assert.Equal(t, []string{
		"isdir a",
		"open a",
		"isdir a",
		"mkdir a",
		"mkdir b",
		"open b",
		"listall",
	}, fs.Actions)
}

func TestListClientFiles_Smoke(t *testing.T) {
	fs := NewLoggingFilesystem()
	files, err := ListClientFiles(fs)
	assert.Nil(t, err)
	assert.Empty(t, files)
}

func TestListClientFiles_FilesAndDirs(t *testing.T) {
	fs := NewLoggingFilesystem()
	_, _ = fs.Open("a")
	_ = fs.Mkdir("b")
	_, _ = fs.Open("b/nested1")
	_, _ = fs.Open("b/nested2")

	files, err := ListClientFiles(fs)
	assert.Nil(t, err)
	assert.Len(t, files, 4)

	assert.Equal(t, "a", files[0].Filename)
	assert.False(t, files[0].IsDir)
	assert.NotNil(t, files[0].Rw)

	assert.Equal(t, "b", files[1].Filename)
	assert.True(t, files[1].IsDir)
	assert.Nil(t, files[1].Rw)

	assert.Equal(t, "b/nested1", files[2].Filename)
	assert.False(t, files[2].IsDir)
	assert.NotNil(t, files[2].Rw)

	assert.Equal(t, "b/nested2", files[3].Filename)
	assert.False(t, files[3].IsDir)
	assert.NotNil(t, files[3].Rw)
}

func TestListServerFiles_Smoke(t *testing.T) {
	fs := NewLoggingFilesystem()
	generator := NewHashGenerator(4, nil, nil)
	files, err := ListServerFiles(fs, generator)
	assert.Nil(t, err)
	assert.Empty(t, files)
}

func TestListServerFiles_FilesAndDirs(t *testing.T) {
	fs := NewLoggingFilesystem()
	_ = fs.Mkdir("a")
	rw, _ := fs.Open("b")
	_, _ = rw.Write([]byte("abc"))
	rw, _ = fs.Open("c")
	_, _ = rw.Write([]byte("abcd"))
	rw, _ = fs.Open("d")
	_, _ = rw.Write([]byte("abcdefg"))
	rw, _ = fs.Open("e")
	_, _ = rw.Write([]byte("abcdefgh"))

	blockSize := 4
	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()
	generator := NewHashGenerator(4, fastHasher, strongHasher)
	files, err := ListServerFiles(fs, generator)
	assert.Nil(t, err)
	assert.Len(t, files, 5)

	expectedCases := []struct{
		Filename string
		IsDir bool
		NumHashes int
	}{
		{"a", true, 0},
		{"b", false, 1},
		{"c", false, 1},
		{"d", false, 2},
		{"e", false, 2},
	}
	for i, expected := range expectedCases {
		assert.Equal(t, expected.Filename, files[i].Filename)
		assert.Equal(t, expected.IsDir, files[i].IsDir)
		assert.Len(t, files[i].FastHashes, expected.NumHashes)
		assert.Len(t, files[i].StrongHashes, expected.NumHashes)
	}
}
