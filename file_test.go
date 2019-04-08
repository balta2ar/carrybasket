package carrybasket

import (
	"crypto/md5"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestLoggingFilesystem_Everything(t *testing.T) {
	fs := NewLoggingFilesystem()

	w, err := fs.OpenWrite("d")
	assert.NotNil(t, w)
	assert.Nil(t, err)

	_, err = w.Write([]byte("abc"))
	assert.Nil(t, err)

	w, err = fs.OpenWrite("x")
	assert.NotNil(t, w)
	assert.Nil(t, err)
	_, err = w.Write([]byte("abc"))
	assert.Nil(t, err)

	w, err = fs.OpenWrite("y")
	assert.NotNil(t, w)
	assert.Nil(t, err)
	_, err = w.Write([]byte("123"))
	assert.Nil(t, err)

	_ = fs.Mkdir("dir")

	assert.Error(t, fs.Move("a", "b"))
	assert.Error(t, fs.Move("y", "dir"))
	assert.Nil(t, fs.Move("x", "d"))
	assert.Nil(t, fs.Move("d", "b"))

	r, err := fs.OpenRead("b")
	assert.Nil(t, err)
	result, err := ioutil.ReadAll(r)
	assert.Equal(t, "abc", string(result))

	assert.Nil(t, fs.Delete("b"))
	assert.Error(t, fs.Delete("b"))
	assert.Error(t, fs.Delete("a"))

	filenames, err := fs.ListAll()
	assert.Nil(t, err)
	assert.Equal(t, []string{"dir", "y"}, filenames)

	assert.Equal(t, []string{
		"openwrite d",
		"openwrite x",
		"openwrite y",
		"mkdir dir",
		"move a b",
		"move y dir",
		"move x d",
		"move d b",
		"openread b",
		"delete b",
		"delete b",
		"delete a",
		"listall",
	}, fs.Actions)
}

func TestLoggingFilesystem_OpenWriteOpenReadOpenRead(t *testing.T) {
	fs := NewLoggingFilesystem()

	w, err := fs.OpenWrite("a")
	assert.NotNil(t, w)
	assert.Nil(t, err)

	_, err = w.Write([]byte("abc"))
	assert.Nil(t, err)

	// This read is bytes.Buffer-specific. This is used to
	// advance read pointer to make sure that following reads
	// are reset when Open is called.
	r, err := fs.OpenRead("a")
	assert.NotNil(t, w)
	assert.Nil(t, err)
	result, err := ioutil.ReadAll(r)
	assert.Nil(t, err)
	assert.Equal(t, "abc", string(result))

	r, err = fs.OpenRead("a")
	assert.NotNil(t, r)
	assert.Nil(t, err)

	result, err = ioutil.ReadAll(r)
	assert.Nil(t, err)
	assert.Equal(t, "abc", string(result))

	r, err = fs.OpenRead("a")
	assert.NotNil(t, r)
	assert.Nil(t, err)

	result, err = ioutil.ReadAll(r)
	assert.Nil(t, err)
	assert.Equal(t, "abc", string(result))

	filenames, err := fs.ListAll()
	assert.Nil(t, err)
	assert.Equal(t, []string{"a"}, filenames)
}

func TestLoggingFilesystem_IsPath(t *testing.T) {
	fs := NewLoggingFilesystem()

	assert.False(t, fs.IsPath("a"))

	r, err := fs.OpenRead("a")
	assert.Nil(t, r)
	assert.Error(t, err)

	w, err := fs.OpenWrite("a")
	assert.NotNil(t, w)
	assert.Nil(t, err)

	r, err = fs.OpenRead("a")
	assert.NotNil(t, r)
	assert.Nil(t, err)

	assert.True(t, fs.IsPath("a"))

	assert.Equal(t, []string{
		"ispath a",
		"openread a",
		"openwrite a",
		"openread a",
		"ispath a",
	}, fs.Actions)
}

func TestLoggingFilesystem_IsDirMkDir(t *testing.T) {
	fs := NewLoggingFilesystem()

	assert.False(t, fs.IsDir("a"))

	r, err := fs.OpenRead("a")
	assert.Nil(t, r)
	assert.Error(t, err)

	w, err := fs.OpenWrite("a")
	assert.NotNil(t, w)
	assert.Nil(t, err)

	assert.False(t, fs.IsDir("a"))
	assert.Error(t, fs.Mkdir("a"))
	assert.Nil(t, fs.Mkdir("b"))

	r, err = fs.OpenRead("b")
	assert.Nil(t, nil)
	assert.Error(t, err)

	filenames, err := fs.ListAll()
	assert.Nil(t, err)
	assert.Equal(t, []string{"a", "b"}, filenames)

	assert.Equal(t, []string{
		"isdir a",
		"openread a",
		"openwrite a",
		"isdir a",
		"mkdir a",
		"mkdir b",
		"openread b",
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
	_, _ = fs.OpenWrite("a")
	_ = fs.Mkdir("b")
	_, _ = fs.OpenWrite("b/nested1")
	_, _ = fs.OpenWrite("b/nested2")

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
	files, err := ListServerFiles(fs, generator, nil)
	assert.Nil(t, err)
	assert.Empty(t, files)
}

func TestListServerFiles_FilesAndDirs(t *testing.T) {
	fs := NewLoggingFilesystem()
	_ = fs.Mkdir("a")
	w, _ := fs.OpenWrite("b")
	_, _ = w.Write([]byte("abc"))
	w, _ = fs.OpenWrite("c")
	_, _ = w.Write([]byte("abcd"))
	w, _ = fs.OpenWrite("d")
	_, _ = w.Write([]byte("abcdefg"))
	w, _ = fs.OpenWrite("e")
	_, _ = w.Write([]byte("abcdefgh"))

	blockSize := 4
	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()
	generator := NewHashGenerator(4, fastHasher, strongHasher)
	contentCache := NewBlockCache()
	files, err := ListServerFiles(fs, generator, contentCache)
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
