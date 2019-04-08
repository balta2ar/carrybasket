package carrybasket

import (
	"bytes"
	"crypto/md5"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestContentReconstructor_Smoke(t *testing.T) {
	strongHashCache := NewBlockCache()
	strongHasher := md5.New()
	reconstructor := NewContentReconstructor(strongHasher, strongHashCache)
	reconstructor.Reconstruct(nil, ioutil.Discard)
}

func TestContentReconstructor_Empty(t *testing.T) {
	strongHashCache := NewBlockCache()
	strongHasher := md5.New()
	reconstructor := NewContentReconstructor(strongHasher, strongHashCache)
	buffer := bytes.NewBuffer(nil)
	n := reconstructor.Reconstruct([]Block{}, buffer)
	assert.Equal(t, uint64(0), n)
	assert.Equal(t, "", buffer.String())
}

func TestContentReconstructor_OneContent(t *testing.T) {
	strongHashCache := NewBlockCache()
	strongHasher := md5.New()
	reconstructor := NewContentReconstructor(strongHasher, strongHashCache)
	buffer := bytes.NewBuffer(nil)
	blocks := []Block{
		NewContentBlock(0, 4, []byte("1234")),
	}
	n := reconstructor.Reconstruct(blocks, buffer)
	assert.Equal(t, uint64(4), n)
	assert.Equal(t, "1234", buffer.String())
}

func TestContentReconstructor_TwoContent(t *testing.T) {
	strongHashCache := NewBlockCache()
	strongHasher := md5.New()
	reconstructor := NewContentReconstructor(strongHasher, strongHashCache)
	buffer := bytes.NewBuffer(nil)
	blocks := []Block{
		NewContentBlock(0, 4, []byte("1234")),
		NewContentBlock(4, 4, []byte("abcd")),
	}
	n := reconstructor.Reconstruct(blocks, buffer)
	assert.Equal(t, uint64(8), n)
	assert.Equal(t, "1234abcd", buffer.String())
}

func TestContentReconstructor_TwoContentInvalidOffset(t *testing.T) {
	strongHashCache := NewBlockCache()
	strongHasher := md5.New()
	reconstructor := NewContentReconstructor(strongHasher, strongHashCache)
	buffer := bytes.NewBuffer(nil)
	blocks := []Block{
		NewContentBlock(0, 4, []byte("1234")),
		NewContentBlock(100, 4, []byte("abcd")),
	}
	assert.PanicsWithValue(t, "current offset does not match another block offset",
		func() { reconstructor.Reconstruct(blocks, buffer) })
}

func TestContentReconstructor_TwoContentReorder(t *testing.T) {
	strongHashCache := NewBlockCache()
	strongHasher := md5.New()
	reconstructor := NewContentReconstructor(strongHasher, strongHashCache)
	buffer := bytes.NewBuffer(nil)
	blocks := []Block{
		NewContentBlock(4, 4, []byte("1234")),
		NewContentBlock(0, 4, []byte("abcd")),
	}
	n := reconstructor.Reconstruct(blocks, buffer)
	assert.Equal(t, uint64(8), n)
	assert.Equal(t, "abcd1234", buffer.String())
}

func TestContentReconstructor_MissingHash(t *testing.T) {
	strongHashCache := NewBlockCache()
	strongHasher := md5.New()
	reconstructor := NewContentReconstructor(strongHasher, strongHashCache)
	buffer := bytes.NewBuffer(nil)
	blocks := []Block{
		NewHashedBlock(0, 4, []byte("abcd")),
	}
	assert.PanicsWithValue(t, "could not find hashed block in the cache",
		func() { reconstructor.Reconstruct(blocks, buffer) })
}

func TestContentReconstructor_ContentAndHash(t *testing.T) {
	strongHashCache := NewBlockCache()
	strongHashCache.Set([]byte("#abcd"), NewContentBlock(0, 4, []byte("wxyz")))
	strongHasher := md5.New()
	reconstructor := NewContentReconstructor(strongHasher, strongHashCache)
	buffer := bytes.NewBuffer(nil)
	blocks := []Block{
		NewContentBlock(0, 4, []byte("1234")),
		NewHashedBlock(4, 4, []byte("#abcd")),
	}
	n := reconstructor.Reconstruct(blocks, buffer)
	assert.Equal(t, uint64(8), n)
	assert.Equal(t, "1234wxyz", buffer.String())
}
