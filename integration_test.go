package main

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/stretchr/testify/assert"
	"hash"
	"strings"
	"testing"
)

type File struct {
	Filename string
	IsDir    bool
	Content  string
}

func assertGenerateProduceReconstruct(
	t *testing.T,
	blockSize int,
	clientContent string,
	serverContent string,
) {
	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()

	generator := NewHashGenerator(blockSize, fastHasher, strongHasher)
	generatorResult := generator.Scan(strings.NewReader(serverContent))

	fastCache := NewBlockCache()
	fastCache.AddHashes(generatorResult.fastHashes)
	strongCache := NewBlockCache()
	strongCache.AddHashes(generatorResult.strongHashes)
	fastHasher.Reset()
	strongHasher.Reset()
	producer := NewBlockProducer(blockSize, fastHasher, strongHasher, fastCache, strongCache)
	r := strings.NewReader(clientContent)
	producerResult := producer.Scan(r)

	contentCache := NewBlockCache()
	contentCache.AddContents(generatorResult.strongHashes, generatorResult.contentBlocks)
	reconstructor := NewContentReconstructor(contentCache)
	serverOutputFile := bytes.NewBuffer(nil)
	reconstructor.Reconstruct(producerResult, serverOutputFile)

	assert.Equal(t, clientContent, serverOutputFile.String())
}

func makeFilesystem(files []File) VirtualFilesystem {
	fs := NewLoggingFilesystem()

	for _, file := range files {
		if file.IsDir {
			_ = fs.Mkdir(file.Filename)
		} else {
			rw, _ := fs.Open(file.Filename)
			_, _ = rw.Write([]byte(file.Content))
		}
	}

	return fs
}

func TestIntegration_Smoke(t *testing.T) {
	blockSize := 4
	serverContent := ""
	clientContent := ""

	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()

	generator := NewHashGenerator(blockSize, fastHasher, strongHasher)
	generatorResult := generator.Scan(strings.NewReader(serverContent))
	assert.Empty(t, generatorResult.contentBlocks)

	fastCache := NewBlockCache()
	fastCache.AddHashes(generatorResult.fastHashes)
	strongCache := NewBlockCache()
	strongCache.AddHashes(generatorResult.strongHashes)
	fastHasher.Reset()
	strongHasher.Reset()
	producer := NewBlockProducer(blockSize, fastHasher, strongHasher, fastCache, strongCache)
	r := strings.NewReader(clientContent)
	producerResult := producer.Scan(r)
	assert.Empty(t, producerResult)

	contentCache := NewBlockCache()
	contentCache.AddContents(generatorResult.strongHashes, generatorResult.contentBlocks)
	reconstructor := NewContentReconstructor(contentCache)
	serverOutputFile := bytes.NewBuffer(nil)
	n := reconstructor.Reconstruct(producerResult, serverOutputFile)
	assert.Equal(t, uint64(0), n)
	assert.Equal(t, clientContent, serverOutputFile.String())
}

func TestIntegration_ContentEquality(t *testing.T) {
	testCases := []struct {
		blockSize     int
		clientContent string
		serverContent string
	}{
		{4, "", ""},
		{4, "abcd1234", "abcd1234"},
		{4, "abcd123", "abcd1234"},
		{4, "abcd1234", "abcd123"},
		{4, "ab1234", "abcd123"},
		{4, "abcd1234", "ab123"},
		{4, "abcd34", "abcd123"},
		{4, "1234", "abcd"},

		{2, "1278", "12345678"},
		{2, "5678", "12345678"},
		{2, "12345678", "1278"},
		{2, "12345678", "12"},
		{2, "12345678", "78"},
	}
	for i, tt := range testCases {
		fmt.Printf("***** Running test case %v (%v, '%v' => '%v')\n",
			i, tt.blockSize, tt.clientContent, tt.serverContent)
		assertGenerateProduceReconstruct(
			t, tt.blockSize, tt.clientContent, tt.serverContent)
	}
}

func assertFilesystemsEqual(t *testing.T, leftFs VirtualFilesystem, rightFs VirtualFilesystem) {
	leftFiles, err := leftFs.ListAll()
	assert.Nil(t, err)
	rightFiles, err := rightFs.ListAll()
	assert.Nil(t, err)

	assert.Equal(t, len(leftFiles), len(rightFiles))
	assert.Equal(t, leftFiles, rightFiles)

	// TODO: compare each file's content and IsDir flag
}

func TestIntegration_SyncClientServerOffline(t *testing.T) {
	blockSize := 4
	makeFastHash := func() hash.Hash32 { return NewMackerras(blockSize) }
	makeStrongHash := func() hash.Hash { return md5.New() }

	fastHasher := makeFastHash()
	strongHasher := makeStrongHash()
	generator := NewHashGenerator(blockSize, fastHasher, strongHasher)

	clientFiles := []File{
		{"a", true, ""},
		{"b", false, "abcd"},
	}
	clientFs := makeFilesystem(clientFiles)
	listedClientFiles, err := ListClientFiles(clientFs)
	assert.Nil(t, err)
	assert.Len(t, listedClientFiles, 2)

	serverFiles := []File{
		{"b", false, "1234"},
	}
	serverFs := makeFilesystem(serverFiles)
	serverContentCache := NewBlockCache()
	listedServerFiles, err := ListServerFiles(serverFs, generator, serverContentCache)
	assert.Nil(t, err)
	assert.Len(t, listedServerFiles, 1)

	factory := NewProducerFactory(blockSize, makeFastHash, makeStrongHash)
	comparator := NewFilesComparator(factory)
	commands := comparator.Compare(listedClientFiles, listedServerFiles)

	reconstructor := NewContentReconstructor(serverContentCache)
	applier := NewAdjustmentCommandApplier()
	err = applier.Apply(commands, serverFs, reconstructor)
	assert.Nil(t, err)

	assertFilesystemsEqual(t, clientFs, serverFs)
}
