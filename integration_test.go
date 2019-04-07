package main

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/stretchr/testify/assert"
	"hash"
	"io/ioutil"
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

func assertSyncOffline(t *testing.T, blockSize int, clientFiles []File, serverFiles []File) []AdjustmentCommand {
	makeFastHash := func() hash.Hash32 { return NewMackerras(blockSize) }
	makeStrongHash := func() hash.Hash { return md5.New() }

	fastHasher := makeFastHash()
	strongHasher := makeStrongHash()
	generator := NewHashGenerator(blockSize, fastHasher, strongHasher)

	clientFs := makeFilesystem(clientFiles)
	listedClientFiles, err := ListClientFiles(clientFs)
	assert.Nil(t, err)
	assert.Len(t, listedClientFiles, len(clientFiles))

	serverFs := makeFilesystem(serverFiles)
	serverContentCache := NewBlockCache()
	listedServerFiles, err := ListServerFiles(serverFs, generator, serverContentCache)
	assert.Nil(t, err)
	assert.Len(t, listedServerFiles, len(serverFiles))

	factory := NewProducerFactory(blockSize, makeFastHash, makeStrongHash)
	comparator := NewFilesComparator(factory)
	commands := comparator.Compare(listedClientFiles, listedServerFiles)
	fmt.Printf("commands %+v\n", commands)

	reconstructor := NewContentReconstructor(serverContentCache)
	applier := NewAdjustmentCommandApplier()
	err = applier.Apply(commands, serverFs, reconstructor)
	assert.Nil(t, err)

	assertFilesystemsEqual(t, clientFs, serverFs)
	return commands
}

func assertUniqueNumberOfBlocks(
	t *testing.T,
	commands []AdjustmentCommand,
	expectedNumHashedBlocks int,
	expectedNumContentBlocks int,
) {
	uniqueHashes := make(map[string]struct{}, 0)
	uniqueContents := make(map[string]struct{}, 0)

	for _, command := range commands {
		commandApply, ok := command.(AdjustmentCommandApplyBlocksToFile)
		if !ok {
			continue
		}

		for _, abstractBlock := range commandApply.blocks {
			switch block := abstractBlock.(type) {
			case HashedBlock:
				uniqueHashes[string(block.HashSum())] = struct{}{}
			case ContentBlock:
				uniqueHashes[string(block.Content())] = struct{}{}
			}
		}
	}

	assert.Equal(t, expectedNumHashedBlocks, len(uniqueHashes))
	assert.Equal(t, expectedNumContentBlocks, len(uniqueContents))
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

	for i := 0; i < len(leftFiles); i++ {
		assert.Equal(t, leftFs.IsDir(leftFiles[i]), rightFs.IsDir(rightFiles[i]))
		if !leftFs.IsDir(leftFiles[i]) {
			leftRw, err := leftFs.Open(leftFiles[i])
			assert.Nil(t, err)
			assert.NotNil(t, leftRw)
			rightRw, err := leftFs.Open(rightFiles[i])
			assert.Nil(t, err)
			assert.NotNil(t, rightRw)

			leftContents, err := ioutil.ReadAll(leftRw)
			rightContents, err := ioutil.ReadAll(rightRw)
			assert.Equal(t, leftContents, rightContents)
		}
	}
}

func TestIntegration_SyncClientServerOfflineEmpty(t *testing.T) {
	clientFiles := []File{
	}
	serverFiles := []File{
		{"b", false, "1234"},
	}
	assertSyncOffline(t, 4, clientFiles, serverFiles)
}

func TestIntegration_SyncClientServerOfflineDirAndOverwrite(t *testing.T) {
	clientFiles := []File{
		{"a", true, ""},
		{"b", false, "abcd"},
	}
	serverFiles := []File{
		{"b", false, "1234"},
	}
	assertSyncOffline(t, 4, clientFiles, serverFiles)
}

func TestIntegration_SyncClientServerOfflineReplaceDirWithFile(t *testing.T) {
	clientFiles := []File{
		{"a", false, "123"},
		{"b", false, "123"},
	}
	serverFiles := []File{
		{"a", true, ""},
		{"a/1", false, "1"},
		{"a/2", false, "2"},
		{"b", false, "123"},
	}
	assertSyncOffline(t, 4, clientFiles, serverFiles)
}

func TestIntegration_SyncClientServerOfflineReplaceFileWithDir(t *testing.T) {
	clientFiles := []File{
		{"a", true, ""},
		{"a/1", false, "1"},
		{"a/2", false, "2"},
	}
	serverFiles := []File{
		{"a", false, "123"},
	}
	assertSyncOffline(t, 4, clientFiles, serverFiles)
}

func TestIntegration_SyncClientServerOfflineAppendContent(t *testing.T) {
	clientFiles := []File{
		{"a", true, ""},
		{"a/1", false, "1234aaaa"},
		{"a/2", false, "2345bbbb"},
	}
	serverFiles := []File{
		{"a", true, ""},
		{"a/1", false, "1234"},
		{"a/2", false, "2345"},
	}
	assertSyncOffline(t, 4, clientFiles, serverFiles)
}

func TestIntegration_SyncClientServerOfflinePrependContent(t *testing.T) {
	clientFiles := []File{
		{"a", true, ""},
		{"a/1", false, "aaaa1234"},
		{"a/2", false, "bbbb2345"},
	}
	serverFiles := []File{
		{"a", true, ""},
		{"a/1", false, "1234"},
		{"a/2", false, "2345"},
	}
	assertSyncOffline(t, 4, clientFiles, serverFiles)
}

func TestIntegration_SyncClientServerOfflineRemoveContent(t *testing.T) {
	clientFiles := []File{
		{"a", true, ""},
		{"a/1", false, "XXXXaaaa1234"},
		{"a/2", false, "bbbbXXXX2345"},
		{"a/3", false, "aaaa1234XXXX"},
	}
	serverFiles := []File{
		{"a", true, ""},
		{"a/1", false, "aaaa1234"},
		{"a/2", false, "bbbb2345"},
		{"a/3", false, "aaaa1234"},
	}
	assertSyncOffline(t, 4, clientFiles, serverFiles)
}

func TestIntegration_ContentReuse_NoCommands(t *testing.T) {
	clientFiles := []File{
		{"a", false, "abcd"},
	}
	serverFiles := []File{
		{"a", false, "abcd"},
	}
	commands := assertSyncOffline(t, 4, clientFiles, serverFiles)
	assert.Empty(t, commands)
}

func TestIntegration_ContentReuse_AllHashed(t *testing.T) {
	clientFiles := []File{
		{"a", false, "abcdabcdabcd"},
		{"b", false, "abcdabcdabcd"},
	}
	serverFiles := []File{
		{"a", false, "abcd"},
	}
	 assertSyncOffline(t, 4, clientFiles, serverFiles)
	//assertUniqueNumberOfBlocks(t, commands, 5, 0)
}
