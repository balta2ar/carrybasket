package carrybasket

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/stretchr/testify/assert"
	"hash"
	"strings"
	"testing"
)

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
	reconstructor := NewContentReconstructor(strongHasher, contentCache)
	serverOutputFile := bytes.NewBuffer(nil)
	reconstructor.Reconstruct(producerResult, serverOutputFile)

	assert.Equal(t, clientContent, serverOutputFile.String())
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

	reconstructor := NewContentReconstructor(strongHasher, serverContentCache)
	applier := NewAdjustmentCommandApplier()
	err = applier.Apply(commands, serverFs, reconstructor)
	assert.Nil(t, err)

	assertFilesystemsEqual(t, clientFs, serverFs)
	return commands
}

func assertNumberOfSentBlocks(
	t *testing.T,
	commands []AdjustmentCommand,
	expectedUniqueNumHashedBlocks int,
	expectedTotalNumHashedBlocks int,
	expectedUniqueNumContentBlocks int,
	expectedTotalNumContentBlocks int,
) {
	uniqueHashes := make(map[string]struct{}, 0)
	uniqueContents := make(map[string]struct{}, 0)
	var totalNumHashes, totalNumContents int

	for _, command := range commands {
		commandApply, ok := command.(AdjustmentCommandApplyBlocksToFile)
		if !ok {
			continue
		}

		for _, abstractBlock := range commandApply.blocks {
			switch block := abstractBlock.(type) {
			case HashedBlock:
				uniqueHashes[string(block.HashSum())] = struct{}{}
				totalNumHashes += 1
			case ContentBlock:
				uniqueContents[string(block.Content())] = struct{}{}
				totalNumContents += 1
			}
		}
	}

	assert.Equal(t, expectedUniqueNumHashedBlocks, len(uniqueHashes))
	assert.Equal(t, expectedTotalNumHashedBlocks, totalNumHashes)
	assert.Equal(t, expectedUniqueNumContentBlocks, len(uniqueContents))
	assert.Equal(t, expectedTotalNumContentBlocks, totalNumContents)
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
	reconstructor := NewContentReconstructor(strongHasher, contentCache)
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
	commands := assertSyncOffline(t, 4, clientFiles, serverFiles)
	assertNumberOfSentBlocks(
		t, commands,
		0, 0,
		2, 2,
	)
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
	commands := assertSyncOffline(t, 4, clientFiles, serverFiles)
	assertNumberOfSentBlocks(
		t, commands,
		2, 2,
		2, 2,
	)
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
	commands := assertSyncOffline(t, 4, clientFiles, serverFiles)
	assertNumberOfSentBlocks(
		t, commands,
		2, 2,
		2, 2,
	)
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
	commands := assertSyncOffline(t, 4, clientFiles, serverFiles)
	assertNumberOfSentBlocks(
		t, commands,
		5, 8,
		1, 1,
	)
}

func TestIntegration_ContentReuse_OneHashCommand(t *testing.T) {
	clientFiles := []File{
		{"a", false, "abcd"},
	}
	serverFiles := []File{
		{"a", false, "abcd"},
	}
	commands := assertSyncOffline(t, 4, clientFiles, serverFiles)
	assert.Len(t, commands, 1)
	assertNumberOfSentBlocks(
		t, commands,
		1, 1,
		0, 0,
	)
}

func TestIntegration_ContentReuse_AllHashed(t *testing.T) {
	clientFiles := []File{
		{"a", false, "abcdabcdabcd"},
		{"b", false, "abcdabcdabcd"},
	}
	serverFiles := []File{
		{"a", false, "abcd"},
	}
	commands := assertSyncOffline(t, 4, clientFiles, serverFiles)
	assertNumberOfSentBlocks(
		t, commands,
		1, 6,
		0, 0,
	)
}

func TestIntegration_ContentReuse_AllContent(t *testing.T) {
	clientFiles := []File{
		{"a", false, "123"},
		{"b", false, "234"},
		{"c", false, "234"},
	}
	serverFiles := []File{
		{"a", false, "abcd"},
	}
	commands := assertSyncOffline(t, 4, clientFiles, serverFiles)
	assertNumberOfSentBlocks(
		t, commands,
		1, 1,
		2, 2,
	)
}

func TestIntegration_ContentReuse_OneUniqueBlockFromClient(t *testing.T) {
	clientFiles := []File{
		{"a", false, "1234"},
		{"b", false, "12341234"},
		{"c", false, "123412341234"},
	}
	serverFiles := []File{
		{"a", false, "abcd"},
	}
	commands := assertSyncOffline(t, 4, clientFiles, serverFiles)
	assertNumberOfSentBlocks(
		t, commands,
		1, 5,
		1, 1,
	)
}
