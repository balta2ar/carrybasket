package main

import (
	"crypto/md5"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func runComparator(
	blockSize int,
	clientFiles []VirtualFile,
	serverHashedFiles []HashedFile,
) []AdjustmentCommand {
	comparator := NewFilesComparator(NewProducerFactory(blockSize))
	commands := comparator.Compare(clientFiles, serverHashedFiles)
	return commands
}

func makeClientFile(filename string, content string) VirtualFile {
	return VirtualFile{filename, strings.NewReader(content)}
}

func makeServerFile(blockSize int, filename string, content string) HashedFile {
	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()
	generator := NewHashGenerator(blockSize, fastHasher, strongHasher)
	result := generator.Scan(strings.NewReader(content))
	return HashedFile{
		filename,
		result.fastHashes,
		result.strongHashes,
	}
}

func TestFilesComparator_Smoke(t *testing.T) {
	blockSize := 4

	comparator := NewFilesComparator(NewProducerFactory(blockSize))
	commands := comparator.Compare(
		[]VirtualFile{},
		[]HashedFile{},
	)
	assert.Empty(t, commands)
}

func TestFilesComparator_RemoveOneOfTwoAndChange(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		{"b", strings.NewReader("abc")},
	}
	serverHashedFiles := []HashedFile{
		{"a", nil, nil},
		{"b", nil, nil},
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 2)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandRemoveFile).filename)
	assert.Equal(t, "b", commands[1].(AdjustmentCommandApplyBlocksToFile).filename)
	blocks := commands[1].(AdjustmentCommandApplyBlocksToFile).blocks
	assert.Len(t, blocks, 1)
	assert.Equal(t, []byte("abc"), blocks[0].(ContentBlock).Content())
}

func TestFilesComparator_RemoveOnlyOne(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
	}
	serverHashedFiles := []HashedFile{
		{"a", nil, nil},
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 1)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandRemoveFile).filename)
}

func TestFilesComparator_AddOneToEmpty(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		{"a", strings.NewReader("abc")},
	}
	serverHashedFiles := []HashedFile{
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 1)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandApplyBlocksToFile).filename)
}

func TestFilesComparator_AddOneToNonEmpty(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		makeClientFile("a", "abc"),
		makeClientFile("b", "1234"),
	}
	serverHashedFiles := []HashedFile{
		makeServerFile(blockSize, "b", "1234"),
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 1)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandApplyBlocksToFile).filename)
}

func TestFilesComparator_InsertAndAppendContent(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		makeClientFile("a", "123abcd"),
		makeClientFile("b", "abcd123"),
	}
	serverHashedFiles := []HashedFile{
		makeServerFile(blockSize, "a", "abcd"),
		makeServerFile(blockSize, "b", "abcd"),
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 2)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandApplyBlocksToFile).filename)
	assert.Equal(t, "b", commands[1].(AdjustmentCommandApplyBlocksToFile).filename)

	blocks1 := commands[0].(AdjustmentCommandApplyBlocksToFile).blocks
	blocks2 := commands[1].(AdjustmentCommandApplyBlocksToFile).blocks

	assert.Len(t, blocks1, 2)
	assert.Equal(t, []byte("123"), blocks1[0].(ContentBlock).Content())
	_ = blocks1[1].(HashedBlock)

	assert.Len(t, blocks2, 2)
	_ = blocks2[0].(HashedBlock)
	assert.Equal(t, []byte("123"), blocks2[1].(ContentBlock).Content())
}
