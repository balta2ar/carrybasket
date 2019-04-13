package carrybasket

import (
	"crypto/md5"
	"github.com/stretchr/testify/assert"
	"hash"
	"io/ioutil"
	"strings"
	"testing"
)

func runComparator(
	blockSize int,
	clientFiles []VirtualFile,
	serverHashedFiles []HashedFile,
) []AdjustmentCommand {
	factory := NewProducerFactory(
		blockSize,
		func() hash.Hash32 { return NewMackerras(blockSize) },
		func() hash.Hash { return md5.New() },
	)
	comparator := NewFilesComparator(factory)
	commands := comparator.Compare(clientFiles, serverHashedFiles)
	return commands
}

func makeClientFile(filename string, isDir bool, content string) VirtualFile {
	return VirtualFile{filename, isDir, strings.NewReader(content)}
}

func makeServerFile(blockSize int, filename string, isDir bool, content string) HashedFile {
	_, hashedFile := makeServerFileAndGetContent(blockSize, filename, isDir, content)
	return hashedFile
}

func makeServerFileAndGetContent(
	blockSize int,
	filename string,
	isDir bool,
	content string,
) (HashGeneratorResult, HashedFile) {
	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()
	generator := NewHashGenerator(blockSize, fastHasher, strongHasher)
	result := generator.Scan(strings.NewReader(content))
	return result, HashedFile{
		filename,
		isDir,
		result.fastHashes,
		result.strongHashes,
	}
}

func TestFilesComparator_Smoke(t *testing.T) {
	blockSize := 4

	factory := NewProducerFactory(
		blockSize,
		func() hash.Hash32 { return NewMackerras(blockSize) },
		func() hash.Hash { return md5.New() },
	)
	comparator := NewFilesComparator(factory)
	commands := comparator.Compare(
		[]VirtualFile{},
		[]HashedFile{},
	)
	assert.Empty(t, commands)
}

func TestFilesComparator_RemoveOneOfTwoAndChange(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		{"b", false, strings.NewReader("abc")},
	}
	serverHashedFiles := []HashedFile{
		{"a", false, nil, nil},
		{"b", false, nil, nil},
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
	clientFiles := []VirtualFile{}
	serverHashedFiles := []HashedFile{
		{"a", false, nil, nil},
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 1)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandRemoveFile).filename)
}

func TestFilesComparator_AddAndRemove(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		makeClientFile("a", false, "abc"),
	}
	serverHashedFiles := []HashedFile{
		makeServerFile(blockSize, "b", false, "1234"),
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 2)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandApplyBlocksToFile).filename)
	assert.Equal(t, "b", commands[1].(AdjustmentCommandRemoveFile).filename)
}

func TestFilesComparator_AddOneToEmpty(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		{"a", false, strings.NewReader("abc")},
	}
	serverHashedFiles := []HashedFile{}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 1)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandApplyBlocksToFile).filename)
}

func TestFilesComparator_AddOneToNonEmpty(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		makeClientFile("a", false, "abc"),
		makeClientFile("b", false, "1234"),
	}
	serverHashedFiles := []HashedFile{
		makeServerFile(blockSize, "b", false, "1234"),
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 2)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandApplyBlocksToFile).filename)
	assert.Equal(t, "b", commands[1].(AdjustmentCommandApplyBlocksToFile).filename)
}

func TestFilesComparator_InsertAndAppendContent(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		makeClientFile("a", false, "123abcd"),
		makeClientFile("b", false, "abcd123"),
	}
	serverHashedFiles := []HashedFile{
		makeServerFile(blockSize, "a", false, "abcd"),
		makeServerFile(blockSize, "b", false, "abcd"),
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
	_ = blocks2[1].(HashedBlock)
}

func TestFilesComparator_MkOneNewDir(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		makeClientFile("a", true, ""),
	}
	serverHashedFiles := []HashedFile{}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 1)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandMkDir).filename)
}

func TestFilesComparator_RmOneDir(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{}
	serverHashedFiles := []HashedFile{
		makeServerFile(blockSize, "a", true, ""),
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 1)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandRemoveFile).filename)
}

func TestFilesComparator_MkOneDirAmongOthers(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		makeClientFile("a", true, ""),
		makeClientFile("b", true, ""),
	}
	serverHashedFiles := []HashedFile{
		makeServerFile(blockSize, "b", true, ""),
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 1)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandMkDir).filename)
}

func TestFilesComparator_RmDirAmongOthers(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		makeClientFile("a", true, ""),
	}
	serverHashedFiles := []HashedFile{
		makeServerFile(blockSize, "a", true, ""),
		makeServerFile(blockSize, "b", true, ""),
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 1)
	assert.Equal(t, "b", commands[0].(AdjustmentCommandRemoveFile).filename)
}

func TestFilesComparator_ReplaceDirWithFile(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		makeClientFile("a", false, "abcd"),
	}
	serverHashedFiles := []HashedFile{
		makeServerFile(blockSize, "a", true, ""),
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 2)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandRemoveFile).filename)
	assert.Equal(t, "a", commands[1].(AdjustmentCommandApplyBlocksToFile).filename)
}

func TestFilesComparator_ReplaceFileWithDir(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		makeClientFile("a", true, ""),
	}
	serverHashedFiles := []HashedFile{
		makeServerFile(blockSize, "a", false, "abcd"),
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)
	assert.Len(t, commands, 2)
	assert.Equal(t, "a", commands[0].(AdjustmentCommandRemoveFile).filename)
	assert.Equal(t, "a", commands[1].(AdjustmentCommandMkDir).filename)
}

func TestAdjustmentCommandApplier_Smoke(t *testing.T) {
	blockSize := 4
	clientContent := "abc1234def"
	serverContent := "1234"

	clientFiles := []VirtualFile{
		makeClientFile("a", false, clientContent),
	}
	generatorResult, file := makeServerFileAndGetContent(
		blockSize, "a", false, "1234",
	)
	serverHashedFiles := []HashedFile{
		file,
		makeServerFile(blockSize, "b", false, serverContent),
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)

	contentCache := NewBlockCache()
	contentCache.AddContents(
		generatorResult.strongHashes, generatorResult.contentBlocks,
	)
	reconstructor := NewContentReconstructor(md5.New(), contentCache)

	fs := NewLoggingFilesystem()
	w, err := fs.OpenWrite("b")
	assert.Nil(t, err)
	n, err := w.Write([]byte(serverContent))
	assert.Nil(t, err)
	assert.Equal(t, len(serverContent), n)

	applier := NewAdjustmentCommandApplier()
	err = applier.Apply(commands, fs, reconstructor)
	assert.Nil(t, err)

	r, err := fs.OpenRead("a")
	result, err := ioutil.ReadAll(r)
	assert.Nil(t, err)
	assert.Equal(t, clientContent, string(result))

	filenames, err := fs.ListAll()
	assert.Nil(t, err)
	assert.Equal(t, []string{"a"}, filenames)
}

func TestAdjustmentCommandApplier_AddOneDir(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{
		makeClientFile("a", true, ""),
	}
	serverHashedFiles := []HashedFile{}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)

	contentCache := NewBlockCache()
	reconstructor := NewContentReconstructor(md5.New(), contentCache)

	fs := NewLoggingFilesystem()
	applier := NewAdjustmentCommandApplier()
	err := applier.Apply(commands, fs, reconstructor)
	assert.Nil(t, err)

	filenames, err := fs.ListAll()
	assert.Nil(t, err)
	assert.Equal(t, []string{"a"}, filenames)
	assert.True(t, fs.IsDir("a"))
}

func TestAdjustmentCommandApplier_RmOneDir(t *testing.T) {
	blockSize := 4
	clientFiles := []VirtualFile{}
	serverHashedFiles := []HashedFile{
		makeServerFile(blockSize, "b", true, ""),
	}
	commands := runComparator(blockSize, clientFiles, serverHashedFiles)

	contentCache := NewBlockCache()
	reconstructor := NewContentReconstructor(md5.New(), contentCache)

	fs := NewLoggingFilesystem()
	assert.Nil(t, fs.Mkdir("b"))
	applier := NewAdjustmentCommandApplier()
	err := applier.Apply(commands, fs, reconstructor)
	assert.Nil(t, err)

	filenames, err := fs.ListAll()
	assert.Nil(t, err)
	assert.Equal(t, []string{}, filenames)
}
