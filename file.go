package carrybasket

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Server-side representation of a file
type HashedFile struct {
	Filename     string
	IsDir        bool
	FastHashes   []Block
	StrongHashes []Block
}

/// Client-side representation of a file
type VirtualFile struct {
	Filename string
	IsDir    bool
	Rw       io.Reader
}

type VirtualFilesystem interface {
	Move(sourceFilename string, destFilename string) error
	Delete(filename string) error
	OpenRead(filename string) (io.Reader, error)
	OpenWrite(filename string) (io.Writer, error)
	IsPath(filename string) bool
	IsDir(filename string) bool
	Mkdir(filename string) error
	ListAll() ([]string, error)
}

type loggingFilesystem struct {
	Actions []string                    /// actions recorded after calls to the filesystem
	storage map[string]*strings.Builder /// internal storage for filenames and data
}

func NewLoggingFilesystem() *loggingFilesystem {
	return &loggingFilesystem{
		Actions: make([]string, 0),
		storage: make(map[string]*strings.Builder),
	}
}

func (lf *loggingFilesystem) Move(sourceFilename string, destFilename string) error {
	lf.Actions = append(lf.Actions, fmt.Sprintf("move %v %v", sourceFilename, destFilename))
	if _, ok := lf.storage[sourceFilename]; !ok {
		return errors.New("source file does not exit")
	}
	rw, ok := lf.storage[destFilename]
	if (rw == nil) && ok {
		return errors.New("destination is a directory")
	}
	lf.storage[destFilename] = lf.storage[sourceFilename]
	delete(lf.storage, sourceFilename)
	return nil
}

func (lf *loggingFilesystem) Delete(filename string) error {
	lf.Actions = append(lf.Actions, fmt.Sprintf("delete %v", filename))
	if _, ok := lf.storage[filename]; ok {
		delete(lf.storage, filename)
		return nil
	}
	return errors.New("file does not exist")
}

func (lf *loggingFilesystem) OpenRead(filename string) (io.Reader, error) {
	lf.Actions = append(lf.Actions, fmt.Sprintf("openread %v", filename))
	r, ok := lf.storage[filename]
	if !ok {
		return nil, errors.New("file does not exist")
	}

	if r == nil {
		return nil, errors.New("file is a directory")
	}

	// Multiple concurrent readers and writers are not supported
	// in this virtual filesystem.
	// This is a terrible thing to do, but it's fine since this
	// concrete implementation is only supposed to be used in
	// tests.
	return strings.NewReader(r.String()), nil
}

func (lf *loggingFilesystem) OpenWrite(filename string) (io.Writer, error) {
	lf.Actions = append(lf.Actions, fmt.Sprintf("openwrite %v", filename))
	rw, ok := lf.storage[filename]
	if ok && (rw == nil) {
		return nil, errors.New("file is a directory")
	}

	buffer := &strings.Builder{}
	lf.storage[filename] = buffer
	return buffer, nil

}

func (lf *loggingFilesystem) IsPath(filename string) bool {
	lf.Actions = append(lf.Actions, fmt.Sprintf("ispath %v", filename))
	_, ok := lf.storage[filename]
	return ok
}

func (lf *loggingFilesystem) IsDir(filename string) bool {
	lf.Actions = append(lf.Actions, fmt.Sprintf("isdir %v", filename))
	rw, ok := lf.storage[filename]
	return (rw == nil) && ok
}

func (lf *loggingFilesystem) Mkdir(filename string) error {
	lf.Actions = append(lf.Actions, fmt.Sprintf("mkdir %v", filename))
	if _, ok := lf.storage[filename]; ok {
		return errors.New("file already exists")
	}
	lf.storage[filename] = nil
	return nil
}

func (lf *loggingFilesystem) ListAll() ([]string, error) {
	lf.Actions = append(lf.Actions, "listall")
	filenames := make([]string, 0, len(lf.storage))

	for filename := range lf.storage {
		filenames = append(filenames, filename)
	}
	sort.Strings(filenames)

	return filenames, nil
}

type actualFilesystem struct {
	prefix string
}

func NewActualFilesystem(prefix string) *actualFilesystem {
	return &actualFilesystem{
		prefix: prefix,
	}
}

func (lf *actualFilesystem) prefixed(filename string) string {
	return filepath.Join(lf.prefix, filename)
}

func (lf *actualFilesystem) unprefixed(filename string) string {
	if strings.HasPrefix(filename, lf.prefix) {
		return filename[len(lf.prefix)+1:]
	}
	return filename
}

func (lf *actualFilesystem) Move(sourceFilename string, destFilename string) error {
	return os.Rename(
		lf.prefixed(sourceFilename),
		lf.prefixed(destFilename),
	)
}

func (lf *actualFilesystem) Delete(filename string) error {
	return os.RemoveAll(lf.prefixed(filename))
}

func (lf *actualFilesystem) OpenRead(filename string) (io.Reader, error) {
	return os.Open(lf.prefixed(filename))
}

func (lf *actualFilesystem) OpenWrite(filename string) (io.Writer, error) {
	if err := os.MkdirAll(
		filepath.Dir(lf.prefixed(filename)),
		os.ModeDir|0755,
	); err != nil {
		return nil, err
	}
	return os.Create(lf.prefixed(filename))
}

func (lf *actualFilesystem) IsPath(filename string) bool {
	if _, err := os.Stat(lf.prefixed(filename)); os.IsNotExist(err) {
		return false
	}
	return true
}

func (lf *actualFilesystem) IsDir(filename string) bool {
	stat, err := os.Stat(lf.prefixed(filename))
	return (err == nil) && stat.IsDir()
}

func (lf *actualFilesystem) Mkdir(filename string) error {
	return os.MkdirAll(lf.prefixed(filename), os.ModeDir|0755)
}

func (lf *actualFilesystem) ListAll() ([]string, error) {
	filenames := make([]string, 0)
	err := filepath.Walk(lf.prefixed("."), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path != "." && path != lf.prefix {
			filenames = append(filenames, lf.unprefixed(path))
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	sort.Strings(filenames)
	return filenames, nil
}

func ListClientFiles(fs VirtualFilesystem) ([]VirtualFile, error) {
	filenames, err := fs.ListAll()
	if err != nil {
		return nil, errors.Wrap(err, "cannot list filesystem")
	}
	clientFiles := make([]VirtualFile, 0, len(filenames))

	for _, filename := range filenames {
		if fs.IsDir(filename) {
			clientFiles = append(clientFiles, VirtualFile{
				Filename: filename,
				IsDir:    true,
				Rw:       nil,
			})
		} else {
			r, err := fs.OpenRead(filename)
			if err != nil {
				return nil, errors.Wrap(err, "cannot open file")
			}
			clientFiles = append(clientFiles, VirtualFile{
				Filename: filename,
				IsDir:    false,
				Rw:       r,
			})
		}
	}

	return clientFiles, nil
}

func ListServerFiles(
	fs VirtualFilesystem,
	generator HashGenerator,
	contentCache BlockCache,
) ([]HashedFile, error) {
	filenames, err := fs.ListAll()
	if err != nil {
		return nil, errors.Wrap(err, "cannot list filesystem")
	}
	serverFiles := make([]HashedFile, 0, len(filenames))

	for _, filename := range filenames {
		if fs.IsDir(filename) {
			serverFiles = append(serverFiles, HashedFile{
				Filename:     filename,
				IsDir:        true,
				FastHashes:   nil,
				StrongHashes: nil,
			})
		} else {
			r, err := fs.OpenRead(filename)
			if err != nil {
				return nil, errors.Wrap(err, "cannot open file")
			}
			generator.Reset()
			generatorResult := generator.Scan(r)
			serverFiles = append(serverFiles, HashedFile{
				Filename:     filename,
				IsDir:        false,
				FastHashes:   generatorResult.fastHashes,
				StrongHashes: generatorResult.strongHashes,
			})
			if contentCache != nil {
				contentCache.AddContents(
					generatorResult.strongHashes, generatorResult.contentBlocks)
			}
		}
	}

	return serverFiles, nil
}
