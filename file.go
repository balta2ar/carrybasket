package main

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"sort"
)

type HashedFile struct {
	Filename     string
	IsDir        bool
	FastHashes   []Block
	StrongHashes []Block
}

/// Entity that is used to abstract away from the actual file system
type VirtualFile struct {
	Filename string
	IsDir    bool
	Rw       io.ReadSeeker
	// TODO: think how you will handle server writes
	//Rw       io.ReadWriteSeeker
}

type OpenMode int

const (
	OpenModeRead OpenMode = iota
	OpenModeWrite
)

type VirtualFilesystem interface {
	Move(sourceFilename string, destFilename string) error
	Delete(filename string) error
	Open(filename string) (io.ReadWriter, error)
	IsPath(filename string) bool
	IsDir(filename string) bool
	Mkdir(filename string) error
	ListAll() ([]string, error)
}

type loggingFilesystem struct {
	Actions []string                 /// actions recorded after calls to the filesystem
	storage map[string]*bytes.Buffer /// internal storage for filenames and data
}

func NewLoggingFilesystem() *loggingFilesystem {
	return &loggingFilesystem{
		Actions: make([]string, 0),
		storage: make(map[string]*bytes.Buffer),
	}
}

func (lf *loggingFilesystem) Move(sourceFilename string, destFilename string) error {
	lf.Actions = append(lf.Actions, fmt.Sprintf("move %v %v", sourceFilename, destFilename))
	if _, ok := lf.storage[sourceFilename]; !ok {
		return errors.New("source file does not exit")
	}
	if _, ok := lf.storage[destFilename]; ok {
		return errors.New("destination file exists")
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

func (lf *loggingFilesystem) Open(filename string) (io.ReadWriter, error) {
	lf.Actions = append(lf.Actions, fmt.Sprintf("open %v", filename))
	rw, ok := lf.storage[filename]
	if ok {
		if rw == nil {
			return nil, errors.New("file is a directory")
		}

		// Multiple concurrent readers and writers are not supported
		// in this virtual filesystem.
		// This is a terrible thing to do, but it's fine since this
		// concrete implementation is only supposed to be used in
		// tests.
		buffer := bytes.NewBuffer(rw.Bytes())
		lf.storage[filename] = buffer
		return rw, nil
	}

	buffer := bytes.NewBuffer(nil)
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
	//return ok && strings.HasSuffix(filename, "/")
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

type actualFilesystem struct{}

func NewActualFilesystem() *actualFilesystem {
	return &actualFilesystem{}
}

func (lf *actualFilesystem) Move(sourceFilename string, destFilename string) error {
	return os.Rename(sourceFilename, destFilename)
}

func (lf *actualFilesystem) Delete(filename string) error {
	return os.Remove(filename)
}

func (lf *actualFilesystem) Open(filename string) (io.ReadWriter, error) {
	return os.Open(filename)
}

func (lf *actualFilesystem) IsPath(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}

func (lf *actualFilesystem) IsDir(filename string) bool {
	stat, err := os.Stat(filename)
	return (err == nil) && stat.IsDir()
}

func (lf *actualFilesystem) Mkdir(filename string) error {
	return os.MkdirAll(filename, os.ModeDir)
}

func (lf *actualFilesystem) ListAll() ([]string, error) {
	filenames := make([]string, 0)
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path != "." {
			filenames = append(filenames, path)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
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
			_, err := fs.Open(filename)
			if err != nil {
				return nil, errors.Wrap(err, "cannot open file")
			}
			clientFiles = append(clientFiles, VirtualFile{
				Filename: filename,
				IsDir:    false,
				Rw:       nil, //rw,
			})
		}
	}

	return clientFiles, nil
}

func ListServerFiles(fs VirtualFilesystem) ([]HashedFile, error) {
	return nil, nil
}
