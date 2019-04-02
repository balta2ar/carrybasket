package main

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
)

type HashedFile struct {
	Filename     string
	FastHashes   []Block
	StrongHashes []Block
}

/// Entity that is used to abstract away from the actual file system
type VirtualFile struct {
	Filename string
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
	//OpenRead(filename string) io.Reader
	//OpenWrite(filename string) io.Writer
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
