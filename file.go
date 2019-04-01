package main

import (
	"fmt"
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
	Actions []string
}

func NewLoggingFilesystem() *loggingFilesystem {
	return &loggingFilesystem{}
}

func (lf *loggingFilesystem) Move(sourceFilename string, destFilename string) error {
	lf.Actions = append(lf.Actions, fmt.Sprintf("move %v %v", sourceFilename, destFilename))
	return nil
}

func (lf *loggingFilesystem) Delete(filename string) error {
	lf.Actions = append(lf.Actions, fmt.Sprintf("delete %v", filename))
	return nil
}

func (lf *loggingFilesystem) Open(filename string) (io.ReadWriter, error) {
	lf.Actions = append(lf.Actions, fmt.Sprintf("open %v", filename))
	return nil, nil
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
