package carrybasket

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
)

type filesystemSandbox struct {
	rootDir        string
	lastCurrentDir string
}

func NewFilesystemSandbox(rootDir string) *filesystemSandbox {
	wd, err := os.Getwd()
	if err != nil {
		panic("could not get current dir")
	}
	sandbox := &filesystemSandbox{
		rootDir:        rootDir,
		lastCurrentDir: wd,
	}
	if err := os.RemoveAll(rootDir); err != nil {
		panic("cannot remove root dir")
	}
	if err := os.MkdirAll(rootDir, os.ModeDir|0755); err != nil {
		panic("cannot make root dir")
	}
	if err := os.Chdir(rootDir); err != nil {
		panic("cannot cd into root dir")
	}
	return sandbox
}

func (fs *filesystemSandbox) Cleanup() error {
	if err := os.Chdir(fs.lastCurrentDir); err != nil {
		panic("cannot cd into last current dir")
	}

	return os.RemoveAll(fs.rootDir)
}

func runClientServerCycle(t *testing.T, client *syncServiceClient, server *syncServiceServer) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := server.Serve(); err != nil {
			t.Fatalf("server serve failed: %v\n", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := client.Dial(); err != nil {
			t.Fatalf("client dial error: %v\n", err)
		}
		defer client.Close()
		if err := client.PullHashedFiles(); err != nil {
			t.Fatalf("client pull error: %v\n", err)
		}
		if err := client.PushAdjustmentCommands(); err != nil {
			t.Fatalf("client push error: %v\n", err)
		}
		server.Stop()
	}()

	wg.Wait()
}

type clientServerRunner struct {
	wg     sync.WaitGroup
	client *syncServiceClient
	server *syncServiceServer
}

func NewClientServerRunner(client *syncServiceClient, server *syncServiceServer) *clientServerRunner {
	runner := &clientServerRunner{
		client: client,
		server: server,
	}
	runner.wg.Add(2)
	return runner
}

func (csr *clientServerRunner) StartServer() {
	go func() {
		defer csr.wg.Done()
		if err := csr.server.Serve(); err != nil {
			panic(fmt.Sprintf("server serve failed: %v\n", err))
		}
	}()
}

func (csr *clientServerRunner) DialClient() {
	if err := csr.client.Dial(); err != nil {
		panic(fmt.Sprintf( "client dial error: %v\n", err))
	}
}

func (csr *clientServerRunner) Stop() {
	csr.client.Close()
	csr.server.Stop()
	csr.wg.Done() // client's part
	csr.wg.Wait()
}

func assertSyncOnline(
	t *testing.T,
	blockSize int,
	clientFs VirtualFilesystem,
	serverFs VirtualFilesystem,
	clientFiles []File,
	serverFiles []File,
) (*syncServiceClient, *syncServiceServer) {
	address := "localhost:20000"
	serverDir := "server"
	clientDir := "client"

	createFiles(clientFs, clientFiles)
	createFiles(serverFs, serverFiles)

	server := NewSyncServiceServer(blockSize, serverDir, serverFs, address)
	client := NewSyncServiceClient(blockSize, clientDir, clientFs, address)

	runClientServerCycle(t, client, server)
	assertFilesystemsEqual(t, clientFs, serverFs)

	return client, server
}

func TestSync_VirtualFilesystem(t *testing.T) {
	blockSize := 4
	clientFiles := []File{
		{"a", true, ""},
		{"a/1", false, "XXXXaaaa1234"},
		{"a/2", false, "bbbbXXXX2345"},
		{"a/3", false, "aaaa1234XXXX"},
		{"c", true, ""},
		{"c/1", false, "abc"},
	}
	serverFiles := []File{
		{"a", true, ""},
		{"a/1", false, "aaaa1234"},
		{"a/2", false, "bbbb2345"},
		{"a/3", false, "aaaa1234"},
		{"b", true, ""},
		{"b/1", false, "123"},
	}

	assertSyncOnline(
		t, blockSize,
		NewLoggingFilesystem(), NewLoggingFilesystem(),
		clientFiles, serverFiles,
	)
}

func TestSync_ActualFilesystem(t *testing.T) {
	sandbox := NewFilesystemSandbox("sandbox")
	defer sandbox.Cleanup()

	blockSize := 4
	clientFiles := []File{
		{"a", true, ""},
		{"a/1", false, "XXXXaaaa1234"},
		{"a/2", false, "bbbbXXXX2345"},
		{"a/3", false, "aaaa1234XXXX"},
		{"c", true, ""},
		{"c/1", false, "abc"},
	}
	serverFiles := []File{
		{"a", true, ""},
		{"a/1", false, "aaaa1234"},
		{"a/2", false, "bbbb2345"},
		{"a/3", false, "aaaa1234"},
		{"b", true, ""},
		{"b/1", false, "123"},
	}

	assertSyncOnline(
		t,
		blockSize,
		NewActualFilesystem("client"),
		NewActualFilesystem("server"),
		clientFiles,
		serverFiles,
	)

}

func TestSync_Watcher(t *testing.T) {
	blockSize := 4
	clientFiles := []File{
		{"a", true, ""},
		{"a/1", false, "XXXXaaaa1234"},
	}
	serverFiles := []File{
		{"a", true, ""},
		{"a/1", false, "aaaa1234"},
	}

	serverFs := NewLoggingFilesystem()
	clientFs := NewLoggingFilesystem()

	createFiles(clientFs, clientFiles)
	createFiles(serverFs, serverFiles)

	address := "localhost:20000"
	serverDir := "server"
	clientDir := "client"

	server := NewSyncServiceServer(blockSize, serverDir, serverFs, address)
	client := NewSyncServiceClient(blockSize, clientDir, clientFs, address)
	runner := NewClientServerRunner(client, server)
	runner.StartServer()
	runner.DialClient()

	client.SyncCycle()
	assertFilesystemsEqual(t, clientFs, serverFs)

	watcher := NewWatcherSyncServiceClient(client)
	events := make(chan ChangeEvent, 0)
	syncDone := make(chan struct{}, 0)
	watcher.Watch(events, syncDone)

	clientFs.Mkdir("b")
	events <- ChangeEvent{}
	<-syncDone
	assertFilesystemsEqual(t, clientFs, serverFs)

	clientFs.Delete("a")
	events <- ChangeEvent{}
	<-syncDone
	assertFilesystemsEqual(t, clientFs, serverFs)

	w, err := clientFs.OpenWrite("c")
	assert.Nil(t, err)
	w.Write([]byte("ccc"))
	events <- ChangeEvent{}
	<-syncDone
	assertFilesystemsEqual(t, clientFs, serverFs)

	runner.Stop()
}
