package carrybasket

import (
	"sync"
	"testing"
)

func TestSync_Smoke(t *testing.T) {
	blockSize := 4
	address := "localhost:20000"
	serverDir := "server"
	clientDir := "client"

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

	clientFs := makeFilesystem(clientFiles)
	serverFs := makeFilesystem(serverFiles)
	//serverFs := NewLoggingFilesystem()
	//clientFs := NewLoggingFilesystem()

	server := NewSyncServiceServer(blockSize, serverDir, serverFs, address)
	client := NewSyncServiceClient(blockSize, clientDir, clientFs, address)

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
	assertFilesystemsEqual(t, clientFs, serverFs)
}
