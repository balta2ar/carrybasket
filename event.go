package carrybasket

import (
	"fmt"
	"github.com/radovskyb/watcher"
)

type ChangeEvent struct {
}

type actualFileEventWatcher struct {
	rootDir string
	watcher *watcher.Watcher
}

func NewActualFileEventWatcher(rootDir string) *actualFileEventWatcher {
	return &actualFileEventWatcher{
		rootDir: rootDir,
		watcher: watcher.New(),
	}
}

func (ew *actualFileEventWatcher) Watch(eventSink chan<- ChangeEvent) {
	ew.watcher.SetMaxEvents(1)
	ew.watcher.FilterOps(
		watcher.Create,
		watcher.Remove,
		watcher.Rename,
		watcher.Write,
	)
}

type WatcherSyncServiceClient interface {
	Watch(eventSource <-chan ChangeEvent)
}

type watcherSyncServiceClient struct {
	syncClient SyncServiceClient
}

func NewWatcherSyncServiceClient(syncClient SyncServiceClient) *watcherSyncServiceClient {
	return &watcherSyncServiceClient{
		syncClient: syncClient,
	}
}

func (c *watcherSyncServiceClient) Watch(
	eventSource <-chan ChangeEvent,
	syncDone chan<- struct{},
) {
	//return
	go func() {
		for {
			select {
			case <-eventSource:
				if err := c.syncClient.SyncCycle(); err != nil {
					close(syncDone)
					panic(fmt.Sprintf("watcher: sync cycle error: %v\n", err))
				}
				syncDone <- struct{}{}
			}
		}
	}()
}
