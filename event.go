package carrybasket

import (
	"fmt"
	"github.com/radovskyb/watcher"
	"log"
	"time"
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

func (ew *actualFileEventWatcher) Watch(
	eventSink chan<- ChangeEvent,
	duration time.Duration,
) {
	ew.watcher.SetMaxEvents(1)
	ew.watcher.FilterOps(
		watcher.Create,
		watcher.Remove,
		watcher.Rename,
		watcher.Write,
	)

	go func() {
		for {
			select {
			case event := <-ew.watcher.Event:
				log.Printf("event: %v\n", event)
				eventSink <- struct{}{}
				log.Println("event has been sent")
			case err := <-ew.watcher.Error:
				log.Printf("error: %v\n", err)
			case <-ew.watcher.Closed:
				log.Printf("closed")
				return
			}
		}
	}()

	if err := ew.watcher.AddRecursive(ew.rootDir); err != nil {
		log.Fatalf("error adding watch dir: %v\n", err)
	}

	if err := ew.watcher.Start(duration); err != nil {
		log.Fatalf("error starting watcher: %v\n", err)
	}
}

func (ew *actualFileEventWatcher) Wait() {
	ew.watcher.Wait()
}

type WatcherSyncServiceClient interface {
	Watch(eventSource <-chan ChangeEvent)
}

type changeHandler struct {
	syncClient SyncServiceClient
}

func NewChangeHandler(syncClient SyncServiceClient) *changeHandler {
	return &changeHandler{
		syncClient: syncClient,
	}
}

func (c *changeHandler) Watch(
	eventSource <-chan ChangeEvent,
	syncCycleDone chan<- struct{},
) {
	go func() {
		for {
			select {
			case <-eventSource:
				if err := c.syncClient.SyncCycle(); err != nil {
					close(syncCycleDone)
					panic(fmt.Sprintf("watcher: sync cycle error: %v\n", err))
				}
				syncCycleDone <- struct{}{}
			}
		}
	}()
}
