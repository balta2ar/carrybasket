package main

import (
	"github.com/balta2ar/carrybasket"
	"github.com/urfave/cli"
	"log"
	"os"
	"time"
)

func action(c *cli.Context) error {
	targetDir := c.Args().Get(0)
	log.Printf("targetDir %v\n", targetDir)
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		log.Fatalln("Please specify an existing target dir")
	}
	log.Printf("command %v\n", targetDir)
	log.Println("starting")

	blockSize := 64 * 1024
	fs := carrybasket.NewActualFilesystem(".")
	address := "localhost:20000"

	log.Printf(
		"starting client: blockSize %v, targetDir %v, address %v (pid %v)\n",
		blockSize, targetDir, address, os.Getpid(),
	)
	os.Chdir(targetDir)
	client := carrybasket.NewSyncServiceClient(blockSize, targetDir, fs, address)
	err := client.Dial()
	if err != nil {
		log.Fatalf("dial error: %v\n", err)
	}
	defer client.Close()

	// one-time sync in the beginning
	if err := client.SyncCycle(); err != nil {
		log.Fatalf("client sync error: %v\n", err)
	}

	changeHandler := carrybasket.NewChangeHandler(client)
	fileWatcher := carrybasket.NewActualFileEventWatcher(".")
	events := make(chan carrybasket.ChangeEvent, 0)
	syncCycleDone := make(chan struct{}, 0)

	go func() {
		for {
			select {
			case <-syncCycleDone:
				log.Println("main routine: sync cycle done")
			}
		}
	}()

	changeHandler.Watch(events, syncCycleDone)
	fileWatcher.Watch(events, time.Second*1)

	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "carrybasket_client"
	app.Usage = "Run carrybasket client"
	app.Action = action

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

}
