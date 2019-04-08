package main

import (
	"github.com/balta2ar/carrybasket"
	"log"
)

func main() {
	log.Println("starting")

	client := carrybasket.NewSyncServiceClient()
	err := client.Dial()
	if err != nil {
		log.Fatalf("dial error: %v\n", err)
	}
	err = client.PullHashedBlocks()
	if err != nil {
		log.Fatalf("pull error: %v\n", err)
	}

	err = client.PushAdjustmentCommands()
	if err != nil {
		log.Fatalf("push error: %v\n", err)
	}

	err = client.Close()
	if err != nil {
		log.Fatalf("close error: %v\n", err)
	}

	log.Println("done")

}
