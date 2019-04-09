package main

//go:generate protoc -I=.. --go_out=plugins=grpc:../rpc ../sync_service.proto

import (
	"fmt"
	"log"
	"os"

	"github.com/balta2ar/carrybasket"
	"github.com/urfave/cli"
)

func action(c *cli.Context) error {
	targetDir := c.Args().Get(0)
	fmt.Printf("targetDir %v\n", targetDir)
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		log.Fatalln("Please speficy existing target dir")
	}
	fmt.Printf("command %v\n", targetDir)

	blockSize := 4
	server := carrybasket.NewSyncServiceServer(blockSize, targetDir)
	err = server.Serve()
	if err != nil {
		log.Fatalf("serve error: %v\n", err)
	}

	log.Println("done")

	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "carrybasket_server"
	app.Usage = "Run carrybasket server"
	app.Action = action

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
