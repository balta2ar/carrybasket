package main

//go:generate protoc -I=.. --go_out=plugins=grpc:../rpc ../sync_service.proto

import (
	"log"
	"os"

	"github.com/balta2ar/carrybasket"
	"github.com/urfave/cli"
)

func action(c *cli.Context) error {
	targetDir := c.Args().Get(0)
	log.Printf("targetDir %v\n", targetDir)
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		log.Fatalln("Please specify an existing target dir")
	}

	blockSize := 64 * 1024
	fs := carrybasket.NewActualFilesystem()
	address := "localhost:20000"

	log.Printf(
		"starting server: blockSize %v, targetDir %v, address %v\n",
		blockSize, targetDir, address,
	)
	os.Chdir(targetDir)
	server := carrybasket.NewSyncServiceServer(blockSize, targetDir, fs, address)
	err := server.Serve()
	if err != nil {
		log.Fatalf("server serve error: %v\n", err)
	}

	log.Println("server done")
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
