package main

import (
	"github.com/balta2ar/carrybasket"
	"github.com/urfave/cli"
	"log"
	"os"
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
	fs := carrybasket.NewActualFilesystem()
	address := "localhost:20000"

	log.Printf(
		"starting client: blockSize %v, targetDir %v, address %v\n",
		blockSize, targetDir, address,
	)
	os.Chdir(targetDir)
	client := carrybasket.NewSyncServiceClient(blockSize, targetDir, fs, address)
	err := client.Dial()
	if err != nil {
		log.Fatalf("dial error: %v\n", err)
	}
	defer client.Close()
	log.Println("pulling...")
	err = client.PullHashedFiles()
	if err != nil {
		log.Fatalf("pull error: %v\n", err)
	}
	log.Println("pull done")

	log.Println("pushing...")
	err = client.PushAdjustmentCommands()
	if err != nil {
		log.Fatalf("push error: %v\n", err)
	}

	log.Println("push done")
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
