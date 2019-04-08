package main

//go:generate protoc -I=.. --go_out=plugins=grpc:../rpc ../sync_service.proto

import (
	"github.com/balta2ar/carrybasket"
	"log"
)

func main() {
	log.Println("starting")

	server := carrybasket.NewSyncServiceServer()
	err := server.Serve()
	if err != nil {
		log.Fatalf("serve error: %v\n", err)
	}

	log.Println("done")
}
