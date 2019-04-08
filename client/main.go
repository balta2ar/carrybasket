package main

import (
	"context"
	"google.golang.org/grpc"
	"io"
	"log"
	pb "github.com/balta2ar/carrybasket/rpc"
)

func main() {
	connection, err := grpc.Dial("localhost:20000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("dial error: %v\n", err)
	}
	defer connection.Close()

	client := pb.NewSyncServiceClient(connection)

	pullStream, err := client.PullHashedBlocks(context.Background(), &pb.ProtoEmpty{})
	if err != nil {
		log.Fatalf("error receiving pullStream: %v\n", err)
	}

	for {
		hashedFile, err := pullStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("recv error %v\n", err)
		}
		log.Printf("received hashed file: %v\n", hashedFile)
	}

	pushStream, err := client.PushHashedAndContentBlocks(context.Background())
	if err != nil {
		log.Fatalf("push error: %v\n", err)
	}

	hashedBlock := pb.ProtoBlock{
		Type:    pb.ProtoBlockType_HASHED,
		Offset:  0,
		Size:    4,
		Hashsum: []byte("hashsum"),
		Content: []byte(""),
	}
	contentBlock := pb.ProtoBlock{
		Type:    pb.ProtoBlockType_CONTENT,
		Offset:  0,
		Size:    4,
		Hashsum: []byte(""),
		Content: []byte("content"),
	}

	command := pb.ProtoAdjustmentCommand{
		Type: pb.ProtoAdjustmentCommandType_APPLY_BLOCKS_TO_FILE,
		Filename: "file1",
		Blocks: []*pb.ProtoBlock{&hashedBlock, &contentBlock},
	}

	err = pushStream.Send(&command)
	if err == io.EOF {
		log.Printf("push EOF")
	} else if err != nil {
		log.Fatalf("push stream send error: %v\n", err)
	}
	reply, err := pushStream.CloseAndRecv()
	if err != nil {
		log.Fatalf("error closing: %v\n", err)
	}
	log.Printf("reply: %v\n", reply)

	//var command *pb.ProtoAdjustmentCommand

}