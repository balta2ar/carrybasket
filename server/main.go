package main

//go:generate protoc -I=.. --go_out=plugins=grpc:../rpc ../sync_service.proto

import (
	pb "github.com/balta2ar/carrybasket/rpc"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
)

type syncServiceServer struct {
}

func (s *syncServiceServer) PullHashedBlocks(
	empty *pb.ProtoEmpty,
	stream pb.SyncService_PullHashedBlocksServer,
) error {
	fastHashBlock := pb.ProtoBlock{
		Type:    pb.ProtoBlockType_HASHED,
		Offset:  0,
		Size:    4,
		Hashsum: []byte("123"),
		Content: []byte(""),
	}
	hashedFile := pb.ProtoHashedFile{
		Filename:     "file1",
		IsDir:        false,
		FastHashes:   []*pb.ProtoBlock{&fastHashBlock},
		StrongHashes: []*pb.ProtoBlock{&fastHashBlock},
	}

	log.Println("sending")
	err := stream.Send(&hashedFile)
	if err != nil {
		log.Printf("error sending: %v\n", err)
	}

	return nil
}

func (s *syncServiceServer) PushHashedAndContentBlocks(
	stream pb.SyncService_PushHashedAndContentBlocksServer,
) error {

	for {

		command, err := stream.Recv()
		if err == io.EOF {
			log.Println("EOF, done")
			return stream.SendAndClose(&pb.ProtoEmpty{})
		}

		if err != nil {
			log.Println("recv error: %v\n", err)
			return err
		}

		log.Printf("received block: %v\n", command)

	}
}

func main() {
	log.Println("starting")

	listener, err := net.Listen("tcp", ":20000")
	if err != nil {
		log.Fatal("cannot listen: %v\n", err)
	}
	log.Println("listening on 20000...")

	rpcServer := grpc.NewServer()
	pb.RegisterSyncServiceServer(rpcServer, &syncServiceServer{})
	err = rpcServer.Serve(listener)
	if err != nil {
		log.Fatalf("server error: %v\n", err)
	}

	log.Println("done")
}
