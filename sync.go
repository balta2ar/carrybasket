package carrybasket

//
// Server
//

import (
	"context"
	pb "github.com/balta2ar/carrybasket/rpc"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
)

type syncServiceServer struct {
}

func NewSyncServiceServer() *syncServiceServer {
	return &syncServiceServer{}
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

func (s *syncServiceServer) PushAdjustmentCommands(
	stream pb.SyncService_PushAdjustmentCommandsServer,
) error {

	for {

		command, err := stream.Recv()
		if err == io.EOF {
			log.Println("EOF, done")
			return stream.SendAndClose(&pb.ProtoEmpty{})
		}

		if err != nil {
			log.Printf("recv error: %v\n", err)
			return err
		}

		log.Printf("received block: %v\n", command)

	}
}

func (s *syncServiceServer) Serve() error {
	log.Println("starting")

	listener, err := net.Listen("tcp", "localhost:20000")
	if err != nil {
		log.Fatalf("cannot listen: %v\n", err)
	}
	log.Println("listening on 20000...")

	rpcServer := grpc.NewServer()
	pb.RegisterSyncServiceServer(rpcServer, &syncServiceServer{})
	err = rpcServer.Serve(listener)
	if err != nil {
		log.Fatalf("server error: %v\n", err)
	}

	log.Println("done")
	return nil
}

//
// Client
//

type syncServiceClient struct {
	connection *grpc.ClientConn
	client     pb.SyncServiceClient
}

func NewSyncServiceClient() *syncServiceClient {
	return &syncServiceClient{}
}

func (c *syncServiceClient) Dial() error {
	connection, err := grpc.Dial("localhost:20000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("dial error: %v\n", err)
	}

	client := pb.NewSyncServiceClient(connection)

	c.connection = connection
	c.client = client
	return nil
}

func (c *syncServiceClient) Close() error {
	return c.connection.Close()
}

func (c *syncServiceClient) PullHashedBlocks() error {

	pullStream, err := c.client.PullHashedBlocks(context.Background(), &pb.ProtoEmpty{})

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
	return nil
}

func (c *syncServiceClient) PushAdjustmentCommands() error {

	pushStream, err := c.client.PushAdjustmentCommands(context.Background())
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
		Type:     pb.ProtoAdjustmentCommandType_APPLY_BLOCKS_TO_FILE,
		Filename: "file1",
		Blocks:   []*pb.ProtoBlock{&hashedBlock, &contentBlock},
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

	return nil
}
