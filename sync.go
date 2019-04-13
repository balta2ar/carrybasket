package carrybasket

import (
	"context"
	"crypto/md5"
	"github.com/pkg/errors"
	"hash"
	"io"
	"log"
	"net"

	pb "github.com/balta2ar/carrybasket/rpc"
	"google.golang.org/grpc"
)

type SyncServiceClient interface {
	SyncCycle() error
}

//
// Server
//

type syncServiceServer struct {
	blockSize int
	targetDir string
	fs        VirtualFilesystem
	address   string

	contentCache BlockCache
	rpcServer    *grpc.Server
}

func NewSyncServiceServer(
	blockSize int,
	targetDir string,
	fs VirtualFilesystem,
	address string,
) *syncServiceServer {
	return &syncServiceServer{
		blockSize: blockSize,
		targetDir: targetDir,
		fs:        fs,
		address:   address,

		contentCache: NewBlockCache(),
	}
}

func (s *syncServiceServer) PullHashedFiles(
	empty *pb.ProtoEmpty,
	stream pb.SyncService_PullHashedFilesServer,
) error {

	makeFastHash := func() hash.Hash32 { return NewMackerras(s.blockSize) }
	makeStrongHash := func() hash.Hash { return md5.New() }

	fastHasher := makeFastHash()
	strongHasher := makeStrongHash()
	generator := NewHashGenerator(s.blockSize, fastHasher, strongHasher)

	listedServerFiles, err := ListServerFiles(s.fs, generator, s.contentCache)
	if err != nil {
		return err
	}

	log.Println("sending hashed files")

	for _, serverFile := range listedServerFiles {
		protoHashedFile := serverFile.asProtoHashedFile()
		log.Printf("sending %v\n", serverFile.Filename)
		err := stream.Send(&protoHashedFile)
		if err != nil {
			log.Printf("send error: %v\n", err)
			return err
		}
	}

	return nil
}

func (s *syncServiceServer) PushAdjustmentCommands(
	stream pb.SyncService_PushAdjustmentCommandsServer,
) error {
	commands := make([]AdjustmentCommand, 0)

	for {

		protoCommand, err := stream.Recv()
		if err == io.EOF {
			log.Println("EOF, done")
			err = stream.SendAndClose(&pb.ProtoEmpty{})
			if err != nil {
				log.Printf("send and close error: %v\n", err)
				return err
			}
			break
		}

		if err != nil {
			log.Printf("recv error: %v\n", err)
			return err
		}
		log.Printf(
			"received protoCommand for filename: %v\n",
			protoCommand.Filename,
		)

		command := protoAdjustmentCommandasAdjustmentCommand(protoCommand)
		commands = append(commands, command)
	}

	makeStrongHash := func() hash.Hash { return md5.New() }
	strongHasher := makeStrongHash()

	reconstructor := NewContentReconstructor(strongHasher, s.contentCache)
	applier := NewAdjustmentCommandApplier()
	err := applier.Apply(commands, s.fs, reconstructor)
	if err != nil {
		log.Printf("error applying commands: %v\n", err)
		return err
	}
	return nil
}

func (s *syncServiceServer) Serve() error {
	log.Println("server starting")

	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		log.Printf("server cannot listen: %v\n", err)
		return err
	}
	log.Printf("sever listening on %v...\n", s.address)

	s.rpcServer = grpc.NewServer()
	pb.RegisterSyncServiceServer(s.rpcServer, s)
	err = s.rpcServer.Serve(listener)
	if err != nil {
		log.Printf("server error: %v\n", err)
		return err
	}

	log.Println("server done")
	return nil
}

func (s *syncServiceServer) Stop() {
	s.rpcServer.GracefulStop()
}

//
// Client
//

type syncServiceClient struct {
	blockSize int
	targetDir string
	fs        VirtualFilesystem
	address   string

	connection *grpc.ClientConn
	client     pb.SyncServiceClient

	serverHashedFiles []HashedFile
}

func NewSyncServiceClient(
	blockSize int,
	targetDir string,
	fs VirtualFilesystem,
	address string,
) *syncServiceClient {
	return &syncServiceClient{
		blockSize: blockSize,
		targetDir: targetDir,
		fs:        fs,
		address:   address,

		serverHashedFiles: make([]HashedFile, 0),
	}
}

func (c *syncServiceClient) Reset() {
	c.serverHashedFiles = make([]HashedFile, 0)
}

func (c *syncServiceClient) Dial() error {
	connection, err := grpc.Dial(c.address, grpc.WithInsecure())
	if err != nil {
		log.Printf("dial error: %v\n", err)
		return err
	}

	client := pb.NewSyncServiceClient(connection)

	c.connection = connection
	c.client = client
	return nil
}

func (c *syncServiceClient) Close() error {
	return c.connection.Close()
}

func (c *syncServiceClient) PullHashedFiles() error {
	c.Reset()

	pullStream, err := c.client.PullHashedFiles(context.Background(), &pb.ProtoEmpty{})

	if err != nil {
		log.Fatalf("error receiving pullStream: %v\n", err)
	}

	for {
		protoHashedFile, err := pullStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("recv error %v\n", err)
			return err
		}
		log.Printf(
			"received hashed file for filename: %v\n",
			protoHashedFile.Filename,
		)

		hashedFile := protoHashedFileAsHashedFile(protoHashedFile)
		c.serverHashedFiles = append(c.serverHashedFiles, hashedFile)
	}
	return nil
}

func (c *syncServiceClient) PushAdjustmentCommands() error {
	makeFastHash := func() hash.Hash32 { return NewMackerras(c.blockSize) }
	makeStrongHash := func() hash.Hash { return md5.New() }

	listedClientFiles, err := ListClientFiles(c.fs)
	log.Printf("client listed %d files\n", len(listedClientFiles))

	factory := NewProducerFactory(c.blockSize, makeFastHash, makeStrongHash)
	comparator := NewFilesComparator(factory)
	log.Println("comparing files...")
	commands := comparator.Compare(listedClientFiles, c.serverHashedFiles)

	pushStream, err := c.client.PushAdjustmentCommands(context.Background())
	if err != nil {
		log.Fatalf("push error: %v\n", err)
	}

	log.Println("pushing commands...")
	for _, abstractCommand := range commands {

		protoCommand := adjustmentCommandAsProtoAdjustmentCommand(abstractCommand)
		err = pushStream.Send(&protoCommand)
		if err == io.EOF {
			log.Printf("push EOF")
		} else if err != nil {
			log.Printf("push stream send error: %v\n", err)
			return err
		}
	}

	reply, err := pushStream.CloseAndRecv()
	if err != nil {
		log.Printf("error closing: %v\n", err)
		return nil
	}
	log.Printf("reply: %v\n", reply)

	return nil
}

func (c *syncServiceClient) SyncCycle() error {
	log.Println("sync cycle: pulling...")
	err := c.PullHashedFiles()
	if err != nil {
		return errors.Wrap(err, "sync cycle: pull error: %v")
	}
	log.Println("sync cycle: pull done")

	log.Println("sync cycle: pushing...")
	err = c.PushAdjustmentCommands()
	if err != nil {
		return errors.Wrap(err, "sync cycle: push error: %v")
	}

	log.Println("sync cycle: push done")
	return nil
}
