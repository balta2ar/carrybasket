package carrybasket

import (
	"context"
	"crypto/md5"
	"hash"
	"io"
	"log"
	"net"

	pb "github.com/balta2ar/carrybasket/rpc"
	"google.golang.org/grpc"
)

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

func (hf *HashedFile) asProtoHashedFile() pb.ProtoHashedFile {
	protoHashedFile := pb.ProtoHashedFile{
		Filename:     hf.Filename,
		IsDir:        hf.IsDir,
		FastHashes:   []*pb.ProtoBlock{},
		StrongHashes: []*pb.ProtoBlock{},
	}

	for _, abstractBlock := range hf.FastHashes {
		block := abstractBlock.(HashedBlock)
		protoHashedFile.FastHashes = append(
			protoHashedFile.FastHashes,
			&pb.ProtoBlock{
				Type:    pb.ProtoBlockType_HASHED,
				Offset:  block.Offset(),
				Size:    block.Size(),
				Hashsum: block.HashSum(),
				Content: []byte{},
			})
	}
	for _, abstractBlock := range hf.StrongHashes {
		block := abstractBlock.(HashedBlock)
		protoHashedFile.StrongHashes = append(
			protoHashedFile.StrongHashes,
			&pb.ProtoBlock{
				Type:    pb.ProtoBlockType_HASHED,
				Offset:  block.Offset(),
				Size:    block.Size(),
				Hashsum: block.HashSum(),
				Content: []byte{},
			})
	}
	return protoHashedFile
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

	log.Println("sending")

	for _, serverFile := range listedServerFiles {
		protoHashedFile := serverFile.asProtoHashedFile()
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
		log.Printf("received protoCommand: %v\n", protoCommand)

		var command AdjustmentCommand
		switch protoCommand.Type {
		case pb.ProtoAdjustmentCommandType_REMOVE_FILE:
			command = AdjustmentCommandRemoveFile{
				protoCommand.Filename,
			}

		case pb.ProtoAdjustmentCommandType_MK_DIR:
			command = AdjustmentCommandMkDir{
				protoCommand.Filename,
			}

		case pb.ProtoAdjustmentCommandType_APPLY_BLOCKS_TO_FILE:
			blocks := make([]Block, 0, len(protoCommand.Blocks))
			for _, protoBlock := range protoCommand.Blocks {
				switch protoBlock.Type {
				case pb.ProtoBlockType_HASHED:
					blocks = append(
						blocks,
						NewHashedBlock(
							protoBlock.Offset,
							protoBlock.Size,
							protoBlock.Hashsum,
						),
					)

				case pb.ProtoBlockType_CONTENT:
					blocks = append(
						blocks,
						NewContentBlock(
							protoBlock.Offset,
							protoBlock.Size,
							protoBlock.Content,
						),
					)
				}
			}

			command = AdjustmentCommandApplyBlocksToFile{
				protoCommand.Filename,
				blocks,
			}
		}

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
		log.Printf("received hashed file: %v\n", protoHashedFile)

		hashedFile := HashedFile{
			Filename:     protoHashedFile.Filename,
			IsDir:        protoHashedFile.IsDir,
			FastHashes:   []Block{},
			StrongHashes: []Block{},
		}
		for _, fastHashedBlock := range protoHashedFile.FastHashes {
			hashedFile.FastHashes = append(
				hashedFile.FastHashes,
				NewHashedBlock(
					fastHashedBlock.Offset,
					fastHashedBlock.Size,
					fastHashedBlock.Hashsum,
				),
			)
		}
		for _, strongHashedBlock := range protoHashedFile.StrongHashes {
			hashedFile.StrongHashes = append(
				hashedFile.StrongHashes,
				NewHashedBlock(
					strongHashedBlock.Offset,
					strongHashedBlock.Size,
					strongHashedBlock.Hashsum,
				),
			)
		}
		c.serverHashedFiles = append(c.serverHashedFiles, hashedFile)
	}
	return nil
}

func (c *syncServiceClient) PushAdjustmentCommands() error {
	makeFastHash := func() hash.Hash32 { return NewMackerras(c.blockSize) }
	makeStrongHash := func() hash.Hash { return md5.New() }

	listedClientFiles, err := ListClientFiles(c.fs)

	factory := NewProducerFactory(c.blockSize, makeFastHash, makeStrongHash)
	comparator := NewFilesComparator(factory)
	commands := comparator.Compare(listedClientFiles, c.serverHashedFiles)

	pushStream, err := c.client.PushAdjustmentCommands(context.Background())
	if err != nil {
		log.Fatalf("push error: %v\n", err)
	}

	for _, abstractCommand := range commands {
		var protoCommand pb.ProtoAdjustmentCommand

		switch command := abstractCommand.(type) {
		case AdjustmentCommandApplyBlocksToFile:
			protoCommand = pb.ProtoAdjustmentCommand{
				Type:     pb.ProtoAdjustmentCommandType_APPLY_BLOCKS_TO_FILE,
				Filename: command.filename,
				Blocks:   []*pb.ProtoBlock{},
			}

			for _, abstractBlock := range command.blocks {
				switch block := abstractBlock.(type) {
				case ContentBlock:
					protoCommand.Blocks = append(
						protoCommand.Blocks,
						&pb.ProtoBlock{
							Type:    pb.ProtoBlockType_CONTENT,
							Offset:  block.Offset(),
							Size:    block.Size(),
							Hashsum: []byte{},
							Content: block.Content(),
						},
					)

				case HashedBlock:
					protoCommand.Blocks = append(
						protoCommand.Blocks,
						&pb.ProtoBlock{
							Type:    pb.ProtoBlockType_HASHED,
							Offset:  block.Offset(),
							Size:    block.Size(),
							Hashsum: block.HashSum(),
							Content: []byte{},
						},
					)
				}
			}

		case AdjustmentCommandRemoveFile:
			protoCommand = pb.ProtoAdjustmentCommand{
				Type:     pb.ProtoAdjustmentCommandType_REMOVE_FILE,
				Filename: command.filename,
				Blocks:   []*pb.ProtoBlock{},
			}

		case AdjustmentCommandMkDir:
			protoCommand = pb.ProtoAdjustmentCommand{
				Type:     pb.ProtoAdjustmentCommandType_MK_DIR,
				Filename: command.filename,
				Blocks:   []*pb.ProtoBlock{},
			}
		}

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
