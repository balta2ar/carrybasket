package carrybasket

import (
	pb "github.com/balta2ar/carrybasket/rpc"
)

func adjustmentCommandAsProtoAdjustmentCommand(abstractCommand AdjustmentCommand) pb.ProtoAdjustmentCommand {
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

	return protoCommand
}

func protoHashedFileAsHashedFile(protoHashedFile *pb.ProtoHashedFile) HashedFile {
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
	return hashedFile
}

func protoAdjustmentCommandAsAdjustmentCommand(protoCommand *pb.ProtoAdjustmentCommand) AdjustmentCommand {
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
			blocks = append(blocks, protoBlockAsBlock(protoBlock))
		}

		command = AdjustmentCommandApplyBlocksToFile{
			protoCommand.Filename,
			blocks,
		}
	}
	return command
}

func protoBlockAsBlock(protoBlock *pb.ProtoBlock) Block {
	switch protoBlock.Type {
	case pb.ProtoBlockType_HASHED:
		return NewHashedBlock(
			protoBlock.Offset,
			protoBlock.Size,
			protoBlock.Hashsum,
		)

	case pb.ProtoBlockType_CONTENT:
		return NewContentBlock(
			protoBlock.Offset,
			protoBlock.Size,
			protoBlock.Content,
		)

	}
	return nil
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
