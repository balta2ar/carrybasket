package main

type AdjustmentCommand interface{}

type AdjustmentCommandRemoveFile struct {
	filename string
}

type AdjustmentCommandApplyBlocksToFile struct {
	filename string
	blocks   []Block
}

type AdjustmentCommandMkDir struct {
	filename string
}

/// Files can be compared and based on the comparison results various
/// actions are possible, e.g. remove file from server or apply
/// client blocks to server files.
type FilesComparator interface {
	Compare(
		clientFiles []VirtualFile,
		serverHashedFiles []HashedFile,
	) []AdjustmentCommand
}

type filesComparator struct {
	producerFactory ProducerFactory
}

func NewFilesComparator(producerFactory ProducerFactory) *filesComparator {
	return &filesComparator{
		producerFactory: producerFactory,
	}
}

func anyContentBlocks(blocks []Block) bool {
	for _, block := range blocks {
		if _, ok := block.(ContentBlock); ok {
			return true
		}
	}

	return false
}

func (fc *filesComparator) Compare(
	clientFiles []VirtualFile,
	serverHashedFiles []HashedFile,
) []AdjustmentCommand {
	var commands []AdjustmentCommand
	var i, j int

	addClientFile := func(i int, fastHashBlocks []Block, strongHashBlocks []Block) {
		// both are files
		producer := fc.producerFactory.MakeProducer(fastHashBlocks, strongHashBlocks)
		blocks := producer.Scan(NewStackedReadSeeker(clientFiles[i].Rw))
		// if all blocks are hashed, the content is the same, no need to transfer anything
		if anyContentBlocks(blocks) {
			commands = append(commands,
				AdjustmentCommandApplyBlocksToFile{clientFiles[i].Filename, blocks},
			)
		}
	}

	addClientFileOrDir := func(i int, fastHashBlocks []Block, strongHashBlocks []Block) {
		// called only when the server counterpart is missing
		if clientFiles[i].IsDir {
			commands = append(commands,
				AdjustmentCommandMkDir{clientFiles[i].Filename},
			)
			return

		}

		// both are files
		addClientFile(i, fastHashBlocks, strongHashBlocks)
	}

	compareAndAddClientFileOrDir := func(i int, j int, fastHashBlocks []Block, strongHashBlocks []Block) {
		// called when filenames are the same but isDir flag may be different
		clientDir, serverDir := clientFiles[i].IsDir, serverHashedFiles[j].IsDir
		// both are dirs, nothing's changed
		if clientDir && serverDir {
			return
		}

		// client file, server dir
		if !clientDir && serverDir {
			// remove server dir, replace it with new client file
			commands = append(commands,
				AdjustmentCommandRemoveFile{serverHashedFiles[j].Filename},
			)
			addClientFile(i, nil, nil)
			return
		}

		// client dir, server file
		if clientDir && !serverDir {
			// remove server file, create client dir
			commands = append(commands,
				AdjustmentCommandRemoveFile{serverHashedFiles[j].Filename},
			)
			commands = append(commands,
				AdjustmentCommandMkDir{clientFiles[i].Filename},
			)
			return
		}

		// both are files
		addClientFile(i, fastHashBlocks, strongHashBlocks)
	}

	for i < len(clientFiles) && j < len(serverHashedFiles) {
		if clientFiles[i].Filename < serverHashedFiles[j].Filename {
			// new client file, add it
			addClientFileOrDir(i, nil, nil)
			i += 1
		} else if clientFiles[i].Filename > serverHashedFiles[j].Filename {
			// new server file, remove it
			commands = append(commands,
				AdjustmentCommandRemoveFile{serverHashedFiles[j].Filename},
			)
			j += 1
		} else {
			// file name is the same, compare contents
			compareAndAddClientFileOrDir(i, j, serverHashedFiles[j].FastHashes, serverHashedFiles[j].StrongHashes)
			i += 1
			j += 1
		}
	}

	// add new files if any
	for i < len(clientFiles) {
		addClientFileOrDir(i, nil, nil)
		i += 1
	}

	// remove server files if any
	for j < len(serverHashedFiles) {
		commands = append(commands,
			AdjustmentCommandRemoveFile{serverHashedFiles[j].Filename},
		)
		j += 1
	}

	return commands
}

type AdjustmentCommandApplier interface {
	Apply(
		commands []AdjustmentCommand,
		fs VirtualFilesystem,
		cr ContentReconstructor,
	) error
}

type adjustmentCommandApplier struct{}

func NewAdjustmentCommandApplier() *adjustmentCommandApplier {
	return &adjustmentCommandApplier{}
}

func (aca *adjustmentCommandApplier) Apply(
	commands []AdjustmentCommand,
	fs VirtualFilesystem,
	cr ContentReconstructor,
) error {
	for _, abstractCommand := range commands {
		switch command := abstractCommand.(type) {
		case AdjustmentCommandRemoveFile:
			if err := fs.Delete(command.filename); err != nil {
				return err
			}

		case AdjustmentCommandMkDir:
			if err := fs.Mkdir(command.filename); err != nil {
				return err
			}

		case AdjustmentCommandApplyBlocksToFile:
			tempFilename := command.filename + ".tmp"
			w, err := fs.Open(tempFilename)
			if err != nil {
				return err
			}

			cr.Reconstruct(command.blocks, w)
			if err := fs.Move(tempFilename, command.filename); err != nil {
				return err
			}
		}
	}

	return nil
}
