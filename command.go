package main

type AdjustmentCommand interface{}

type AdjustmentCommandRemoveFile struct {
	filename string
}

type AdjustmentCommandApplyBlocksToFile struct {
	filename string
	blocks   []Block
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
		producer := fc.producerFactory.MakeProducer(fastHashBlocks, strongHashBlocks)
		blocks := producer.Scan(NewStackedReadSeeker(clientFiles[i].Rw))
		// if all blocks are hashed, the content is the same, no need to transfer anything
		if anyContentBlocks(blocks) {
			commands = append(commands,
				AdjustmentCommandApplyBlocksToFile{clientFiles[i].Filename, blocks},
			)
		}
	}

	for i < len(clientFiles) && j < len(serverHashedFiles) {
		if clientFiles[i].Filename < serverHashedFiles[j].Filename {
			// new client file, add it
			addClientFile(i, nil, nil)
			i += 1
		} else if clientFiles[i].Filename > serverHashedFiles[j].Filename {
			// new server file, remove it
			commands = append(commands,
				AdjustmentCommandRemoveFile{serverHashedFiles[j].Filename},
			)
			j += 1
		} else {
			// file name is the same, compare contents
			addClientFile(i, serverHashedFiles[j].FastHashes, serverHashedFiles[j].StrongHashes)
			i += 1
			j += 1
		}
	}

	// add new files if any
	for i < len(clientFiles) {
		addClientFile(i, nil, nil)
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
		fs []VirtualFilesystem,
		cr ContentReconstructor,
	) error
}

type adjustmentCommandApplier struct{}

func NewAdjustmentCommandApplier() *adjustmentCommandApplier {
	return &adjustmentCommandApplier{}
}

func (aca *adjustmentCommandApplier) Apply(
	commands []AdjustmentCommand,
	fs []VirtualFilesystem,
	cr ContentReconstructor,
) error {
	return nil
}
