package main

/// Cache for blocks. Maps hash ([]byte) to a block.
type BlockCache interface {
	Get(hash []byte) (block Block, ok bool)
	Set(hash []byte, block Block)

	AddHashes(blocks []Block)
	AddContents(hashedBlocks []Block, contentBlocks []Block)
}

type blockCache map[string]Block

func NewBlockCache() *blockCache {
	return &blockCache{}
}

func (bc *blockCache) Len() int { return len(*bc) }

func (bc *blockCache) Get(hash []byte) (Block, bool) {
	val, ok := (*bc)[string(hash)]
	return val, ok
}

func (bc *blockCache) Set(hash []byte, block Block) {
	(*bc)[string(hash)] = block
}

func (bc *blockCache) AddHashes(blocks []Block) {
	for _, block := range blocks {
		bc.Set(block.(HashedBlock).HashSum(), block)
	}
}

func (bc *blockCache) AddContents(hashedBlocks []Block, contentBlocks []Block) {
	for i, hashedBlock := range hashedBlocks {
		bc.Set(hashedBlock.(HashedBlock).HashSum(), contentBlocks[i])
	}
}
