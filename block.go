package carrybasket

/// Block of data. Either a content or hash of a content.
type Block interface {
	/// Offset of the block in the file
	/// (may be different for client and server)
	Offset() uint64
	/// Size of the block. May be smaller than default block size
	/// 1) for fast hash block when it precedes a strong hash block,
	/// 2) for both types of hashes at the end of a file.
	Size() uint64
}

type ContentBlock interface {
	Block
	Content() []byte /// Actual content of a file.
}

type HashedBlock interface {
	Block
	HashSum() []byte /// Hash of the specified file block.
}

type block struct {
	offset uint64
	size   uint64
}

func (b *block) Offset() uint64 { return b.offset }
func (b *block) Size() uint64   { return b.size }

type contentBlock struct {
	block
	content []byte
}

func (cb *contentBlock) Content() []byte { return cb.content }
func NewContentBlock(offset uint64, size uint64, content []byte) *contentBlock {
	return &contentBlock{
		block:   block{offset, size},
		content: append(content[:0:0], content...),
	}
}

type hashedBlock struct {
	block
	hashSum []byte
}

func (hb *hashedBlock) HashSum() []byte { return hb.hashSum }
func NewHashedBlock(offset uint64, size uint64, hashSum []byte) *hashedBlock {
	return &hashedBlock{
		block:   block{offset, size},
		hashSum: append(hashSum[:0:0], hashSum...),
	}
}
