package main

type Block interface {
	Offset() uint64
	Size() uint64
}

type ContentBlock interface {
	Block
	Content() []byte
}

type HashedBlock interface {
	Block
	HashSum() []byte
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
		block:   block{offset, size,},
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
		block:   block{offset, size,},
		hashSum: append(hashSum[:0:0], hashSum...),
	}
}
