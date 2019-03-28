package main

import (
	"crypto/md5"
	"github.com/stretchr/testify/assert"
	"hash/adler32"
	"strings"
	"testing"
)

func TestBlockProducer_Smoke(t *testing.T) {
	blockSize := 1
	fastHash := adler32.New()
	strongHash := md5.New()
	fastCache := NewBlockCache()
	strongCache := NewBlockCache()
	producer := NewBlockProducer(blockSize, fastHash, strongHash, fastCache, strongCache)

	r1 := NewStackedReadSeeker(strings.NewReader("abc"))
	result := producer.Scan(r1)
	assert.NotEmpty(t, result)

	r2 := NewStackedReadSeeker(strings.NewReader(""))
	result = producer.Scan(r2)
	assert.Empty(t, result)
}
