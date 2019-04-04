package main

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func assertGenerateProduceReconstruct(
	t *testing.T,
	blockSize int,
	clientContent string,
	serverContent string,
) {
	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()

	generator := NewHashGenerator(blockSize, fastHasher, strongHasher)
	generatorResult := generator.Scan(strings.NewReader(serverContent))

	fastCache := NewBlockCache()
	fastCache.AddHashes(generatorResult.fastHashes)
	strongCache := NewBlockCache()
	strongCache.AddHashes(generatorResult.strongHashes)
	fastHasher.Reset()
	strongHasher.Reset()
	producer := NewBlockProducer(blockSize, fastHasher, strongHasher, fastCache, strongCache)
	r := strings.NewReader(clientContent)
	producerResult := producer.Scan(r)

	contentCache := NewBlockCache()
	contentCache.AddContents(generatorResult.strongHashes, generatorResult.contentBlocks)
	reconstructor := NewContentReconstructor(contentCache)
	serverOutputFile := bytes.NewBuffer(nil)
	reconstructor.Reconstruct(producerResult, serverOutputFile)

	assert.Equal(t, clientContent, serverOutputFile.String())
}

func TestIntegration_Smoke(t *testing.T) {
	blockSize := 4
	serverContent := ""
	clientContent := ""

	fastHasher := NewMackerras(blockSize)
	strongHasher := md5.New()

	generator := NewHashGenerator(blockSize, fastHasher, strongHasher)
	generatorResult := generator.Scan(strings.NewReader(serverContent))
	assert.Empty(t, generatorResult.contentBlocks)

	fastCache := NewBlockCache()
	fastCache.AddHashes(generatorResult.fastHashes)
	strongCache := NewBlockCache()
	strongCache.AddHashes(generatorResult.strongHashes)
	fastHasher.Reset()
	strongHasher.Reset()
	producer := NewBlockProducer(blockSize, fastHasher, strongHasher, fastCache, strongCache)
	r := strings.NewReader(clientContent)
	producerResult := producer.Scan(r)
	assert.Empty(t, producerResult)

	contentCache := NewBlockCache()
	contentCache.AddContents(generatorResult.strongHashes, generatorResult.contentBlocks)
	reconstructor := NewContentReconstructor(contentCache)
	serverOutputFile := bytes.NewBuffer(nil)
	n := reconstructor.Reconstruct(producerResult, serverOutputFile)
	assert.Equal(t, uint64(0), n)
	assert.Equal(t, clientContent, serverOutputFile.String())
}

func TestIntegration_ContentEquality(t *testing.T) {
	testCases := []struct {
		blockSize     int
		clientContent string
		serverContent string
	}{
		{4, "", ""},
		{4, "abcd1234", "abcd1234"},
		{4, "abcd123", "abcd1234"},
		{4, "abcd1234", "abcd123"},
		{4, "ab1234", "abcd123"},
		{4, "abcd1234", "ab123"},
		{4, "abcd34", "abcd123"},
		{4, "1234", "abcd"},

		{2, "1278", "12345678"},
		{2, "5678", "12345678"},
		{2, "12345678", "1278"},
		{2, "12345678", "12"},
		{2, "12345678", "78"},
	}
	for i, tt := range testCases {
		fmt.Printf("***** Running test case %v (%v, '%v' => '%v')\n",
			i, tt.blockSize, tt.clientContent, tt.serverContent)
		assertGenerateProduceReconstruct(
			t, tt.blockSize, tt.clientContent, tt.serverContent)
	}
}
