package carrybasket

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

type File struct {
	Filename string
	IsDir    bool
	Content  string
}

func createFiles(fs VirtualFilesystem, files []File) {
	for _, file := range files {
		if file.IsDir {
			_ = fs.Mkdir(file.Filename)
		} else {
			w, _ := fs.OpenWrite(file.Filename)
			_, _ = w.Write([]byte(file.Content))
		}
	}
}

func assertFilesystemsEqual(t *testing.T, leftFs VirtualFilesystem, rightFs VirtualFilesystem) {
	leftFiles, err := leftFs.ListAll()
	assert.Nil(t, err)

	rightFiles, err := rightFs.ListAll()
	assert.Nil(t, err)

	assert.Equal(t, len(leftFiles), len(rightFiles))
	assert.Equal(t, leftFiles, rightFiles)

	for i := 0; i < len(leftFiles); i++ {
		assert.Equal(t, leftFs.IsDir(leftFiles[i]), rightFs.IsDir(rightFiles[i]))
		if !leftFs.IsDir(leftFiles[i]) {
			leftR, err := leftFs.OpenRead(leftFiles[i])
			assert.Nil(t, err)
			assert.NotNil(t, leftR)

			rightR, err := rightFs.OpenRead(rightFiles[i])
			assert.Nil(t, err)
			assert.NotNil(t, rightR)

			leftContents, err := ioutil.ReadAll(leftR)
			rightContents, err := ioutil.ReadAll(rightR)
			assert.Equal(t, leftContents, rightContents)
		}
	}
}
