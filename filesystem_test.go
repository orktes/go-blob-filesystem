package blobfs

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestBlobFSFileSystemRead(t *testing.T) {
	bucket := generateTestBucket(t, map[string][]byte{
		"foo": []byte("foo"),
	})

	defer bucket.Close()

	fs := New(bucket)

	file, err := fs.Open("/foo")
	if err != nil {
		t.Error(err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		t.Error(err)
	}

	t.Run("get IsDir", func(t *testing.T) {
		if stat.IsDir() {
			t.Errorf("foo should not be a directory")
		}
	})

	t.Run("get ModTime", func(t *testing.T) {
		if stat.ModTime() == (time.Time{}) {
			t.Error("mod time should be returned for files")
		}
	})

	t.Run("get size", func(t *testing.T) {
		if stat.Size() != 3 {
			t.Errorf("wrong size returned, expected 3 got %d", stat.Size())
		}
	})

	t.Run("read content", func(t *testing.T) {
		data, err := ioutil.ReadAll(file)
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(data, []byte("foo")) {
			t.Error("Wrong content returned", string(data))
		}
	})

	t.Run("seek to absolute 1 & read content", func(t *testing.T) {
		offset, err := file.Seek(1, 0)
		if err != nil {
			t.Error(err)
		}

		if offset != 1 {
			t.Errorf("wrong offset after seek, expected 1 got %d", offset)
		}

		data, err := ioutil.ReadAll(file)
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(data, []byte("oo")) {
			t.Error("Wrong content returned", string(data))
		}
	})

	t.Run("seek to relative 1 (i.e 2) & read content", func(t *testing.T) {
		// First seek back to 1
		_, err := file.Seek(1, 0)
		if err != nil {
			t.Error(err)
		}

		offset, err := file.Seek(1, 1)
		if err != nil {
			t.Error(err)
		}

		if offset != 2 {
			t.Errorf("wrong offset after seek, expected 2 got %d", offset)
		}

		data, err := ioutil.ReadAll(file)
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(data, []byte("o")) {
			t.Error("Wrong content returned", string(data))
		}
	})

	t.Run("seek to 2 from end & read content", func(t *testing.T) {
		offset, err := file.Seek(2, 2)
		if err != nil {
			t.Error(err)
		}

		if offset != 1 {
			t.Errorf("wrong offset after seek, expected 1 got %d", offset)
		}

		data, err := ioutil.ReadAll(file)
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(data, []byte("oo")) {
			t.Error("Wrong content returned", string(data))
		}
	})

	t.Run("seek to offset larger than file & read content", func(t *testing.T) {
		offset, err := file.Seek(100, 0)
		if err != nil {
			t.Error(err)
		}

		if offset != 3 {
			t.Errorf("wrong offset after seek, expected 3 got %d", offset)
		}

		data, err := ioutil.ReadAll(file)
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(data, []byte("")) {
			t.Error("Wrong content returned", string(data))
		}
	})

	t.Run("try to stat file that doesnt exist", func(t *testing.T) {
		file, err := fs.Open("/some_random_path")
		if err != nil {
			t.Error(err)
		}
		defer file.Close()

		_, err = file.Stat()
		if err != os.ErrNotExist {
			t.Errorf("should have received ErrNotExist got %s", err)
		}
	})

	t.Run("try to read file that doesnt exist", func(t *testing.T) {
		file, err := fs.Open("/some_random_path")
		if err != nil {
			t.Error(err)
		}
		defer file.Close()

		_, err = ioutil.ReadAll(file)
		if err != os.ErrNotExist {
			t.Errorf("should have received ErrNotExist got %s", err)
		}
	})
}

func TestBlobFSFileSystemReaddir(t *testing.T) {
	filesMap := map[string][]byte{
		"foo":     []byte("foo"),
		"bar":     []byte("bar"),
		"biz/fuz": []byte("fuz"),
	}
	bucket := generateTestBucket(t, filesMap)
	defer bucket.Close()

	fs := New(bucket)

	t.Run("Readdir root", func(t *testing.T) {
		file, err := fs.Open("/")
		if err != nil {
			t.Error(err)
		}
		defer file.Close()

		stat, err := file.Stat()
		if err != nil {
			t.Error(err)
		}

		if stat.Size() != 0 {
			t.Error("dirs dont return size")
		}

		if !stat.IsDir() {
			t.Error("root should be a directory")
		}

		files, err := file.Readdir(0)
		if err != nil {
			t.Error(err)
		}

		if len(files) != 3 {
			t.Error("wrong amount of files returned")
		}

		if files[0].Name() != "bar" {
			t.Errorf("wrong file name returned, expected bar got %s", files[0].Name())
		}
		if files[1].Name() != "biz" {
			t.Errorf("wrong file name returned, expected biz got %s", files[1].Name())
		}
		if files[2].Name() != "foo" {
			t.Errorf("wrong file name returned, expected foo got %s", files[2].Name())
		}

		if files[0].IsDir() {
			t.Error("bar should not be a dir")
		}
		if !files[1].IsDir() {
			t.Error("biz should be a dir")
		}
		if files[2].IsDir() {
			t.Error("foo should not be a dir")
		}
	})

	t.Run("Readdir with limit 1", func(t *testing.T) {
		file, err := fs.Open("/")
		if err != nil {
			t.Error(err)
		}
		defer file.Close()

		files, err := file.Readdir(1)
		if err != nil {
			t.Error(err)
		}

		if len(files) != 1 {
			t.Error("wrong amount of files returned")
		}

		if files[0].Name() != "bar" {
			t.Errorf("wrong file name returned, expected bar got %s", files[0].Name())
		}

		if files[0].IsDir() {
			t.Error("bar should not be a dir")
		}
	})

	t.Run("Readdir with limit larger than file count", func(t *testing.T) {
		file, err := fs.Open("/")
		if err != nil {
			t.Error(err)
		}
		defer file.Close()

		_, err = file.Readdir(4)
		if err != io.EOF {
			t.Errorf("expected EOF got %s", err)
		}
	})

	t.Run("Readdir biz", func(t *testing.T) {
		file, err := fs.Open("/biz")
		if err != nil {
			t.Error(err)
		}
		defer file.Close()

		stat, err := file.Stat()
		if err != nil {
			t.Error(err)
		}

		if !stat.IsDir() {
			t.Error("biz should be a directory")
		}

		files, err := file.Readdir(0)
		if err != nil {
			t.Error(err)
		}

		if len(files) != 1 {
			t.Error("wrong amount of files returned")
		}

		if files[0].Name() != "fuz" {
			t.Errorf("wrong file name returned, expected fuz got %s", files[0].Name())
		}

		if files[0].IsDir() {
			t.Error("biz/fuz should not be a dir")
		}
	})
}
