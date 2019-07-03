package blobfs

import (
	"context"
	"io"
	"os"
	"strings"

	"gocloud.dev/blob"
)

type blobFile struct {
	name   string
	bucket *blob.Bucket
	ctx    context.Context
	config Config

	fileInfo   *blobFileInfo
	iter       *blob.ListIterator
	dataReader io.ReadCloser
	offset     int64
}

func (bf *blobFile) Close() error {
	if bf.dataReader != nil {
		return bf.dataReader.Close()
	}
	return nil
}

func (bf *blobFile) Read(p []byte) (n int, err error) {
	if bf.dataReader == nil {
		if err := bf.initReader(); err != nil {
			return 0, err
		}
	}
	n, err = bf.dataReader.Read(p)
	if err != nil {
		return
	}

	bf.offset += int64(n)
	return
}

func (bf *blobFile) Seek(offset int64, whence int) (int64, error) {
	if bf.dataReader != nil {
		if err := bf.dataReader.Close(); err != nil {
			return 0, err
		}
	}

	switch whence {
	case 0: // Relative to start
		return bf.initRangeReader(offset)
	case 1: // Relative to current pos
		return bf.initRangeReader(bf.offset + offset)
	case 2: // Relative to end
		stat, err := bf.Stat()
		if err != nil {
			return 0, err
		}

		length := stat.Size()
		return bf.initRangeReader(length - offset)

	}

	return bf.offset, nil
}

func (bf *blobFile) Readdir(count int) (files []os.FileInfo, err error) {
	ctx := bf.ctx
	if bf.config.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, bf.config.RequestTimeout)
		defer cancel()
	}

	name := bf.name
	if name != "" && !strings.HasSuffix(name, "/") {
		name = bf.name + "/"
	}

	if bf.iter == nil {
		bf.iter = bf.bucket.List(&blob.ListOptions{Prefix: name, Delimiter: "/"})
	}

	for {
		obj, err := bf.iter.Next(ctx)
		if err != nil {
			if err == io.EOF && count <= 0 {
				return files, nil
			}
			return files, err
		}

		files = append(files, &blobFileInfo{
			name:   obj.Key, // TODO this might not be the right name
			bucket: bf.bucket,

			isDir: obj.IsDir,
			attrs: &blob.Attributes{
				MD5:     obj.MD5,
				ModTime: obj.ModTime,
				Size:    obj.Size,
			},
		})

		if count > 0 && len(files) == count {
			break
		}
	}

	return files, err
}

func (bf *blobFile) Stat() (os.FileInfo, error) {
	if bf.fileInfo != nil {
		return bf.fileInfo, nil
	}

	ctx := bf.ctx
	if bf.config.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, bf.config.RequestTimeout)
		defer cancel()
	}

	fileInfo, err := getBlobFileInfo(ctx, bf.name, bf.bucket)

	if err == nil {
		bf.fileInfo = fileInfo
	}

	return fileInfo, err
}

func (bf *blobFile) initRangeReader(offset int64) (int64, error) {
	stat, err := bf.Stat()
	if err != nil {
		return 0, err
	}

	if offset > stat.Size() {
		offset = stat.Size()
	}

	reader, err := bf.bucket.NewRangeReader(bf.ctx, bf.name, offset, -1, nil)
	if err != nil {
		return bf.offset, err
	}
	bf.dataReader = reader
	bf.offset = offset

	return offset, nil
}

func (bf *blobFile) initReader() error {
	_, err := bf.initRangeReader(0)
	return err
}
