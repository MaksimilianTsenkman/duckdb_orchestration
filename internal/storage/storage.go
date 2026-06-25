package storage

import "context"

type Storage interface {
	Close() error
	DownloadFile(ctx context.Context, objectName, destinationPath string) error
	UploadFile(ctx context.Context, sourcePath, objectName string) error
	ListFiles(ctx context.Context, prefix string) ([]string, error)
	DeletePrefix(ctx context.Context, prefix string) error
}

type NopCloser struct{}

func (NopCloser) Close() error {
	return nil
}
