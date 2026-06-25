package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type GCPStorage struct {
	client *storage.Client
	bucket string
}

// ObjectInfo carries the metadata needed to decide whether a local copy of an
// object is still fresh.
type ObjectInfo struct {
	Name string
	Size int64
}

var _ Storage = (*GCPStorage)(nil)

func NewGCPStorage(ctx context.Context, bucket string) (*GCPStorage, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP storage client: %w", err)
	}
	return &GCPStorage{
		client: client,
		bucket: bucket,
	}, nil
}

func (s *GCPStorage) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *GCPStorage) DownloadFile(ctx context.Context, objectName, destinationPath string) error {
	rc, err := s.client.Bucket(s.bucket).Object(objectName).NewReader(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	if err := os.MkdirAll(filepath.Dir(destinationPath), 0755); err != nil {
		return err
	}

	f, err := os.Create(destinationPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return err
	}
	return f.Close()
}

func (s *GCPStorage) UploadFile(ctx context.Context, sourcePath, objectName string) error {
	file, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer file.Close()

	wc := s.client.Bucket(s.bucket).Object(objectName).NewWriter(ctx)
	defer wc.Close()

	if _, err := io.Copy(wc, file); err != nil {
		return err
	}
	return nil
}

func (s *GCPStorage) ListFiles(ctx context.Context, prefix string) ([]string, error) {
	var files []string
	it := s.client.Bucket(s.bucket).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		files = append(files, attrs.Name)
	}
	return files, nil
}

// ListObjects returns every object under prefix together with its size, used by
// callers that want to skip re-downloading objects already cached locally.
func (s *GCPStorage) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	it := s.client.Bucket(s.bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		objects = append(objects, ObjectInfo{Name: attrs.Name, Size: attrs.Size})
	}
	return objects, nil
}

// AttrsSize returns the byte size of a single object.
func (s *GCPStorage) AttrsSize(ctx context.Context, objectName string) (int64, error) {
	attrs, err := s.client.Bucket(s.bucket).Object(objectName).Attrs(ctx)
	if err != nil {
		return 0, err
	}
	return attrs.Size, nil
}

func (s *GCPStorage) DeletePrefix(ctx context.Context, prefix string) error {
	it := s.client.Bucket(s.bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.client.Bucket(s.bucket).Object(attrs.Name).Delete(ctx); err != nil {
			return err
		}
	}
}

// InitGCS creates a GCPStorage client if a bucket is configured, or returns nil.
func InitGCS(bucket string) (*GCPStorage, error) {
	if bucket == "" {
		return nil, nil
	}
	return NewGCPStorage(context.Background(), bucket)
}
