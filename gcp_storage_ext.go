package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type GCPStorage struct {
	client *storage.Client
	bucket string
}

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

func (s *GCPStorage) DownloadFile(ctx context.Context, objectName, destinationPath string) error {
	rc, err := s.client.Bucket(s.bucket).Object(objectName).NewReader(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	return os.WriteFile(destinationPath, data, 0644)
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

func initGCS(bucket string) *GCPStorage {
	if bucket == "" {
		return nil
	}
	gcs, err := NewGCPStorage(context.Background(), bucket)
	if err != nil {
		log.Fatalf("init GCS: %v", err)
	}
	return gcs
}
