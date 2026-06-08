package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type LocalStorage struct {
	baseDir string
}

var _ Storage = (*LocalStorage)(nil)

func NewLocalStorage(baseDir string) *LocalStorage {
	return &LocalStorage{baseDir: baseDir}
}

func (s *LocalStorage) Close() error {
	return nil
}

func (s *LocalStorage) DownloadFile(_ context.Context, objectName, destinationPath string) error {
	return copyFile(s.resolve(objectName), destinationPath)
}

func (s *LocalStorage) UploadFile(_ context.Context, sourcePath, objectName string) error {
	return copyFile(sourcePath, s.resolve(objectName))
}

func (s *LocalStorage) ListFiles(_ context.Context, prefix string) ([]string, error) {
	root := s.resolve(prefix)
	info, err := os.Stat(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var files []string
	if !info.IsDir() {
		rel, err := filepath.Rel(s.baseDir, root)
		if err != nil {
			return nil, err
		}
		return []string{filepath.ToSlash(rel)}, nil
	}

	err = filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.baseDir, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	return files, err
}

func (s *LocalStorage) DeletePrefix(_ context.Context, prefix string) error {
	target := s.resolve(prefix)
	return os.RemoveAll(target)
}

func (s *LocalStorage) resolve(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	clean := filepath.FromSlash(strings.TrimSpace(path))
	return filepath.Join(s.baseDir, clean)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}
