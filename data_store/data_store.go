package data_store

import (
	"io/ioutil"
	"path/filepath"
	"strings"
)

type ScanResult struct {
	AbsolutePath string
	Extension    string
	IsDir        bool
	RelativePath string
}

type ScanConfiguration struct {
	extensions      map[string]struct{}
	EmitDirectories bool
}

func (c *ScanConfiguration) AddExtensions(extensions ...string) {
	if c.extensions == nil {
		c.extensions = make(map[string]struct{})
	}

	for _, extension := range extensions {
		c.extensions[strings.ToLower(extension)] = struct{}{}
	}
}

func handleDirectory(recurse func(base string) (int, error), absolutePath, relativePath string, handler func(result *ScanResult), emitDirectories bool) (int, error) {
	count, err := recurse(absolutePath)
	if err != nil {
		return 0, err
	}

	doesDirectoryHaveAnyContent := count > 0

	if !doesDirectoryHaveAnyContent {
		return 0, nil
	}

	if emitDirectories {
		handler(&ScanResult{
			AbsolutePath: absolutePath,
			IsDir:        true,
			RelativePath: relativePath,
		})
	}

	return count, nil
}

func handleFile(fileName string, extensions map[string]struct{}, absolutePath, relativePath string, handler func(result *ScanResult)) (int, error) {
	extension := strings.TrimLeft(strings.ToLower(filepath.Ext(fileName)), ".")

	if _, ok := extensions[extension]; !ok {
		return 0, nil
	}

	handler(&ScanResult{
		AbsolutePath: absolutePath,
		Extension:    extension,
		RelativePath: relativePath,
	})

	return 1, nil
}

func ScanDirectory(root string, config *ScanConfiguration, handler func(result *ScanResult), onComplete func(int, error)) {
	var recurse func(base string) (int, error)

	recurse = func(base string) (int, error) {
		files, err := ioutil.ReadDir(base)
		if err != nil {
			return 0, err
		}

		filesInDirectory := 0

		for _, file := range files {
			absolutePath := filepath.Join(base, file.Name())
			relativePath, _ := filepath.Rel(root, absolutePath)

			if file.IsDir() {
				count, err := handleDirectory(recurse, absolutePath, relativePath, handler, config.EmitDirectories)
				if err != nil {
					return 0, err
				}
				filesInDirectory += count
			} else {
				count, err := handleFile(file.Name(), config.extensions, absolutePath, relativePath, handler)
				if err != nil {
					return 0, err
				}
				filesInDirectory += count
			}
		}

		return filesInDirectory, nil
	}

	onComplete(recurse(root))
}
