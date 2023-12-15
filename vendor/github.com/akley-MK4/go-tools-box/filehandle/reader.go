package filehandle

import (
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

func CalculateDirectoryAllFileSize(dirPath string) (int64, error) {
	infos, readErr := ioutil.ReadDir(dirPath)
	if os.IsNotExist(readErr) {
		return 0, nil
	}
	if readErr != nil {
		return 0, readErr
	}
	if len(infos) <= 0 {
		return 0, nil
	}

	var filesSize int64
	for _, info := range infos {
		if info.IsDir() {
			continue
		}
		filesSize += info.Size()
	}

	return filesSize, nil
}

func CalculateAllFileSizeThroughRecursion(dirPath string, replyFilesSize *int64) error {
	infos, readErr := ioutil.ReadDir(dirPath)
	if os.IsNotExist(readErr) {
		return nil
	}
	if readErr != nil {
		return readErr
	}
	if len(infos) <= 0 {
		return nil
	}

	var dirInfo []fs.FileInfo
	for _, info := range infos {
		if info.IsDir() {
			dirInfo = append(dirInfo, info)
			continue
		}
		atomic.AddInt64(replyFilesSize, info.Size())
	}
	if len(dirInfo) <= 0 {
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(len(dirInfo))
	for _, info := range dirInfo {
		go func(info fs.FileInfo) {
			defer wg.Done()
			CalculateAllFileSizeThroughRecursion(path.Join(dirPath, info.Name()), replyFilesSize)
		}(info)
	}
	wg.Wait()

	return nil
}
