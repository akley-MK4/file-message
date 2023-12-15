package filehandle

import "os"

func CreateDirectory(p string) (bool, error) {
	_, statErr := os.Stat(p)
	if os.IsNotExist(statErr) {
		if mkErr := os.MkdirAll(p, os.ModePerm); mkErr != nil {
			return false, mkErr
		}
		return true, nil
	}
	if statErr != nil {
		return false, statErr
	}

	return false, nil
}

func RemoveDirectory(dirPath string) (bool, error) {
	_, statErr := os.Stat(dirPath)
	if os.IsNotExist(statErr) {
		return false, nil
	}

	return true, os.RemoveAll(dirPath)
}
