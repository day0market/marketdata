package marketdata

import "os"

func fileExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

func createDirIfNotExists(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {

		err := os.MkdirAll(dirPath, os.ModePerm)
		return err
	}

	return nil
}
