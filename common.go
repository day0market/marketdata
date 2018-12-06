package marketdata

import (
	"os"
	"time"
)

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

func timeOnTheFly(year int, mounth int, day int) time.Time {
	t := time.Date(year, time.Month(mounth), day, 0, 0, 0, 0, time.UTC)
	return t
}

func setTimeToSOD(t time.Time) time.Time {
	loc := t.Location()
	t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
	return t
}

func setTimeToEOD(t time.Time) time.Time {
	loc := t.Location()
	t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
	return t
}
