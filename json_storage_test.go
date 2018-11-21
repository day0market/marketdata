package marketdata

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"os"
	"time"
	"fmt"
)

func timeOnTheFly(year int, mounth int, day int) time.Time {
	t := time.Date(year, time.Month(mounth), day, 0, 0, 0, 0, time.UTC)
	return t
}

func getSymbolMetaMock() *JsonSymbolMeta {
	storedDates := map[time.Time]struct{}{
		timeOnTheFly(2010, 1, 1):  struct{}{},
		timeOnTheFly(2010, 1, 2):  struct{}{},
		timeOnTheFly(2010, 1, 3):  struct{}{},
		timeOnTheFly(2010, 1, 4):  struct{}{},
		timeOnTheFly(2010, 1, 5):  struct{}{},
		timeOnTheFly(2010, 1, 15): struct{}{},
		timeOnTheFly(2010, 1, 16): struct{}{},
		timeOnTheFly(2010, 1, 18): struct{}{},
		timeOnTheFly(2010, 1, 19): struct{}{},
		timeOnTheFly(2010, 1, 22): struct{}{},
		timeOnTheFly(2010, 1, 30): struct{}{},
	}

	symbMeta := JsonSymbolMeta{
		"TEST",
		"D",
		storedDates,
	}

	return &symbMeta

}

func TestJsonSymbolMeta_getEmptyRanges(t *testing.T) {

	reqRange := dateRange{
		timeOnTheFly(2009, 12, 15),
		timeOnTheFly(2010, 1, 25),
	}

	expectedRanges := []*dateRange{
		&dateRange{timeOnTheFly(2009, 12, 15), timeOnTheFly(2009, 12, 31)},
		&dateRange{timeOnTheFly(2010, 1, 6), timeOnTheFly(2010, 1, 14)},
		&dateRange{timeOnTheFly(2010, 1, 17), timeOnTheFly(2010, 1, 17)},
		&dateRange{timeOnTheFly(2010, 1, 20), timeOnTheFly(2010, 1, 21)},
		&dateRange{timeOnTheFly(2010, 1, 23), timeOnTheFly(2010, 1, 25)},
	}

	symbMeta := getSymbolMetaMock()

	empty, err := symbMeta.getEmptyRanges(reqRange)
	if err != nil {
		fmt.Println(err)
	}
	for i, a := range empty {
		assert.Equal(t, *expectedRanges[i], *a)
	}

}

func TestJsonSymbolMeta_lastDate(t *testing.T) {
	symbMeta := getSymbolMetaMock()
	expected := timeOnTheFly(2010, 1, 30)

	actual, ok := symbMeta.lastDate()

	if !ok {
		t.Fatal("Result is not ok. Max date is zero")
	}

	assert.Equal(t, expected, actual)

}

func TestJsonSymbolMeta_firstDate(t *testing.T) {
	symbMeta := getSymbolMetaMock()
	expected := timeOnTheFly(2010, 1, 1)

	actual, ok := symbMeta.firstDate()

	if !ok {
		t.Fatal("Result is not ok. Date is more than today")
	}

	assert.Equal(t, expected, actual)

}

func TestJsonStorage_adjustDailyRange(t *testing.T) {
	storage := JsonStorage{
		"./test_path",
	}

	symbolMeta := getSymbolMetaMock()
	range1 := dateRange{
		timeOnTheFly(2010, 1, 30),
		timeOnTheFly(2010, 5, 30),
	}

	actual1, err := storage.findDailyRangeToDownload(range1, symbolMeta)
	if err != nil {
		t.Fatal(err)
	}

	expected1 := dateRange{
		timeOnTheFly(2010, 1, 1),
		timeOnTheFly(2010, 5, 30),
	}

	assert.Equal(t, expected1, actual1)

	range2 := dateRange{
		timeOnTheFly(2009, 1, 30),
		timeOnTheFly(2009, 5, 30),
	}

	actual2, err := storage.findDailyRangeToDownload(range2, symbolMeta)
	if err != nil {
		t.Fatal(err)
	}
	expected2 := dateRange{
		timeOnTheFly(2009, 1, 30),
		timeOnTheFly(2010, 1, 30),
	}

	assert.Equal(t, expected2, actual2)

	range3 := dateRange{
		timeOnTheFly(2010, 1, 15),
		timeOnTheFly(2010, 1, 20),
	}

	_, err = storage.findDailyRangeToDownload(range3, symbolMeta)
	if err == nil {
		t.Fatal("should be error: errNothingToDownload")
	} else {
		switch err.(type) {
		case *errNothingToDownload:
			fmt.Println("Got expected error. OK!")
			return
		default:
			t.Fatal("should be error: errNothingToDownload")
		}
	}

}

func TestJsonStorage_ensureFolder(t *testing.T) {
	defer os.RemoveAll("./test_storage")
	s := JsonStorage{
		"./test_storage",
	}

	err := s.ensureFolders()

	assert.Nil(t, err)

}

func TestJsonStorage_readCandlesFromFile(t *testing.T) {
	path := "./test_data/candles.json"
	candles, err := readCandlesFromFile(path)

	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, len(candles) > 1)

}
