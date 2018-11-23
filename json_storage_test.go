package marketdata

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"os"
	"time"
	"fmt"
	"path"
)

func timeOnTheFly(year int, mounth int, day int) time.Time {
	t := time.Date(year, time.Month(mounth), day, 0, 0, 0, 0, time.UTC)
	return t
}

func getSymbolMetaMock() *JsonSymbolMeta {
	storedDates := []time.Time{
		timeOnTheFly(2010, 1, 1),
		timeOnTheFly(2010, 1, 2),
		timeOnTheFly(2010, 1, 3),
		timeOnTheFly(2010, 1, 4),
		timeOnTheFly(2010, 1, 5),
		timeOnTheFly(2010, 1, 1),
		timeOnTheFly(2010, 1, 1),
		timeOnTheFly(2010, 1, 1),
		timeOnTheFly(2010, 1, 1),
		timeOnTheFly(2010, 1, 2),
		timeOnTheFly(2010, 1, 30),
	}

	symbMeta := JsonSymbolMeta{
		"TEST",
		"D",
		storedDates,
	}

	return &symbMeta

}

func TestJsonSymbolMeta_getEmptyRanges(t *testing.T) {

	reqRange := DateRange{
		timeOnTheFly(2009, 12, 15),
		timeOnTheFly(2010, 1, 25),
	}

	expectedRanges := []*DateRange{
		{timeOnTheFly(2009, 12, 15), timeOnTheFly(2009, 12, 31)},
		{timeOnTheFly(2010, 1, 6), timeOnTheFly(2010, 1, 14)},
		{timeOnTheFly(2010, 1, 17), timeOnTheFly(2010, 1, 17)},
		{timeOnTheFly(2010, 1, 20), timeOnTheFly(2010, 1, 21)},
		{timeOnTheFly(2010, 1, 23), timeOnTheFly(2010, 1, 25)},
	}

	symbMeta := getSymbolMetaMock()

	empty, err := symbMeta.getEmptyRanges(&reqRange)
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

func TestJsonSymbolMeta_Save(t *testing.T) {
	pth := "./test_data/symbol_meta/test_save.json"
	defer os.Remove(pth)
	meta := getSymbolMetaMock()
	fmt.Println(meta)
	err := meta.Save(pth)
	if err != nil {
		t.Fatal(err)
	}

	loadedMeta := JsonSymbolMeta{}
	loadedMeta.Load(pth)

	assert.True(t, len(loadedMeta.ListedDates) > 2)

	expectingDate := timeOnTheFly(2010, 01, 01)

	assert.Equal(t, loadedMeta.ListedDates[0], expectingDate)

}

func TestJsonStorage_findDailyRangeToDownload(t *testing.T) {
	//Todo
	/*storage := JsonStorage{
		"./test_path",
		NewActiveTick(5000, "localhost", 2),
	}

	symbolMeta := getSymbolMetaMock()
	range1 := DateRange{
		timeOnTheFly(2010, 1, 30),
		timeOnTheFly(2010, 5, 30),
	}

	actual1, err := storage.findDailyRangeToDownload(range1, symbolMeta)
	if err != nil {
		t.Fatal(err)
	}

	expected1 := DateRange{
		timeOnTheFly(2010, 1, 1),
		timeOnTheFly(2010, 5, 30),
	}

	assert.Equal(t, expected1, actual1)

	range2 := DateRange{
		timeOnTheFly(2009, 1, 30),
		timeOnTheFly(2009, 5, 30),
	}

	actual2, err := storage.findDailyRangeToDownload(range2, symbolMeta)
	if err != nil {
		t.Fatal(err)
	}
	expected2 := DateRange{
		timeOnTheFly(2009, 1, 30),
		timeOnTheFly(2010, 1, 30),
	}

	assert.Equal(t, expected2, actual2)

	range3 := DateRange{
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
	}*/

}

func TestJsonStorage_ensureFolder(t *testing.T) {
	defer os.RemoveAll("./test_storage")
	s := JsonStorage{
		"./test_storage",
		NewActiveTick(5000, "localhost", 2),
	}

	err := s.createFolders()

	assert.Nil(t, err)

}

func TestJsonStorage_readCandlesFromFile(t *testing.T) {
	pth := "./test_data/candles.json"
	storage := JsonStorage{}
	candles, err := storage.readCandlesFromFile(pth)

	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, len(*candles) > 1)

}

func TestJsonStorage_saveCandlesToFile(t *testing.T) {
	defer os.Remove("./test_data/save_test.json")

	at := NewActiveTick(0, "207.154.204.20", 3)
	dRange := DateRange{timeOnTheFly(2010, 1, 1),
		timeOnTheFly(2012, 5, 3)}
	candles, err := at.GetCandles("SPY", "D", dRange)
	if err != nil {
		t.Fatal(err)
	}

	storage := JsonStorage{
		"./test_data",
		at,
	}

	err = storage.saveCandlesToFile(candles, "./test_data/save_test.json")
	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, fileExists("./test_data/save_test.json"))
}

func TestJsonStorage_saveAndLoadCandles(t *testing.T) {
	defer os.Remove("./test_data/TEST_read_write.json")

	at := NewActiveTick(0, "207.154.204.20", 3)
	dRange := DateRange{timeOnTheFly(2010, 1, 1),
		timeOnTheFly(2012, 5, 3)}
	candles, err := at.GetCandles("SPY", "D", dRange)
	if err != nil {
		t.Fatal(err)
	}

	storage := JsonStorage{
		"./test_data",
		at,
	}

	err = storage.saveCandlesToFile(candles, "./test_data/TEST_read_write.json")
	if err != nil {
		t.Fatal(err)
	}

	loadedCandles, err := storage.readCandlesFromFile("./test_data/TEST_read_write.json")

	if err != nil {
		t.Fatal(err)
	}

	for i, v := range *candles {
		assert.Equal(t, *v, *(*loadedCandles)[i])
	}

}

func TestJsonStorage_updateDailyCandles(t *testing.T) {
	testDir := "./test_data/json_storage"
	os.RemoveAll(testDir)

	at := NewActiveTick(0, "207.154.204.20", 3)
	createDirIfNotExists(testDir)

	storage := JsonStorage{
		testDir,
		at,
	}

	//storage.createFolders()

	range1 := DateRange{timeOnTheFly(2010, 1, 1), timeOnTheFly(2011, 1, 1)}

	err := storage.updateDailyCandles("SPY", &range1)
	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, fileExists(path.Join(testDir, "candles/day", "SPY.json")))
	assert.True(t, fileExists(path.Join(testDir, "candles/day/.meta", "SPY.json")))

	range2 := DateRange{timeOnTheFly(2010, 5, 1), timeOnTheFly(2015, 1, 1)}

	err2 := storage.updateDailyCandles("SPY", &range2)
	if err2 != nil {
		t.Fatal(err2)
	}

	candles, err := storage.readCandlesFromFile(path.Join(testDir, "candles/day", "SPY.json"))
	if err != nil {
		t.Fatal(err)
	}

	realRange := DateRange{timeOnTheFly(2010, 1, 1), timeOnTheFly(2015, 1, 1)}

	datasourceCandles, err := at.GetCandles("SPY", "D", realRange)

	if err != nil {
		t.Fatal(err)
	}

	for i, v := range *datasourceCandles {
		assert.Equal(t, *(*candles)[i], *v)
	}
}
