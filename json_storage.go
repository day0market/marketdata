package marketdata

import (
	"time"
	"os"
	"path"
	"strconv"
	"github.com/pkg/errors"
	"encoding/json"
	"io/ioutil"
	"fmt"
	"path/filepath"
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

type errNothingToDownload struct {
}

func (*errNothingToDownload) Error() string {
	return "Nothing to download"
}

type JsonSymbolMeta struct {
	Symbol      string
	TimeFrame   string
	ListedDates []time.Time //Time should be always 0
}

func (j *JsonSymbolMeta) Load(loadPath string) error {
	jsonFile, err := os.Open(loadPath)

	if err != nil {
		return err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	err = json.Unmarshal(byteValue, j)

	if err != nil {
		return err
	}

	return nil

}

func (j *JsonSymbolMeta) datesSet() map[time.Time]struct{} {
	dates := make(map[time.Time]struct{})
	for _, v := range j.ListedDates {
		dates[v] = struct{}{}
	}
	return dates
}

func (j *JsonSymbolMeta) getEmptyRanges(rng DateRange) ([]*DateRange, error) {
	last := rng.from

	var emptyDates []time.Time
	datesSet := j.datesSet()

	for {
		if last.After(rng.to) {
			break
		}
		_, ok := datesSet[last]

		if !ok {
			emptyDates = append(emptyDates, last)
		}

		last = last.AddDate(0, 0, 1)

	}

	fmt.Println(emptyDates)

	if len(emptyDates) == 0 {
		return nil, nil //ToDo should I return error here
	}

	if len(emptyDates) == 1 {
		rng := DateRange{
			emptyDates[0],
			emptyDates[0],
		}

		return []*DateRange{&rng}, nil
	}

	start, end := emptyDates[0], emptyDates[1]
	var emptyRanges []*DateRange

	for i, v := range emptyDates {
		if i < 2 {
			continue
		}

		delta := int(v.Sub(end).Hours() / 24)
		if delta > 1 {
			rng := DateRange{start, end}
			emptyRanges = append(emptyRanges, &rng)
			start, end = v, v
			continue
		}
		end = v

	}

	rngF := DateRange{start, end}
	emptyRanges = append(emptyRanges, &rngF)

	return emptyRanges, nil
}

func (j *JsonSymbolMeta) firstDate() (time.Time, bool) {
	minTime := time.Now().AddDate(0, 0, 1)
	for _, k := range j.ListedDates {
		if k.Unix() < minTime.Unix() {
			minTime = k
		}

	}
	if minTime.After(time.Now()) {
		return minTime, false
	}
	return minTime, true
}

func (j *JsonSymbolMeta) lastDate() (time.Time, bool) {
	maxTime := time.Time{}

	for _, k := range j.ListedDates {
		if k.Unix() > maxTime.Unix() {
			maxTime = k
		}
	}
	if maxTime.IsZero() {
		return maxTime, false
	}
	return maxTime, true

}

func (j *JsonSymbolMeta) Save(savePath string) error {
	dirName := filepath.Dir(savePath)
	err := createDirIfNotExists(dirName)
	if err != nil {
		return err
	}

	json_, err := json.Marshal(j)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(savePath, json_, 0644)

	return err
}

type JsonStorage struct {
	path     string
	provider HistoryProvider
}

func (p *JsonStorage) ensureFolders() error {
	ticksFolder := path.Join(p.path, "ticks")
	candlesFolder := path.Join(p.path, "candles")

	err := createDirIfNotExists(candlesFolder)
	if err != nil {
		return err
	}

	err = createDirIfNotExists(ticksFolder)
	if err != nil {
		return err
	}

	return nil

}

func (p *JsonStorage) UpdateSymbol(symbol string, tf string, dRange DateRange) error {
	switch tf {
	case "D":
		return p.updateDailyCandles(symbol, dRange)
	case "W":
		return p.updateWeeklyCandles(symbol, dRange)
	case "Tick":
		return p.updateTicks(symbol, dRange)

	default:
		minutes, err := strconv.Atoi(tf)
		if err != nil {
			return errors.New("Can't recognize timeframe. Should be D, W, Tick or Intraday Minutes (1-60)")
		}

		if minutes < 1 || minutes > 60 {
			return errors.New("Intraday minutes should be in range 1-60")
		}

		return p.updateIntradayCandles(minutes, symbol, dRange)

	}

	return nil

}

func (p *JsonStorage) updateDailyCandles(s string, dRange DateRange) error {
	metaPath := path.Join(p.path, "candles/day/.meta", s+".json")
	downloadRange := dRange

	if fileExists(metaPath) {
		symbolMeta, err := p.loadMeta(metaPath)
		if err != nil {
			return err
		}

		downloadRange, err = p.findDailyRangeToDownload(dRange, symbolMeta)
		if err != nil {
			switch err.(type) {
			case *errNothingToDownload:
				return nil
			default:
				return err
			}
		}

	}

	candles, err1 := p.provider.GetCandles(s, "D", downloadRange)
	if err1 != nil {
		return err1
	}

	savePath := path.Join(p.path, "candles/day", s+".json")
	err2 := p.saveCandlesToFile(candles, savePath)

	if err2 != nil {
		return err2
	}

	newMeta := p.genNewDailySymbolMeta(s, downloadRange)
	err3 := newMeta.Save(metaPath)
	if err3 != nil {
		fmt.Println(err3)
		return err3
	}
	return nil

}

func (p *JsonStorage) saveCandlesToFile(candles []*Candle, savePath string) error {
	dirName := filepath.Dir(savePath)
	err := createDirIfNotExists(dirName)
	if err != nil {
		return err
	}

	json_, err := json.Marshal(candles)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(savePath, json_, 0644)
	if err != nil {
		fmt.Println(fmt.Sprintf("Can't write candles to file: %v", err))
	}

	return err
}

func (*JsonStorage) loadMeta(path string) (*JsonSymbolMeta, error) {
	symbolMeta := JsonSymbolMeta{}
	err := symbolMeta.Load(path)
	if err != nil {
		return nil, err
	}

	return &symbolMeta, nil

}

func (*JsonStorage) findDailyRangeToDownload(dRange DateRange, symbolMeta *JsonSymbolMeta) (DateRange, error) {
	firstListed, ok1 := symbolMeta.firstDate()
	lastListed, ok2 := symbolMeta.lastDate()

	if !ok1 && !ok2 {
		return dRange, nil
	}

	if firstListed.Unix() < dRange.from.Unix() && lastListed.Unix() > dRange.to.Unix() && ok1 && ok2 {
		// If we already have all candles in this date range just return without errors
		return DateRange{}, &errNothingToDownload{}
	}

	if dRange.from.Unix() > firstListed.Unix() && ok1 {
		dRange.from = firstListed
	}

	if dRange.to.Unix() < lastListed.Unix() && ok2 {
		dRange.to = lastListed
	}

	return dRange, nil

}

func (p *JsonStorage) genNewDailySymbolMeta(symbol string, dateRange DateRange) *JsonSymbolMeta {
	var listedDates []time.Time
	lastD := dateRange.from
	for {
		if lastD.After(dateRange.to) {
			break
		}
		listedDates = append(listedDates, lastD)
		lastD = lastD.AddDate(0, 0, 1)

	}

	symbolMeta := JsonSymbolMeta{
		symbol,
		"D",
		listedDates,
	}

	return &symbolMeta

}

func (p *JsonStorage) updateWeeklyCandles(s string, dRange DateRange) error {
	return nil

}
func (p *JsonStorage) updateTicks(s string, dRange DateRange) error {
	return nil
}
func (p *JsonStorage) updateIntradayCandles(minutes int, s string, dRange DateRange) error {
	return nil
}

func (p *JsonStorage) GetStoredCandles(TimeFrame string) ([]*Candle, error) {
	return nil, nil
}

func (p *JsonStorage) GetStoredTicks() ([]*Tick, error) {
	return nil, nil
}

func (*JsonStorage) readCandlesFromFile(path string) ([]*Candle, error) {
	if !fileExists(path) {
		return nil, nil //Todo what error?
	}

	jsonFile, err := os.Open(path)

	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var candles []*Candle

	err = json.Unmarshal(byteValue, &candles)

	if err != nil {
		return nil, err
	}

	return candles, err

}
