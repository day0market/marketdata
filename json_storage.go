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
)

func fileExists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

type errNothingToDownload struct {
}

func (*errNothingToDownload) Error() string {
	return "Nothing to download"
}

type dateRange struct {
	from time.Time
	to   time.Time
}

func (d *dateRange) String() string {
	l := "2006-01-02 15:04:05"
	return fmt.Sprintf("From: %v To: %v", d.from.Format(l), d.to.Format(l))
}

type JsonSymbolMeta struct {
	Symbol    string
	TimeFrame string

	listedDates map[time.Time]struct{} //Time should be always 0
}

func (j *JsonSymbolMeta) getEmptyRanges(rng dateRange) ([]*dateRange, error) {
	last := rng.from

	var emptyDates []time.Time

	for {
		if last.After(rng.to) {
			break
		}
		_, ok := j.listedDates[last]

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
		rng := dateRange{
			emptyDates[0],
			emptyDates[0],
		}

		return []*dateRange{&rng}, nil
	}

	start, end := emptyDates[0], emptyDates[1]
	var emptyRanges []*dateRange

	for i, v := range emptyDates {
		if i < 2 {
			continue
		}

		delta := int(v.Sub(end).Hours() / 24)
		if delta > 1 {
			rng := dateRange{start, end}
			emptyRanges = append(emptyRanges, &rng)
			start, end = v, v
			continue
		}
		end = v

	}

	rngF := dateRange{start, end}
	emptyRanges = append(emptyRanges, &rngF)

	return emptyRanges, nil
}

func (j *JsonSymbolMeta) firstDate() (time.Time, bool) {
	minTime := time.Now().AddDate(0, 0, 1)
	for k := range j.listedDates {
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

	for k := range j.listedDates {
		if k.Unix() > maxTime.Unix() {
			maxTime = k
		}
	}
	if maxTime.IsZero() {
		return maxTime, false
	}
	return maxTime, true

}

type JsonStorage struct {
	path string
}

func (p *JsonStorage) ensureFolders() error {
	ticksFolder := path.Join(p.path, "ticks")
	candlesFolder := path.Join(p.path, "candles")

	if _, err := os.Stat(candlesFolder); os.IsNotExist(err) {

		err := os.MkdirAll(candlesFolder, os.ModePerm)
		return err
	}

	if _, err := os.Stat(ticksFolder); os.IsNotExist(err) {

		err := os.MkdirAll(ticksFolder, os.ModePerm)
		return err
	}

	return nil

}

func (p *JsonStorage) UpdateSymbol(symbol string, tf string, dRange dateRange) error {
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

func (p *JsonStorage) updateDailyCandles(s string, dRange dateRange) error {
	metaPath := path.Join(p.path, "candles/day/meta", s+".json")

	if fileExists(metaPath) {
		symbolMeta, err := p.loadMeta(metaPath)
		if err != nil {
			return err
		}

		downloadRange, err := p.findDailyRangeToDownload(dRange, symbolMeta)
		if err != nil {
			switch err.(type) {
			case *errNothingToDownload:
				return nil
			default:
				return err
			}
		}

		fmt.Println(downloadRange)

	}

	return nil

}

func (*JsonStorage) loadMeta(path string) (*JsonSymbolMeta, error) {
	jsonFile, err := os.Open(path)

	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var symbolMeta JsonSymbolMeta

	err = json.Unmarshal(byteValue, &symbolMeta)

	if err != nil {
		return nil, err
	}

	return &symbolMeta, nil

}

func (*JsonStorage) findDailyRangeToDownload(dRange dateRange, symbolMeta *JsonSymbolMeta) (dateRange, error) {
	firstListed, ok1 := symbolMeta.firstDate()
	lastListed, ok2 := symbolMeta.lastDate()

	if !ok1 && !ok2 {
		return dRange, nil
	}

	if firstListed.Unix() < dRange.from.Unix() && lastListed.Unix() > dRange.to.Unix() && ok1 && ok2 {
		// If we already have all candles in this date range just return without errors
		return dateRange{}, &errNothingToDownload{}
	}

	if dRange.from.Unix() > firstListed.Unix() && ok1 {
		dRange.from = firstListed
	}

	if dRange.to.Unix() < lastListed.Unix() && ok2 {
		dRange.to = lastListed
	}

	return dRange, nil

}

func (p *JsonStorage) updateWeeklyCandles(s string, dRange dateRange) error {
	return nil

}
func (p *JsonStorage) updateTicks(s string, dRange dateRange) error {
	return nil
}
func (p *JsonStorage) updateIntradayCandles(minutes int, s string, dRange dateRange) error {
	return nil
}

func (p *JsonStorage) GetStoredCandles(TimeFrame string) ([]*Candle, error) {
	return nil, nil
}

func (p *JsonStorage) GetStoredTicks() ([]*Tick, error) {
	return nil, nil
}

func MarshallCandles(candles []*Candle) ([]byte, error) {
	marshaled, err := json.Marshal(candles)
	if err != nil {
		return nil, err
	}
	//fmt.Println(marshaled)
	return marshaled, err
}

func UnMarshallCandles(json_ []byte) ([]*Candle, error) {
	var candles []*Candle

	err := json.Unmarshal(json_, &candles)

	return candles, err

}

func readCandlesFromFile(path string) ([]*Candle, error) {
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
