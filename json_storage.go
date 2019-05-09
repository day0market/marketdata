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
	"sync"
	"context"
	"strings"
)

const (
	tickfilelayout = "2006-01-02"
)

type tickRequestParams struct {
	trades    bool
	quotes    bool
	date      time.Time
	symbol    string
	startTime TimeOfDay
	endTime   TimeOfDay
}

type ErrNothingToDownload struct {
}

func (*ErrNothingToDownload) Error() string {
	return "Nothing To download"
}

type ErrSymbolDataNotFound struct {
	symbol string
	path   string
}

func (e *ErrSymbolDataNotFound) Error() string {
	return fmt.Sprintf("%v data not found in Path: %v", e.symbol, e.path)
}

// Symbol meta information *************************************************

type JsonSymbolMeta struct {
	Symbol      string
	TimeFrame   string
	ListedDates []time.Time
	HasWeekends bool
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

func (j *JsonSymbolMeta) datesSet() map[int64]struct{} {
	// To avoid TimeZone issues we put in dateset unix times.
	dates := make(map[int64]struct{})
	for _, v := range j.ListedDates {
		dates[v.UTC().Unix()] = struct{}{}
	}
	return dates
}

func (j *JsonSymbolMeta) getEmptyRanges(rng *DateRange) ([]*DateRange, error) {
	emptyDates, err := j.getEmptyDates(rng)
	if err != nil {
		return nil, err
	}

	if len(emptyDates) == 0 {
		return nil, &ErrNothingToDownload{}
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

	fmt.Println(emptyDates)
	min_split := 1
	if j.HasWeekends {
		min_split = 3
	}

	for i := range emptyDates {
		if i < 2 {
			continue
		}

		prev := emptyDates[i-1]
		curr := emptyDates[i]
		delta := int(curr.Sub(prev).Hours() / 24)
		fmt.Println(delta, prev, curr)

		if delta > min_split {
			rng := DateRange{start, prev}
			emptyRanges = append(emptyRanges, &rng)
			start = curr
			fmt.Println(emptyRanges)
			continue
		}
		end = curr
		fmt.Println(emptyRanges)

	}

	rngF := DateRange{start, end}
	emptyRanges = append(emptyRanges, &rngF)

	return emptyRanges, nil
}

func (j *JsonSymbolMeta) getEmptyDates(rng *DateRange) ([]time.Time, error) {
	last := rng.From

	var emptyDates []time.Time
	datesSet := j.datesSet()

	if rng.From.Equal(rng.To) {
		if _, ok := datesSet[rng.From.UTC().Unix()]; ok {
			return emptyDates, nil
		}
		emptyDates = append(emptyDates, rng.From)
		return emptyDates, nil
	}

	for {
		if last.After(rng.To) {
			break
		}
		if j.HasWeekends && (last.Weekday() == 0 || last.Weekday() == 6) {
			last = last.AddDate(0, 0, 1)
			continue
		}
		_, ok := datesSet[last.UTC().Unix()]

		if !ok {
			emptyDates = append(emptyDates, last)
		}

		last = last.AddDate(0, 0, 1)

	}
	fmt.Println(emptyDates)

	return emptyDates, nil

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

func (j *JsonSymbolMeta) save(savePath string) error {
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

func loadMetaIfExists(metaPath string) *JsonSymbolMeta {
	jsonMeta := JsonSymbolMeta{}

	if fileExists(metaPath) {
		jsonMeta.Load(metaPath)

	}

	return &jsonMeta

}

// Storage code starts here **************************************************

type JsonStorage struct {
	UpdateWorkers int
	Path          string
	Provider      HistoryProvider
	TimeZone      *time.Location
	HasWeekends   bool
}

func (p *JsonStorage) createFolders() error {
	ticksFolder := path.Join(p.Path, "ticks")
	candlesFolder := path.Join(p.Path, "candles")

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

func (p *JsonStorage) GetStoredCandles(symbol string, tf string, dRange DateRange) (CandleArray, error) {

	switch tf {
	case "D":
		pth := path.Join(p.Path, "candles", "day", symbol+".json")
		return p.readCandlesFromFile(pth)
	case "W":
		return nil, errors.New("Not implemented")

	default:
		return nil, errors.New("Not implemented")
	}

}

func (p *JsonStorage) GetStoredTicks(symbol string, dRange DateRange, quotes bool, trades bool) (TickArray, error) {

	symbolTickFolder := path.Join(p.Path, "ticks", p.generateTicksFolderName(quotes, trades), symbol)

	if !fileExists(symbolTickFolder) {

		return nil, errors.New(symbol + " not found in storage")
	}

	start := dRange.From
	var loaded TickArray

	for {
		if start.After(dRange.To) {
			break
		}

		if p.HasWeekends && (start.Weekday() == 6 || start.Weekday() == 0) {
			start = start.AddDate(0, 0, 1)
			continue
		}

		pth := path.Join(symbolTickFolder, start.Format(tickfilelayout)+".json")

		if !fileExists(pth) {
			fmt.Println(fmt.Sprintf("file not found %v", pth))
			//Todo log here?
			start = start.AddDate(0, 0, 1)
			continue
		}

		ticks, err := p.readTicksFromFile(pth)
		if err != nil {
			//Todo log here?
			fmt.Println(fmt.Sprintf("error during reading From file %v", err))
			start = start.AddDate(0, 0, 1)
			continue
		}

		loaded = append(loaded, *ticks...)
		start = start.AddDate(0, 0, 1)

	}

	return loaded, nil
}

func (p *JsonStorage) UpdateSymbolCandles(params CandlesUpdateParams) error {
	err := params.checkErrors()

	if err != nil {
		return err
	}

	params.modifyTimes()

	dRange := DateRange{
		params.FromDate, params.ToDate,
	}
	switch params.TimeFrame {
	case "D":
		return p.updateDailyCandles(params.Symbol, &dRange)
	case "W":
		return p.updateWeeklyCandles(params.Symbol, &dRange)

	default:
		minutes, err := strconv.Atoi(params.TimeFrame)
		if err != nil {
			return errors.New("Can't recognize timeframe. Should be D, W, Tick or Intraday Minutes (1-60)")
		}

		if minutes < 1 || minutes > 60 {
			return errors.New("Intraday minutes should be in range 1-60")
		}

		return p.updateIntradayCandles(minutes, params.Symbol, &dRange)

	}

}

func (p *JsonStorage) saveCandlesToFile(candles *CandleArray, savePath string) error {
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
		fmt.Println(fmt.Sprintf("Can't write candles To file: %v", err))
	}

	return err
}

func (*JsonStorage) readCandlesFromFile(pth string) (CandleArray, error) {
	if !fileExists(pth) {
		return nil, &ErrSymbolDataNotFound{"", pth}
	}

	jsonFile, err := os.Open(pth)

	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var candles CandleArray

	err = json.Unmarshal(byteValue, &candles)

	if err != nil {
		return nil, err
	}

	return candles, err

}

func (p *JsonStorage) updateDailyCandles(s string, dRange *DateRange) error {

	downloadRange, err := p.findDailyRangeToDownload(dRange, s)
	if err != nil {
		switch err.(type) {
		case *ErrNothingToDownload:
			return nil
		default:
			return err
		}
	}

	candles, err1 := p.Provider.GetCandles(s, "D", *downloadRange)
	if err1 != nil {
		return err1
	}

	savePath := path.Join(p.Path, "candles/day", s+".json")
	err2 := p.saveCandlesToFile(&candles, savePath)

	if err2 != nil {
		return err2
	}

	newMeta := p.genNewDailySymbolMeta(s, downloadRange)
	metaPath := path.Join(p.Path, "candles/day/.meta", s+".json")
	err3 := newMeta.save(metaPath)
	if err3 != nil {
		fmt.Println(err3)
		return err3
	}
	return nil

}

func (p *JsonStorage) findDailyRangeToDownload(dRange *DateRange, symbol string) (*DateRange, error) {
	metaPath := path.Join(p.Path, "candles/day/.meta", symbol+".json")
	downloadRange := *dRange

	if !fileExists(metaPath) {
		return &downloadRange, nil
	}

	symbolMeta := JsonSymbolMeta{}
	err := symbolMeta.Load(metaPath)
	if err != nil {
		return nil, err
	}

	firstListed, ok1 := symbolMeta.firstDate()
	lastListed, ok2 := symbolMeta.lastDate()

	if !ok1 && !ok2 {
		return &downloadRange, nil
	}

	if firstListed.Unix() < dRange.From.Unix() && lastListed.Unix() > dRange.To.Unix() && ok1 && ok2 {
		// If we already have all candles in this date range just return without errors
		return nil, &ErrNothingToDownload{}
	}

	if dRange.From.Unix() > firstListed.Unix() && ok1 {
		downloadRange.From = firstListed
	}

	if dRange.To.Unix() < lastListed.Unix() && ok2 {
		downloadRange.To = lastListed
	}

	return &downloadRange, nil

}

func (p *JsonStorage) genNewDailySymbolMeta(symbol string, dateRange *DateRange) *JsonSymbolMeta {
	var listedDates []time.Time
	lastD := dateRange.From
	for {
		if lastD.After(dateRange.To) {
			break
		}
		listedDates = append(listedDates, lastD)
		lastD = lastD.AddDate(0, 0, 1)

	}

	symbolMeta := JsonSymbolMeta{
		symbol,
		"D",
		listedDates,
		p.HasWeekends,
	}

	return &symbolMeta

}

func (p *JsonStorage) updateWeeklyCandles(s string, dRange *DateRange) error {
	return nil

}

func (p *JsonStorage) updateIntradayCandles(minutes int, s string, dRange *DateRange) error {
	return nil
}

/* Updates symbol ticks in given time range. Begin and end dates are included. Not matter what is time of the day
set. For example if end date is 2012-01-12 9:45AM we download full day of 2012-01-12. If end date is today, we cut range
and update everything until yesterday. Today is always ignored. We use TimeZone To check this
*/
func (p *JsonStorage) UpdateSymbolTicks(params TickUpdateParams) error {
	err := params.checkErrors()
	if err != nil {
		return err
	}

	err = params.modifyTimes(p.TimeZone)
	if err != nil {
		return err
	}

	folderName := p.generateTicksFolderName(params.Quotes, params.Trades)
	metaPath := path.Join(p.Path, "ticks", folderName, ".meta", params.Symbol+".json")

	jsonMeta := loadMetaIfExists(metaPath)
	jsonMeta.HasWeekends = p.HasWeekends

	dRange := DateRange{params.FromDate, params.ToDate}

	emptyDates, err := jsonMeta.getEmptyDates(&dRange)

	if err != nil {
		return err
	}

	if emptyDates == nil {
		return errors.Wrapf(&ErrNothingToDownload{}, "UpdateSymbolTicks() Symbol: %v dRange: %v", params.Symbol, &dRange)
	}

	wg := &sync.WaitGroup{}

	datesChan := make(chan tickRequestParams, 2)
	errorsChan := make(chan error)
	successChan := make(chan struct{}, 1)
	ctx, finish := context.WithCancel(context.Background())

	defer func() {
		if errorsChan != nil {
			//close(errorsChan)
			errorsChan = nil
		}
		if successChan != nil {
			//close(successChan)
			successChan = nil
		}

		finish()

		storageFolder := path.Join(p.Path, "ticks", folderName, params.Symbol)
		listedDates, err := p.getStoredTickDates(storageFolder)
		if err != nil {
			//Todo log here?
			return
		}
		jsonMeta.ListedDates = listedDates
		jsonMeta.save(metaPath)

	}()

	var retError error

	//Workers pool
	for i := 0; i < p.UpdateWorkers; i++ {
		go func() {
			wg.Add(1)
			p.tickUpdateWorker(ctx, datesChan, errorsChan, successChan)
			fmt.Println("Done")
			wg.Done()
		}()
	}

	// Requests producer
	go func() {
		defer close(datesChan)
		for _, d := range emptyDates {

			params := tickRequestParams{
				params.Trades,
				params.Quotes,
				d,

				params.Symbol,
				params.StartTime,
				params.EndTime,
			}

			datesChan <- params
		}

	}()

	counter := 0

LoadingLoop:
	for {

		select {
		case err, ok := <-errorsChan:
			if !ok {
				continue LoadingLoop
			}
			errorsChan = nil
			datesChan = nil
			finish()
			return err
		case <-successChan:
			counter++
			fmt.Println(counter, len(emptyDates))
			if counter == len(emptyDates) {
				finish()
				break LoadingLoop
			}

		default:
			if counter == len(emptyDates) {
				finish()
				break LoadingLoop
			}

		}
	}

	wg.Wait()

	return retError
}

func (p *JsonStorage) tickUpdateWorker(ctx context.Context, params chan tickRequestParams, errorsChan chan<- error,
	successChan chan<- struct{}) {
LOOP:
	for {
		select {
		case <-ctx.Done():
			return
		case par, ok := <-params:

			if !ok {
				return
			}

			d := par.date
			r := DateRange{}
			r.From = time.Date(d.Year(), d.Month(), d.Day(), par.startTime.Hour, par.startTime.Minute, par.startTime.Second, 0, time.UTC)
			r.To = time.Date(d.Year(), d.Month(), d.Day(), par.endTime.Hour, par.endTime.Minute, par.endTime.Second, 0, time.UTC)

			folderName := p.generateTicksFolderName(par.quotes, par.trades)

			savePath := path.Join(p.Path, "ticks", folderName, par.symbol, par.date.Format(tickfilelayout)+".json")

			ticks, err := p.Provider.GetTicks(par.symbol, r, par.quotes, par.trades)
			if err != nil {
				switch errors.Cause(err).(type) {
				case *ErrEmptyResponse:
					err = p.saveTicksToFile(&ticks, savePath)
					if err != nil {
						errorsChan <- err
						return
					}
					successChan <- struct{}{}
					continue LOOP

				default:
					errorsChan <- err
					return

				}
			}

			err = p.saveTicksToFile(&ticks, savePath)

			if err != nil {
				errorsChan <- err
				return
			}

			successChan <- struct{}{}

		}

	}
}

func (*JsonStorage) generateTicksFolderName(quotes bool, trades bool) string {
	folderName := ""
	if quotes {
		folderName += "quotes"
	}
	if trades {
		if folderName != "" {
			folderName += "_trades"
		} else {
			folderName += "trades"
		}
	}

	return folderName
}

func (p *JsonStorage) saveTicksToFile(ticks *TickArray, savePath string) error {
	dirName := filepath.Dir(savePath)
	err := createDirIfNotExists(dirName)
	if err != nil {
		return err
	}

	json_, err := json.Marshal(ticks)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(savePath, json_, 0644)
	if err != nil {
		fmt.Println(fmt.Sprintf("Can't write ticks To file: %v", err))
	}

	return err
}

func (*JsonStorage) readTicksFromFile(pth string) (*TickArray, error) {
	if !fileExists(pth) {
		return nil, &ErrSymbolDataNotFound{"", pth}
	}

	jsonFile, err := os.Open(pth)

	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var ticks TickArray

	err = json.Unmarshal(byteValue, &ticks)

	if err != nil {
		return nil, err
	}

	return &ticks, err

}

func (p *JsonStorage) getStoredTickDates(pth string) ([]time.Time, error) {
	var listed []time.Time
	files, err := ioutil.ReadDir(pth)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		filename := strings.Split(f.Name(), ".")[0]
		t, err := time.Parse(tickfilelayout, filename)
		if err != nil {
			// Todo log???
			continue
		}

		t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, p.TimeZone)
		listed = append(listed, t)
	}

	return listed, nil

}
