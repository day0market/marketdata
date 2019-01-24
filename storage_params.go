package marketdata

import (
	"time"
	"github.com/pkg/errors"
	"strconv"
)

type TimeOfDay struct{
	Hour int
	Minute int
	Second int
}

// Public request params
type TickUpdateParams struct {
	Symbol   string
	FromDate time.Time
	ToDate   time.Time
	StartTime TimeOfDay //We can request and store only particular time range of day. But if u mix requests if will give a mass in storage
	EndTime TimeOfDay
	Quotes   bool
	Trades   bool
}

func (p *TickUpdateParams) checkErrors() error {
	if !p.Trades && !p.Quotes {
		return errors.New("Wrong parameters. Should be selected trades, quotes or both")
	}

	if p.FromDate.After(p.ToDate) {
		return errors.New("From date should be less than To date")
	}

	if p.Symbol == "" {
		return errors.New("Symbol not specified")
	}

	return nil

}

func (p *TickUpdateParams) modifyTimes(loc *time.Location) error {
	p.FromDate = time.Date(p.FromDate.Year(), p.FromDate.Month(), p.FromDate.Day(), 0, 0, 0, 0, loc)
	p.ToDate = time.Date(p.ToDate.Year(), p.ToDate.Month(), p.ToDate.Day(), 0, 0, 0, 0, loc)

	yest := time.Now().In(loc).AddDate(0, 0, -1)
	yest = setTimeToSOD(yest)

	if p.ToDate.After(yest) {
		p.ToDate = yest
	}

	if p.FromDate.After(p.ToDate) {
		return errors.New("FromDate is after ToDate after internal modification. Maybe you requested only today's date? It's" +
			"not possible. We can't store current date in storage.")
	}

	return nil
}

type CandlesUpdateParams struct {
	Symbol         string
	TimeFrame      string
	FromDate       time.Time
	ToDate         time.Time
	UpdateWeekends bool
}

func (p *CandlesUpdateParams) modifyTimes() {
	p.ToDate = time.Date(p.ToDate.Year(), p.ToDate.Month(), p.ToDate.Day(), 0, 0, 0, 0, time.UTC)
	p.FromDate = time.Date(p.FromDate.Year(), p.FromDate.Month(), p.FromDate.Day(), 0, 0, 0, 0, time.UTC)
}

func (p *CandlesUpdateParams) checkErrors() error {
	if p.FromDate.After(p.ToDate) {
		return errors.New("From date should be less than To date")
	}

	if p.Symbol == "" {
		return errors.New("Symbol not specified")
	}

	if p.TimeFrame != "D" && p.TimeFrame != "W" {
		minutes, err := strconv.Atoi(p.TimeFrame)
		if err != nil {
			return errors.New("Can't recognize timeframe. Should be D, W, Tick or Intraday Minutes (1-60)")
		}

		if minutes < 1 || minutes > 60 {
			return errors.New("Intraday minutes should be in range 1-60")
		}

	}
	return nil
}
