package marketdata

import (
	"time"
	"fmt"
)

type ErrParsingStoredCandles struct {
	entry string
}

func (e *ErrParsingStoredCandles) Error() string {
	return fmt.Sprintf("Can't parse %v to candle", e.entry)
}

type Storage interface {
	GetListedDates(TimeFrame string) ([]time.Time, error)
	GetStoredCandles(TimeFrame string) ([]*Candle, error)
	GetStoredTicks() ([]*Tick, error)
}
