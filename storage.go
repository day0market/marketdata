package marketdata

import (
	"fmt"
)

type ErrParsingStoredCandles struct {
	entry string
}

func (e *ErrParsingStoredCandles) Error() string {
	return fmt.Sprintf("Can't parse %v To candle", e.entry)
}

type Storage interface {
	GetStoredCandles(symbol string, tf string, dRange DateRange) (CandleArray, error)
	GetStoredTicks(symbol string, dRange DateRange, quotes bool, trades bool) (TickArray, error)
}
