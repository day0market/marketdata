package marketdata

import (
	"fmt"
)

type ErrUnexpectedResponseCode struct {
	code uint16
	url string
}

func (e *ErrUnexpectedResponseCode) Error() string {
	return fmt.Sprintf("Expected code 200, got %v. URL: %v", e.code, e.url)
}

type ErrParsingMarketData struct {
	raw string
	cls string
}

func (e *ErrParsingMarketData) Error() string {
	return fmt.Sprintf("Can't parse raw string: %v to %v struct", e.raw, e.cls)
}

type ErrWrongRequest struct {
	request string
}

func (e *ErrWrongRequest) Error() string {
	return fmt.Sprintf("Invalid request: %v", e.request)
}

func (e *ErrWrongRequest) Temporal() bool {
	return false
}

type ErrDatasourceNotConnected struct {
	datasource string
}

func (e *ErrDatasourceNotConnected) Error() string {
	return fmt.Sprintf("%v is not connected", e.datasource)
}

type ErrNothingToParse struct {
}

func (e *ErrNothingToParse) Error() string {
	return "Nothing to parse"
}

type HistoryProvider interface {
	GetCandles(symbol string, timeframe string, dRange DateRange) ([]*Candle, error)
	GetTicks(symbol string, dRange DateRange, quotes bool, trades bool) ([]*Tick, error)
}

type RealTimeTickProvider interface {
	Subscribe()
}
