package marketdata

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type DateRange struct {
	From time.Time
	To   time.Time
}

func (d *DateRange) String() string {
	l := "2006-01-02 15:04:05"
	return fmt.Sprintf("From: %v To: %v", d.From.Format(l), d.To.Format(l))
}

type Tick struct {
	Symbol string

	IsOpening bool
	IsClosing bool

	LastPrice float64
	LastSize  int64
	LastExch  string
	Datetime  time.Time

	BidExch string
	AskExch string

	BidPrice float64
	AskPrice float64
	BidSize  int64
	AskSize  int64

	CondQuote string
	Cond1     string
	Cond2     string
	Cond3     string
	Cond4     string
}

func (t *Tick) HasQuote() bool {
	if math.IsNaN(t.AskPrice) || math.IsNaN(t.BidPrice) {
		return false
	}
	if t.AskSize <= 0 || t.BidSize <= 0 {
		return false
	}

	if t.AskPrice <= 0 || t.BidPrice <= 0 {
		return false
	}

	return true
}

func (t *Tick) HasTrade() bool {
	if math.IsNaN(t.LastPrice) || t.LastPrice <= 0 {
		return false
	}

	if t.LastSize <= 0 {
		return false
	}

	return true

}

func (t *Tick) String() string {
	if t == nil {
		return ""
	}
	str := fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v", t.Datetime.Unix(), t.Symbol, t.LastPrice,
		t.LastSize, t.LastExch, t.BidPrice, t.BidSize, t.BidExch,
		t.AskPrice, t.AskSize, t.AskExch, t.CondQuote, t.Cond1, t.Cond2, t.Cond3, t.Cond4)

	return str
}

type Candle struct {
	Open         float64
	High         float64
	Low          float64
	Close        float64
	AdjClose     float64
	Volume       int64
	OpenInterest int64
	Datetime     time.Time
}

type QuoteSnapshot struct {
}

type CandleArray []*Candle
type TickArray []*Tick

func (t TickArray) Sort() TickArray {
	sort.SliceStable(t, func(i, j int) bool {
		return t[i].Datetime.Unix() < t[j].Datetime.Unix()
	})
	return t
}
