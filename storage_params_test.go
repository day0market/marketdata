package marketdata

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
)

func TestTickUpdateParams_modifyTimes(t *testing.T) {
	fromDate := time.Date(2012, 11, 16, 12, 55, 50, 10, time.UTC)
	toDate := time.Date(2017, 2, 3, 19, 55, 50, 10, time.UTC)
	params := TickUpdateParams{
		Symbol:   "SPY",
		FromDate: fromDate,
		ToDate:   toDate,
		Trades:   true,
		Quotes:   true,
	}

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}

	params.modifyTimes(loc)

	expectingFrom := time.Date(2012, 11, 16, 0, 0, 0, 0, loc)
	expectingTo := time.Date(2017, 2, 3, 0, 0, 0, 0, loc)

	assert.Equal(t, params.FromDate, expectingFrom)
	assert.Equal(t, params.ToDate, expectingTo)

	toDate = time.Now().In(loc).AddDate(0, 0, 5)

	params = TickUpdateParams{
		Symbol:   "SPY",
		FromDate: fromDate,
		ToDate:   toDate,
		Trades:   true,
		Quotes:   true,
	}

	params.modifyTimes(loc)

	expectingTo = time.Now().AddDate(0, 0, -1)
	expectingTo = time.Date(expectingTo.Year(), expectingTo.Month(), expectingTo.Day(), 0, 0, 0, 0, loc)

	assert.Equal(t, params.FromDate, expectingFrom)
	assert.Equal(t, params.ToDate, expectingTo)

	fromDate = time.Date(2012, 11, 16, 2, 10, 0, 0, loc)
	toDate = time.Date(2012, 11, 16, 1, 25, 0, 0, loc)

	params = TickUpdateParams{
		Symbol:   "SPY",
		FromDate: fromDate,
		ToDate:   toDate,
		Trades:   true,
		Quotes:   true,
	}

	err = params.modifyTimes(loc)
	if err != nil {
		t.Fatal(err)
	}

	expecting := time.Date(2012, 11, 16, 0, 0, 0, 0, loc)

	assert.Equal(t, params.FromDate, expecting)
	assert.Equal(t, params.ToDate, expecting)

}
