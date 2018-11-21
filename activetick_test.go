package marketdata

import (
	"testing"
	"time"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
)

func TestActiveTick_GetCandles_Day(t *testing.T) {
	at := NewActiveTick(5000, "localhost", 2)
	from := time.Date(2018, 11, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2018, 11, 10, 0, 0, 0, 0, time.UTC)
	dRange := DateRange{
		from, to,
	}
	candles, err := at.GetCandles("SPY", "D", dRange)
	if err != nil {
		t.Fatal(fmt.Sprintf("%v", err))
	}

	if len(candles) != 7 {
		t.Fatal(fmt.Sprintf("Candles len mistmatch %v", len(candles)))
	}

	expectedDate := time.Date(2018, 11, 1, 0, 0, 0, 0, time.UTC)

	if candles[0].Datetime != expectedDate {
		t.Fatal(fmt.Sprintf("Expected %v, got %v", expectedDate, candles[0].Datetime))
	}

	//20181101000000,271.600000,273.730000,270.380000,273.370000,89496311
	expectedCandle := Candle{
		271.60,
		273.73,
		270.38,
		273.37,
		273.37,
		89496311,
		0,
		expectedDate,
	}

	if *candles[0] != expectedCandle {
		t.Fatal(fmt.Sprintf("Expected candle %v but got %v", expectedCandle, *candles[0]))
	}
}

func TestActiveTick_GetCandles_Intraday(t *testing.T) {
	at := NewActiveTick(5000, "localhost", 2)
	from := time.Date(2018, 11, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2018, 11, 10, 0, 0, 0, 0, time.UTC)
	dRange := DateRange{
		from, to,
	}
	candles, err := at.GetCandles("SPY", "30", dRange)
	if err != nil {
		t.Fatal(fmt.Sprintf("%v", err))
	}

	if len(candles) != 98 {
		t.Fatal(fmt.Sprintf("Candles len mistmatch %v", len(candles)))
	}

	expectedDate := time.Date(2018, 11, 1, 9, 30, 0, 0, time.UTC)

	if candles[0].Datetime != expectedDate {
		t.Fatal(fmt.Sprintf("Expected %v, got %v", expectedDate, candles[0].Datetime))
	}

	//20181101093000,271.600000,272.180000,270.780000,271.290000,12164925
	expectedCandle := Candle{
		271.60,
		272.18,
		270.78,
		271.29,
		271.29,
		12164925,
		0,
		expectedDate,
	}

	if *candles[0] != expectedCandle {
		t.Fatal(fmt.Sprintf("Expected candle %v but got %v", expectedCandle, *candles[0]))
	}
}

func TestActiveTick_getResponse(t *testing.T) {
	url := "http://localhost:5001/optionChain?symbol=msft"
	resp, err := getResponse(url)
	expectedError := ErrDatasourceNotConnected{"ActiveTick"}
	assert.Equal(t, &expectedError, err)
	assert.Equal(t, resp, "")

	url = "http://localhost:5000/optionChain?symbol=MSFT"
	resp, err = getResponse(url)

	assert.Equal(t, nil, err)
	assert.NotEqual(t, resp, "")
}

func TestActiveTick_GetTicks(t *testing.T) {
	symbol := "PSCC"

	at := NewActiveTick(5000, "localhost", 2)
	from := time.Date(2018, 11, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2018, 11, 5, 0, 0, 0, 0, time.UTC)
	dRange := DateRange{
		from, to,
	}
	ticks, err := at.GetTicks(symbol, dRange, true, true)

	assert.Equal(t, nil, err)
	assert.True(t, len(ticks) > 0)
	fmt.Println(ticks[1])
	for _, tk := range ticks {
		if tk.HasQuote {
			assert.True(t, math.IsNaN(tk.LastPrice))
			assert.False(t, math.IsNaN(tk.BidPrice))
			assert.False(t, math.IsNaN(tk.AskPrice))
			continue
		}
		if tk.HasTrade {
			assert.False(t, math.IsNaN(tk.LastPrice))
			assert.True(t, math.IsNaN(tk.BidPrice))
			assert.True(t, math.IsNaN(tk.AskPrice))

		}
	}

}
