package marketdata

import (
	"testing"
	"time"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sort"
	"os"
	"bufio"
	"strings"
)

func mockActiveTick() *ActiveTick {
	at := NewActiveTick(84, "207.154.204.20", 2)
	return &at
}

func loadTicksMock() []time.Time {
	pth := "test_data/activetick/PSCC.txt"
	file, err := os.Open(pth)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var times []time.Time
	var prevTime time.Time
	line := 1
	for scanner.Scan() {
		splited := strings.Split(scanner.Text(), ",")
		time_, err := getTickTime(splited[1])
		if time_.Unix() < prevTime.Unix() {
			fmt.Println(fmt.Sprintf("Line: %v current date %v", line, time_))
		}
		line ++
		prevTime = *time_
		if err != nil {
			fmt.Println(err)
		}
		times = append(times, *time_)

	}

	isSorted := sort.SliceIsSorted(times, func(i, j int) bool {
		return times[i].Unix() < times[j].Unix()
	})
	if !isSorted {
		fmt.Println("Not sorted. Are u sure? ")
	}
	return times

}

func TestActiveTick_GetCandles_Day(t *testing.T) {
	at := mockActiveTick()
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
		t.Fatal(fmt.Sprintf("Expected %v, got %v", expectedDate, (candles)[0].Datetime))
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
	at := mockActiveTick()
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

	url = "http://207.154.204.20/optionChain?symbol=MSFT"
	resp, err = getResponse(url)

	assert.Equal(t, nil, err)
	assert.NotEqual(t, resp, "")
}

func TestActiveTick_GetTicks(t *testing.T) {
	symbol := "PSCC"

	at := mockActiveTick()
	loadTicksMock()

	from := time.Date(2018, 11, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2018, 11, 5, 0, 0, 0, 0, time.UTC)
	dRange := DateRange{
		from, to,
	}
	ticks, err := at.GetTicks(symbol, dRange, true, true)

	sorted := sort.SliceIsSorted(ticks, func(i, j int) bool {
		return ticks[i].Datetime.Unix() > ticks[j].Datetime.Unix()
	})

	assert.True(t, sorted)

	assert.Equal(t, nil, err)
	assert.True(t, len(ticks) > 0)

	for _, tk := range ticks {
		if tk.HasQuote {
			assert.True(t, tk.LastPrice == -1)
			assert.False(t, tk.BidPrice == -1)
			assert.False(t, tk.AskPrice == -1)
			continue
		}
		if tk.HasTrade {
			assert.False(t, tk.LastPrice == -1)
			assert.True(t, tk.BidPrice == -1)
			assert.True(t, tk.AskPrice == -1)

		}
	}

}

func TestActiveTick_parseTickTime(t *testing.T) {
	// Simple check

	str_time := "20181101075308267"
	time_, _ := getTickTime(str_time)

	expected := time.Date(2018, 11, 1, 7, 53, 8, 267000, time.UTC)

	assert.Equal(t, *time_, expected)

	// Check From file. All dates should be sorted if we read them properly

	pth := "test_data/activetick/PSCC.txt"
	file, err := os.Open(pth)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var times []time.Time
	var prevTime time.Time
	line := 1
	for scanner.Scan() {
		splited := strings.Split(scanner.Text(), ",")
		time_, err := getTickTime(splited[1])
		if err != nil {
			t.Fatal(err)
		}
		assert.True(t, time_.Unix() >= prevTime.Unix(), fmt.Sprintf("Line: %v current date %v", line, time_))

		line ++
		prevTime = *time_

		times = append(times, *time_)

	}

	isSorted := sort.SliceIsSorted(times, func(i, j int) bool {
		return times[i].Unix() < times[j].Unix()
	})
	assert.True(t, isSorted)

}
