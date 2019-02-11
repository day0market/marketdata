package marketdata

import (
	"time"
	"strconv"
	"net/http"
	"io/ioutil"
	"strings"
	"fmt"
	"github.com/pkg/errors"
)

const (
	layout = "20060102150405"
)

func convertTimeToActiveTickFormat(t time.Time) string {
	return t.Format(layout)
}

type ActiveTick struct {
	tries   uint8
	baseurl string
}

func NewActiveTick(port uint16, host string, tries uint8) ActiveTick {
	baseurl := "http://" + host
	if port != 0 && port != 84 {
		baseurl += ":" + strconv.Itoa(int(port))
	}

	at := ActiveTick{
		tries,
		baseurl,
	}

	return at
}

func (a ActiveTick) GetCandles(symbol string, timeFrame string, dRange DateRange) (CandleArray, error) {

	from := convertTimeToActiveTickFormat(dRange.From)
	to := convertTimeToActiveTickFormat(dRange.To)

	var uri string

	switch timeFrame {

	case "D":
		uri = fmt.Sprintf("/barData?symbol=%v&historyType=1&beginTime=%v&endTime=%v",
			strings.ToUpper(symbol), from, to)
	case "W":
		uri = fmt.Sprintf("/barData?symbol=%v&historyType=2&beginTime=%v&endTime=%v",
			strings.ToUpper(symbol), from, to)

	default:
		mins, err := strconv.Atoi(timeFrame)
		if err != nil {
			return nil, err
		}
		if mins > 60 || mins < 1 {
			return nil, errors.New("Intraday minutes should be From 1 To 60")
		}
		uri = fmt.Sprintf("/barData?symbol=%v&historyType=0&intradayMinutes=%v&beginTime=%v&endTime=%v",
			strings.ToUpper(symbol), timeFrame, from, to)

	}

	rawData, err := a.getRawData(uri)

	if err != nil {
		return nil, err
	}

	candles, err := parseToCandlesList(rawData)

	return candles, err
}

func (a ActiveTick) GetTicks(symbol string, dRange DateRange, quotes bool, trades bool) (TickArray, error) {

	if !quotes && !trades {
		err := ErrWrongRequest{"Should be selected trades, quotes or both"}
		params := struct {
			symbol string
			dRange DateRange
			quotes bool
			trades bool
		}{symbol, dRange, quotes, trades}
		return nil, errors.Wrapf(&err, "GetTicks(%v)", params)
	}

	q, t := 1, 1
	if !quotes {
		q = 0
	}
	if !trades {
		t = 0
	}

	from := convertTimeToActiveTickFormat(dRange.From)
	to := convertTimeToActiveTickFormat(dRange.To)

	uri := fmt.Sprintf("/tickData?symbol=%v&trades=%v&quotes=%v&beginTime=%v&endTime=%v", symbol, t, q, from, to)


	rawData, err := a.getRawData(uri)

	if err != nil {
		return nil, err
	}

	ticks, err := parseToTQ(rawData)
	if err != nil {
		return nil, err
	}

	return ticks, nil

}

func (a ActiveTick) GetQuotesSnapshot(symbols []string) ([]QuoteSnapshot, error) {
	return nil, nil

}

func (a ActiveTick) getRawData(uri string) (string, error) {
	url := a.baseurl + uri

	var tries uint8

	for {
		tries ++

		content, err := getResponse(url)

		if err != nil {
			switch errors.Cause(err).(type) {
			case *ErrUnexpectedResponseCode:
				return "", errors.Wrapf(err, "getRawData(%v)", uri)
			case *ErrWrongRequest:
				return "", errors.Wrapf(err, "getRawData(%v)", uri)
			case *ErrEmptyResponse:
				return "", errors.Wrapf(err, "getRawData(%v)", uri)
			case *ErrDatasourceNotConnected:
				return "", errors.Wrapf(err, "getRawData(%v)", uri)

			default:
				if tries > a.tries {
					return "", errors.Wrapf(err, "getRawData(%v) tries: %v", uri, tries)
				}
				continue
			}
		}

		return content, err
	}

	return "", nil

}

func getResponse(url string) (string, error) {
	response, err := http.Get(url)

	if err != nil {
		if strings.Contains(err.Error(), "target machine actively refused") {
			return "", &ErrDatasourceNotConnected{"ActiveTick"}
		}
		return "", err
	}

	if response.StatusCode != 200 {
		return "", &ErrUnexpectedResponseCode{uint16(response.StatusCode), url}
	}

	defer response.Body.Close()

	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	content_s := string(content)

	if strings.HasPrefix(content_s, "0") {
		fmt.Println("Prefix: " + content_s)
		if strings.Contains(content_s, "client is not connected") {
			return "", &ErrDatasourceNotConnected{"ActiveTick"}
		}
		return "", &ErrEmptyResponse{url}
	}
	return string(content_s), err

}

func parseToCandlesList(raw string) (CandleArray, error) {
	if raw == "" {
		return nil, &ErrNothingToParse{}
	}
	lines := strings.Split(raw, "\r\n")
	var candles CandleArray

	for _, l := range lines {

		s := strings.Split(l, ",")

		if len(s) != 6 {
			if !strings.Contains(l, ",") {
				continue
			}
			return nil, &ErrParsingMarketData{l, "Candle"}
			//return candles, errors.New(fmt.Sprintf("Can't parse To candle. Wrong string len: %v. Row: %q. Parsed: %v",
			//	len(s), l, len(candles)))
		}
		if s[0] == "00000000000000" {
			return nil, &ErrParsingMarketData{"Wrong time: " + l, "Candle"}
		}
		datetime, err := time.Parse(layout, s[0])
		if err != nil {
			return nil, &ErrParsingMarketData{l, "Candle"}
		}

		open, err := strconv.ParseFloat(s[1], 64)
		if err != nil {
			return nil, &ErrParsingMarketData{l, "Candle"}
		}

		high, err := strconv.ParseFloat(s[2], 64)
		if err != nil {
			return nil, &ErrParsingMarketData{l, "Candle"}
		}

		low, err := strconv.ParseFloat(s[3], 64)
		if err != nil {
			return nil, &ErrParsingMarketData{l, "Candle"}
		}

		close_, err := strconv.ParseFloat(s[4], 64)
		if err != nil {
			return nil, &ErrParsingMarketData{l, "Candle"}
		}

		volume, err := strconv.ParseInt(s[5], 10, 64)
		if err != nil {
			return nil, &ErrParsingMarketData{l, "Candle"}
		}

		candle := Candle{
			open,
			high,
			low,
			close_,
			close_,
			volume,
			0,
			datetime,
		}

		candles = append(candles, &candle)

	}

	return candles, nil
}

func parseToTQ(raw string) (TickArray, error) {
	if raw == "" {
		return nil, &ErrNothingToParse{}
	}
	lines := strings.Split(raw, "\r\n")
	var ticks TickArray

	for _, l := range lines {

		s := strings.Split(l, ",")

		if len(s) != 9 {
			if !strings.Contains(l, ",") {
				continue
			}
			return nil, &ErrParsingMarketData{l, "ToQ"}
		}

		if s[0] == "T" {
			tq, err := parseTickLine(s)
			if err != nil {
				return nil, err
			}
			ticks = append(ticks, tq)
			continue
		}

		if s[0] == "Q" {
			tq, err := parseQuoteLine(s)
			if err != nil {
				return nil, err
			}
			ticks = append(ticks, tq)
			continue
		}

	}

	return ticks, nil

}

func parseTickLine(s []string) (*Tick, error) {
	// T,20120803153000551,616.550000,100,Y,0,14,0,0
	datetime, err := getTickTime(s[1])

	if err != nil {
		return nil, err
	}

	last, err := strconv.ParseFloat(s[2], 64)
	if err != nil {
		return nil, &ErrParsingMarketData{s[2], "Tick"}
	}

	lastSize, err := strconv.ParseInt(s[3], 10, 64)
	if err != nil {
		return nil, &ErrParsingMarketData{s[2], "Tick"}
	}

	lastExch, cond1, cond2, cond3, cond4 := s[4], s[5], s[6], s[7], s[8]

	tick := Tick{
		false,
		true,
		false,
		false,
		last,
		lastSize,
		lastExch,
		*datetime,
		"",
		"",
		-1,
		-1,
		-1,
		-1,
		"",
		cond1,
		cond2,
		cond3,
		cond4,
	}

	return &tick, nil

}

func parseQuoteLine(s []string) (*Tick, error) {
	// Q,20120803153000133,616.540000,616.630000,2,1,B,Q,0
	datetime, err := getTickTime(s[1])

	if err != nil {
		return nil, err
	}

	bid, err := strconv.ParseFloat(s[2], 64)
	if err != nil {
		return nil, &ErrParsingMarketData{s[2], "Tick"}
	}

	ask, err := strconv.ParseFloat(s[3], 64)
	if err != nil {
		return nil, &ErrParsingMarketData{s[3], "Tick"}
	}

	bidsize, err := strconv.ParseInt(s[4], 10, 64)
	if err != nil {
		return nil, &ErrParsingMarketData{s[4], "Tick"}
	}

	askSize, err := strconv.ParseInt(s[5], 10, 64)
	if err != nil {
		return nil, &ErrParsingMarketData{s[5], "Tick"}
	}

	bidExch, askExch, condQ := s[6], s[7], s[8]

	tick := Tick{
		true,
		false,
		false,
		false, //Todo
		-1,
		-1,
		"",
		*datetime,
		bidExch,
		askExch,
		bid,
		ask,
		bidsize,
		askSize,
		condQ,
		"",
		"",
		"",
		"",
	}

	return &tick, nil

}

func getTickTime(s string) (*time.Time, error) {
	str_time := s
	datetime, err := time.Parse(layout, str_time[:len(str_time)-3])
	if err != nil {
		return nil, &ErrParsingMarketData{"Can't parse time: " + s, "Tick"}

	}

	//Go can't parse tick datetime format with ms. So here is a hook. We parse time without
	//ms and after that we add them
	ms, err := strconv.Atoi(str_time[len(str_time)-3:])
	if err != nil {
		return nil, &ErrParsingMarketData{"Can't extract ms: " + s, "Tick"}
	}

	ms_to_add := time.Duration(ms) * time.Microsecond
	datetime = datetime.Add(ms_to_add)

	return &datetime, nil

}
