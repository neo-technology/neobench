package neobench

import (
	"encoding/csv"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

// Caching concurrency-safe mechanism for loading CSV data into scripts
type CsvLoader struct {
	open func(name string) (io.ReadCloser, error)
}

func NewCsvLoader() *CsvLoader {
	return &CsvLoader{
		open: func(name string) (io.ReadCloser, error) { return os.Open(name) },
	}
}

func (l *CsvLoader) Load(name string) ([]interface{}, error) {
	f, err := l.open(name)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read csv '%s'", name)
	}
	defer f.Close()

	csvFile := csv.NewReader(f)
	csvFile.ReuseRecord = true
	csvFile.TrimLeadingSpace = true

	out := make([]interface{}, 0)
	for {
		rec, err := csvFile.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrapf(err, "error while reading CSV: '%s'", name)
		}

		row := make([]interface{}, len(rec))
		for i, cell := range rec {
			row[i] = csvParseCell(cell)
		}
		out = append(out, row)
	}

	return out, nil
}

func csvParseCell(raw string) interface{} {
	iVal, err := strconv.ParseInt(raw, 10, 64)
	if err == nil {
		return iVal
	}
	fVal, err := strconv.ParseFloat(raw, 64)
	if err == nil {
		return fVal
	}
	return raw
}

func fakeCsvLoader(files map[string]string) *CsvLoader {
	l := &CsvLoader{
		open: func(name string) (io.ReadCloser, error) {
			content, found := files[name]
			if !found {
				return nil, fmt.Errorf("(test) not found: %s", name)
			}
			return ioutil.NopCloser(strings.NewReader(content)), nil
		},
	}
	return l
}
