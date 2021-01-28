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
	"sync"
)

// Caching concurrency-safe mechanism for loading CSV data into scripts
type CsvLoader struct {
	m sync.RWMutex

	cache map[string][]interface{}

	open func(name string) (io.ReadCloser, error)
}

func NewCsvLoader() *CsvLoader {
	return &CsvLoader{
		cache: make(map[string][]interface{}),
		open:  func(name string) (io.ReadCloser, error) { return os.Open(name) },
	}
}

func (l *CsvLoader) getCached(name string) ([]interface{}, bool) {
	l.m.RLock()
	defer l.m.RUnlock()

	entry, found := l.cache[name]
	return entry, found
}

func (l *CsvLoader) Load(name string) ([]interface{}, error) {
	if cached, found := l.getCached(name); found {
		return cached, nil
	}

	// Not found; we've now dropped the read lock; get a write lock, check nobody else
	// did the deed in the interim and otherwise load
	l.m.Lock()
	defer l.m.Unlock()

	// Someone else may have had time, while we dropped the lock, to do the load
	cached, found := l.cache[name]
	if found {
		return cached, nil
	}

	// No, do the load
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

	l.cache[name] = out

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
		cache: make(map[string][]interface{}),
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
