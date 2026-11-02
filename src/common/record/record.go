package record

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/parquet-go/parquet-go"
)

type Record struct {
	ArticleID    string `parquet:"identifier"`
	Abstract     string `parquet:"abstract"`
	DateModified int64  `parquet:"date_modified"`
	Image        struct {
		ContentURL string `parquet:"content_url"`
		Bytes      []byte `parquet:"-"`
		Error      string `parquet:"-"`
	} `parquet:"image"`
	InLanguage struct {
		Identifier string `parquet:"identifier"`
	} `parquet:"in_language"`
	Name     string `parquet:"name"`
	Sections string `parquet:"sections"`
	URL      string `parquet:"url"`
}

func ParseParquetBatchToRecords(batch []byte) ([]Record, error) {
	reader := parquet.NewGenericReader[Record](bytes.NewReader(batch))
	defer reader.Close()

	var records []Record
	buf := make([]Record, 100)

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			records = append(records, buf[:n]...)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return records, nil
}

func ToBytes[T any](records []T) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(records)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func FromBytes[T any](data []byte) ([]T, error) {
	var records []T
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&records)
	if err != nil {
		return nil, err
	}
	return records, nil
}
