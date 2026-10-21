package record

import "bytes"
import "encoding/gob"
import "github.com/parquet-go/parquet-go"


type Record struct {
	Abstract string `parquet:"abstract"`
	DateModified int64 `parquet:"date_modified"`
	Image struct {
		ContentURL string `parquet:"content_url"`
	} `parquet:"image"`
	InLanguage string   `parquet:"in_language"`
	Name       string   `parquet:"name"`
	Sections   string `parquet:"sections"`
	URL        string   `parquet:"url"`
}

func ParseParquetBatchToRecords(batch []byte) ([]Record, error) {
	reader := parquet.NewGenericReader[Record](bytes.NewReader(batch))
	defer reader.Close()

	var records []Record
	buf := make([]Record, 100)

	for {
		n, err := reader.Read(buf)
		if n == 0 {  // EOF
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, buf[:n]...)
	}
	return records, nil
}

func ToBytes(records []Record) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(records)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func FromBytes(data []byte) ([]Record, error) {
	var records []Record
	dec := gob.NewDecoder(bytes.NewReader(data))

	err := dec.Decode(&records)
	if err != nil {
		return nil, err
	}
	return records, nil
}