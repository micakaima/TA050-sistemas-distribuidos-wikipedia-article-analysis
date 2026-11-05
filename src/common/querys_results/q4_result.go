package querysresults

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

// Representa el resultado final de la query 4. Implementa la interfaz WritableResult.
type Q4Result struct {
	TopWords generic.Batch[*WordFrequency]
}

// Representa una palabra con su frecuencia de aparición (presencia) entre los abstracts.
type WordFrequency struct {
	Word  string
	Count uint32
}

func (w *WordFrequency) ToRow() string {
	return fmt.Sprintf("%s,%d\n", w.Word, w.Count)
}

func (w *WordFrequency) ToBytes() ([]byte, error) {
	result := utils.SerializeString(w.Word)
	result = append(result, utils.SerializeUint32(w.Count)...)
	return result, nil
}

func WordFrequencyFromBytes(data []byte, offset *int) (*WordFrequency, error) {
	word, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	count, err := utils.ReadUint32FromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &WordFrequency{Word: word, Count: count}, nil
}

func NewEmptyWordFrequencyBatch(n int) generic.Batch[*WordFrequency] {
	return generic.NewEmptyBatch[*WordFrequency](n)
}

func WordFrequencyBatchFromBytes(data []byte) (*generic.Batch[*WordFrequency], error) {
	batch, err := generic.BatchFromBytes(data, WordFrequencyFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}

func (queryResult *Q4Result) ToBytes() ([]byte, error) {
	result := utils.SerializeUint32(uint32(queryResult.TopWords.Len()))
	for _, word := range queryResult.TopWords.Items {
		wordBytes, err := word.ToBytes()
		if err != nil {
			return nil, err
		}
		result = append(result, wordBytes...)
	}
	return result, nil
}

func Q4ResultFromBytes(data []byte) (*Q4Result, error) {
	offset := 0
	numWords, err := utils.ReadUint32FromBytes(data, &offset)
	if err != nil {
		return nil, err
	}

	words := NewEmptyWordFrequencyBatch(int(numWords))
	for range numWords {
		word, err := WordFrequencyFromBytes(data, &offset)
		if err != nil {
			return nil, err
		}
		words.Add(word)
	}
	return &Q4Result{TopWords: words}, nil
}

func (queryResult *Q4Result) WriteToFile(path string) error {
	dir := filepath.Dir(path)
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, word := range queryResult.TopWords.Items {
		if _, err := file.WriteString(word.ToRow()); err != nil {
			return err
		}
	}
	return nil
}

func (queryResult *Q4Result) DeserializeQueryResult(data []byte) (generic.Batch[*WordFrequency], error) {
	q4Result, err := Q4ResultFromBytes(data)
	if err != nil {
		return generic.Batch[*WordFrequency]{}, err
	}
	return q4Result.TopWords, nil
}

func (queryResult *Q4Result) NewEmptyBatch(len int) generic.Batch[*WordFrequency] {
	return NewEmptyWordFrequencyBatch(len)
}

func (queryResult *Q4Result) DeserializeArticleBatch(data []byte) (*generic.Batch[*WordFrequency], error) {
	return WordFrequencyBatchFromBytes(data)
}

func (queryResult *Q4Result) BuildQueryResult(batch generic.Batch[*WordFrequency]) *QueryResult {
	sort.Slice(batch.Items, func(i, j int) bool {
		if batch.Items[i].Count != batch.Items[j].Count {
			return batch.Items[i].Count > batch.Items[j].Count // count desc
		}
		return batch.Items[i].Word < batch.Items[j].Word // word asc
	})
	if len(batch.Items) > 20 {
		batch.Items = batch.Items[:20]
	}
	result := Q4Result{TopWords: batch}
	payload, _ := result.ToBytes()
	return &QueryResult{
		QueryCode:     Q4Code,
		ResultPayload: payload,
	}
}
