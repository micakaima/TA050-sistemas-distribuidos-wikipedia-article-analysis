package querysresults

import (
	"os"
	"path/filepath"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

// Representa el resultado final de la query 3. Implementa la interfaz WritableResult.
type Q3Result struct {
	ResultWords generic.Batch[*SharedWord]
}

// Representa una palabra única presente en abstracts de artículos en inglés y en francés.
type SharedWord struct {
	Word string
}

func (w *SharedWord) ToRow() string {
	return w.Word + "\n"
}

func (w *SharedWord) ToBytes() ([]byte, error) {
	return utils.SerializeString(w.Word), nil
}

func SharedWordFromBytes(data []byte, offset *int) (*SharedWord, error) {
	word, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &SharedWord{Word: word}, nil
}

func NewEmptySharedWordBatch(n int) generic.Batch[*SharedWord] {
	return generic.NewEmptyBatch[*SharedWord](n)
}

func SharedWordBatchFromBytes(data []byte) (*generic.Batch[*SharedWord], error) {
	batch, err := generic.BatchFromBytes(data, SharedWordFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}

func (queryResult *Q3Result) ToBytes() ([]byte, error) {
	result := utils.SerializeUint32(uint32(queryResult.ResultWords.Len()))
	for _, word := range queryResult.ResultWords.Items {
		wordBytes, err := word.ToBytes()
		if err != nil {
			return nil, err
		}
		result = append(result, wordBytes...)
	}
	return result, nil
}

func Q3ResultFromBytes(data []byte) (*Q3Result, error) {
	offset := 0
	numWords, err := utils.ReadUint32FromBytes(data, &offset)
	if err != nil {
		return nil, err
	}

	words := NewEmptySharedWordBatch(int(numWords))
	for range numWords {
		word, err := SharedWordFromBytes(data, &offset)
		if err != nil {
			return nil, err
		}
		words.Add(word)
	}
	return &Q3Result{ResultWords: words}, nil
}

func (queryResult *Q3Result) WriteToFile(path string) error {
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
	for _, word := range queryResult.ResultWords.Items {
		if _, err := file.WriteString(word.ToRow()); err != nil {
			return err
		}
	}
	return nil
}
