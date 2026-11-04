package querysresults

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

const DATE_FORMAT = "02-01-2006"

// Representa el resultado final de la query 1. Implementa la interfaz WritableResult.
type Q1Result struct {
	ResultArticles generic.Batch[*English2020Article]
}

// Representa un artículo escrito en inglés hasta el 2020 inclusive.
type English2020Article struct {
	ArticleID    string
	DateModified int64
	URL          string
}

func (article *English2020Article) ToRow() string {
	date := time.UnixMilli(article.DateModified).UTC().Format(DATE_FORMAT)
	return fmt.Sprintf("%s,%s,%s\n", article.ArticleID, date, article.URL)
}

func (article *English2020Article) ToBytes() ([]byte, error) {
	result := utils.SerializeString(article.ArticleID)
	result = append(result, utils.SerializeUint64(uint64(article.DateModified))...)
	result = append(result, utils.SerializeString(article.URL)...)
	return result, nil
}

func English2020ArticleFromBytes(data []byte, offset *int) (*English2020Article, error) {
	articleID, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	dateModified, err := utils.ReadUint64FromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	url, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &English2020Article{
		ArticleID:    articleID,
		DateModified: int64(dateModified),
		URL:          url,
	}, nil
}

func NewEmptyEnglish2020ArticleBatch(len int) generic.Batch[*English2020Article] {
	return generic.NewEmptyBatch[*English2020Article](len)
}

func English2020ArticleBatchFromBytes(data []byte) (*generic.Batch[*English2020Article], error) {
	batch, err := generic.BatchFromBytes(data, English2020ArticleFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}

func (queryResult *Q1Result) ToBytes() ([]byte, error) {
	result := utils.SerializeUint32(uint32(queryResult.ResultArticles.Len()))
	for _, article := range queryResult.ResultArticles.Items {
		articleBytes, err := article.ToBytes()
		if err != nil {
			return nil, err
		}
		result = append(result, articleBytes...)
	}
	return result, nil
}

func Q1ResultFromBytes(data []byte) (*Q1Result, error) {
	offset := 0
	numArticles, err := utils.ReadUint32FromBytes(data, &offset)
	if err != nil {
		return nil, err
	}

	articles := NewEmptyEnglish2020ArticleBatch(int(numArticles))
	for range numArticles {
		article, err := English2020ArticleFromBytes(data, &offset)
		if err != nil {
			return nil, err
		}
		articles.Add(article)
	}
	q1Result := Q1Result{ResultArticles: articles}
	return &q1Result, nil
}

func (queryResult *Q1Result) WriteToFile(path string) error {
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
	for _, article := range queryResult.ResultArticles.Items {
		if _, err := file.WriteString(article.ToRow()); err != nil {
			return err
		}
	}
	return nil
}

func (queryResult *Q1Result) DeserializeQueryResult(data []byte) (generic.Batch[*English2020Article], error) {
	q1Result, err := Q1ResultFromBytes(data)
	if err != nil {
		return generic.Batch[*English2020Article]{}, err
	}
	return q1Result.ResultArticles, nil
}

func (queryResult *Q1Result) NewEmptyBatch(len int) generic.Batch[*English2020Article] {
	return NewEmptyEnglish2020ArticleBatch(len)
}

func (queryResult *Q1Result) DeserializeArticleBatch(data []byte) (*generic.Batch[*English2020Article], error) {
	return English2020ArticleBatchFromBytes(data)
}

func (queryResult *Q1Result) BuildQueryResult(batch generic.Batch[*English2020Article]) *QueryResult {
	result := Q1Result{ResultArticles: batch}
	payload, _ := result.ToBytes()
	return &QueryResult{
		QueryCode:     Q1Code,
		ResultPayload: payload,
	}
}
