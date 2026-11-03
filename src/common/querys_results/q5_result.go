package querysresults

import (
	"fmt"
	"os"
	"path/filepath"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

// Representa el resultado final de la query 5. Implementa la interfaz WritableResult.
type Q5Result struct {
	ResultArticles generic.Batch[*AboveAverageArticle]
}

// Representa un artículo escrito a partir del 2021 cuya cant. de palabras en el abstract es
// mayor al promedio de palabras de abstract de los artículos escritos hasta el 2020 inclusive.
type AboveAverageArticle struct {
	ArticleID string
	URL       string
	WordCount uint32
}

func (article *AboveAverageArticle) ToRow() string {
	return fmt.Sprintf("%s,%s,%d\n", article.ArticleID, article.URL, article.WordCount)
}

func (article *AboveAverageArticle) ToBytes() ([]byte, error) {
	result := utils.SerializeString(article.ArticleID)
	result = append(result, utils.SerializeString(article.URL)...)
	result = append(result, utils.SerializeUint32(article.WordCount)...)
	return result, nil
}

func AboveAverageArticleFromBytes(data []byte, offset *int) (*AboveAverageArticle, error) {
	articleID, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	url, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	wordCount, err := utils.ReadUint32FromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &AboveAverageArticle{
		ArticleID: articleID,
		URL:       url,
		WordCount: wordCount,
	}, nil
}

func NewEmptyAboveAverageArticleBatch(len int) generic.Batch[*AboveAverageArticle] {
	return generic.NewEmptyBatch[*AboveAverageArticle](len)
}

func AboveAverageArticleBatchFromBytes(data []byte) (*generic.Batch[*AboveAverageArticle], error) {
	batch, err := generic.BatchFromBytes(data, AboveAverageArticleFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}

func (queryResult *Q5Result) ToBytes() ([]byte, error) {
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

func Q5ResultFromBytes(data []byte) (*Q5Result, error) {
	offset := 0
	numArticles, err := utils.ReadUint32FromBytes(data, &offset)
	if err != nil {
		return nil, err
	}

	articles := NewEmptyAboveAverageArticleBatch(int(numArticles))
	for range numArticles {
		article, err := AboveAverageArticleFromBytes(data, &offset)
		if err != nil {
			return nil, err
		}
		articles.Add(article)
	}
	q5Result := Q5Result{ResultArticles: articles}
	return &q5Result, nil
}

func (queryResult *Q5Result) WriteToFile(path string) error {
	dir := filepath.Dir(path)
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
