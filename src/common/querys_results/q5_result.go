package querysresults

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type Q5Result struct {
	ResultArticles []AboveAverageArticle
}

type AboveAverageArticle struct {
	ArticleID string
	URL       string
	WordCount uint32
}

func (article *AboveAverageArticle) ToRow() string {
	return fmt.Sprintf("%s,%s,%d", article.ArticleID, article.URL, article.WordCount)
}

func (article *AboveAverageArticle) ToBytes() ([]byte, error) {
	result := utils.SerializeString(article.ArticleID)
	result = append(result, utils.SerializeString(article.URL)...)
	result = append(result, utils.SerializeUint32(article.WordCount)...)
	return result, nil
}

func AboveAverageArticleFromBytes(data []byte) (*AboveAverageArticle, error) {
	reader := bytes.NewReader(data)
	article, err := AboveAverageArticleFromReader(reader)
	if err != nil {
		return nil, err
	}
	return &article, nil
}

func AboveAverageArticleFromReader(reader io.Reader) (AboveAverageArticle, error) {
	articleID, err := utils.ReadString(reader)
	if err != nil {
		return AboveAverageArticle{}, err
	}
	URL, err := utils.ReadString(reader)
	if err != nil {
		return AboveAverageArticle{}, err
	}
	wordCount, err := utils.ReadUint32(reader)
	if err != nil {
		return AboveAverageArticle{}, err
	}
	return AboveAverageArticle{
		ArticleID: articleID,
		URL:       URL,
		WordCount: wordCount,
	}, nil
}

func (queryResult *Q5Result) ToBytes() ([]byte, error) {
	result := utils.SerializeUint32(uint32(len(queryResult.ResultArticles)))
	for _, article := range queryResult.ResultArticles {
		articleBytes, err := article.ToBytes()
		if err != nil {
			return nil, err
		}
		result = append(result, articleBytes...)
	}
	return result, nil
}

func Q5ResultFromBytes(data []byte) (*Q5Result, error) {
	reader := bytes.NewReader(data)

	numArticles, err := utils.ReadUint32(reader)
	if err != nil {
		return nil, err
	}

	resultArticles := []AboveAverageArticle{}
	for range numArticles {
		article, err := AboveAverageArticleFromReader(reader)
		if err != nil {
			return nil, err
		}
		resultArticles = append(resultArticles, article)
	}
	return &Q5Result{ResultArticles: resultArticles}, nil
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
	for _, article := range queryResult.ResultArticles {
		if _, err := file.WriteString(article.ToRow()); err != nil {
			return err
		}
	}
	return nil
}
