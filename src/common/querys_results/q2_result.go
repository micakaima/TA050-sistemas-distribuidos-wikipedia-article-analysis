package querysresults

import (
	"fmt"
	"os"
	"path/filepath"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

// Representa el resultado final de la query 2. Implementa la interfaz WritableResult.
type Q2Result struct {
	ResultArticles generic.Batch[*ImageArticle]
}

// Representa un artículo escrito hasta el 2020 inclusive ...
type ImageArticle struct {
	ArticleID  string
	Name       string
	URL        string
	ImageBytes []byte
	Error      string
}

func (article *ImageArticle) ToRow() string {
	return fmt.Sprintf("%s,%s,%s,%s,%s\n", article.ArticleID, article.Name, article.URL, string(article.ImageBytes), article.Error)
}

func (article *ImageArticle) ToBytes() ([]byte, error) {
	result := utils.SerializeString(article.ArticleID)
	result = append(result, utils.SerializeString(article.Name)...)
	result = append(result, utils.SerializeString(article.URL)...)
	result = append(result, utils.SerializeImageBytes(article.ImageBytes)...)
	result = append(result, utils.SerializeString(article.Error)...)
	return result, nil
}

func ImageArticleFromBytes(data []byte, offset *int) (*ImageArticle, error) {
	articleID, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	name, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	url, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	imageBytes, err := utils.ReadImageBytesFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	errorMsg, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &ImageArticle{
		ArticleID:  articleID,
		Name:       name,
		URL:        url,
		ImageBytes: imageBytes,
		Error:      errorMsg,
	}, nil
}

func NewEmptyImageArticleBatch(len int) generic.Batch[*ImageArticle] {
	return generic.NewEmptyBatch[*ImageArticle](len)
}

func ImageArticleBatchFromBytes(data []byte) (*generic.Batch[*ImageArticle], error) {
	batch, err := generic.BatchFromBytes(data, ImageArticleFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}

func (queryResult *Q2Result) ToBytes() ([]byte, error) {
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

func Q2ResultFromBytes(data []byte) (*Q2Result, error) {
	offset := 0
	numArticles, err := utils.ReadUint32FromBytes(data, &offset)
	if err != nil {
		return nil, err
	}
	articles := NewEmptyImageArticleBatch(int(numArticles))
	for range numArticles {
		article, err := ImageArticleFromBytes(data, &offset)
		if err != nil {
			return nil, err
		}
		articles.Add(article)
	}
	q2Result := Q2Result{ResultArticles: articles}
	return &q2Result, nil
}

func (queryResult *Q2Result) WriteToFile(path string) error {
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
