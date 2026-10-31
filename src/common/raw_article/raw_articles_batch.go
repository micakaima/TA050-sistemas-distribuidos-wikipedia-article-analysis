package messageprotocol

import (
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type RawArticlesBatch []RawArticle

// Serializa un batch de artículos / filas crudas.
// La estructura de la serialización es [Largo del batch] + [Bytes de los artículos serializados].
func (batch *RawArticlesBatch) ToBytes() ([]byte, error) {
	result := utils.SerializeUint32(uint32(len(*batch)))
	for _, article := range *batch {
		articleBytes, err := article.ToBytes()
		if err != nil {
			return nil, err
		}
		result = append(result, articleBytes...)
	}
	return result, nil
}

// Devuelve un batch de artículos / filas crudas a partir de una serie de bytes.
func RawArticlesBatchFromBytes(data []byte) (*RawArticlesBatch, error) {
	offset := 0
	numArticles, err := utils.ReadUint32FromBytes(data, &offset)
	if err != nil {
		return nil, err
	}

	articles := []RawArticle{}
	for range numArticles {
		article, err := RawArticleFromBytes(data, &offset)
		if err != nil {
			return nil, err
		}
		articles = append(articles, *article)
	}
	batch := RawArticlesBatch(articles)
	return &batch, nil
}
