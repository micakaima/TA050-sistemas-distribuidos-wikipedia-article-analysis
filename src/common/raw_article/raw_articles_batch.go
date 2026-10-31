package messageprotocol

import (
	"bytes"
	"io"

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
	reader := bytes.NewReader(data)
	rawArticlesBatch, err := RawArticlesBatchFromReader(reader)
	if err != nil {
		return nil, err
	}
	return &rawArticlesBatch, nil
}

// Devuelve un batch de artículos / filas crudas a partir de un reader.
// Lee [Largo del batch] y luego, en base a éste, lee los bytes de todos los artículos que hacen al batch.
func RawArticlesBatchFromReader(reader io.Reader) (RawArticlesBatch, error) {
	numArticles, err := utils.ReadUint32(reader)
	if err != nil {
		return nil, err
	}

	batch := []RawArticle{}
	for range numArticles {
		record, err := RawArticleFromReader(reader)
		if err != nil {
			return nil, err
		}
		batch = append(batch, record)
	}
	return batch, nil
}
