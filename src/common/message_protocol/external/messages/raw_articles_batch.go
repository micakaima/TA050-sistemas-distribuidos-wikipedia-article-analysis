package messageprotocol

import (
	"bytes"
	"io"

	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
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
	rawArticlesBatch, err := ReadArticlesBatch(reader)
	if err != nil {
		return nil, err
	}
	return &rawArticlesBatch, nil
}

// Envía un batch artículos / filas crudas.
// La estructura del envío es [Código del mensaje (batch)] + [Bytes del batch serializado],
// donde [Bytes del batch serializado] = [Largo del batch] + [Bytes de los artículos serializados].
func WriteArticlesBatch(writer io.Writer, rawArticlesBatch RawArticlesBatch) error {
	batchBytes, err := rawArticlesBatch.ToBytes()
	if err != nil {
		return err
	}

	result := []byte{byte(ArticlesBatch)}
	result = append(result, batchBytes...)
	return safeio.SendAll(writer, result)
}

// Lee un batch de artículos / filas crudas.
// Lee [Largo del batch] y luego, en base a éste, lee los bytes de todos los artículos que hacen al batch.
func ReadArticlesBatch(reader io.Reader) (RawArticlesBatch, error) {
	numArticles, err := utils.ReadUint32(reader)
	if err != nil {
		return nil, err
	}

	batch := []RawArticle{}
	for range numArticles {
		record, err := ReadArticle(reader)
		if err != nil {
			return nil, err
		}
		batch = append(batch, record)
	}
	return batch, nil
}
