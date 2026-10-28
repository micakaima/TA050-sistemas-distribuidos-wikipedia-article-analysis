package messageprotocol

import (
	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/utils"
	"io"
)

// Envía un batch artículos / filas crudas.
// La estructura del envío es [Código del mensaje (batch)] + [Largo del batch] + [Bytes de los artículos serializados].
func WriteArticlesBatch(writer io.Writer, rawArticles []map[string]any) error {
	result := []byte{byte(ArticlesBatch)}
	result = append(result, utils.SerializeUint32(uint32(len(rawArticles)))...)

	for _, article := range rawArticles {
		articleBytes, err := SerializeArticle(article)
		if err != nil {
			return err
		}
		result = append(result, articleBytes...)
	}
	return safeio.SendAll(writer, result)
}

// Lee un batch de artículos / filas crudas.
// Lee [Largo del batch] y luego, en base a éste, lee los bytes de todos los artículos que hacen al batch.
func ReadArticlesBatch(reader io.Reader) ([]map[string]interface{}, error) {
	numArticles, err := utils.ReadUint32(reader)
	if err != nil {
		return nil, err
	}

	batch := []map[string]interface{}{}
	for range numArticles {
		record, err := ReadArticle(reader)
		if err != nil {
			return nil, err
		}
		batch = append(batch, record)
	}
	return batch, nil
}
