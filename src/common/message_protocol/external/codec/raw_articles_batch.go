package messageprotocol

import (
	"io"

	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	article "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/raw_article"
)

// Envía un batch artículos / filas crudas.
// La estructura del envío es [Código del mensaje (batch)] + [Bytes del batch serializado],
// donde [Bytes del batch serializado] = [Largo del batch] + [Bytes de los artículos serializados].
func WriteArticlesBatch(writer io.Writer, rawArticlesBatch article.RawArticlesBatch) error {
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
func ReadArticlesBatch(reader io.Reader) (article.RawArticlesBatch, error) {
	return article.RawArticlesBatchFromReader(reader)
}
