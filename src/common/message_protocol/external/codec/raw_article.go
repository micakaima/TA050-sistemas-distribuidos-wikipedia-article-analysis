package messageprotocol

import (
	"io"

	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	article "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/raw_article"
)

// Envía un artículo / fila cruda.
// La estructura del envío es [Código del mensaje (artículo)] + [Bytes del artículo serializado],
// donde [Bytes del artículo serializado] = [Longitud bytes del artículo] + [Bytes del artículo].
func WriteArticle(writer io.Writer, rawArticle article.RawArticle) error {
	serializedArticleBytes, err := rawArticle.ToBytes()
	if err != nil {
		return err
	}

	result := []byte{byte(Article)}
	result = append(result, serializedArticleBytes...)
	return safeio.SendAll(writer, result)
}

// Lee un artículo / fila cruda.
// Lee [Longitud bytes del artículo] y luego, en base a éste, [Bytes del artículo].
// Finalmente deserializa los bytes del artículo.
func ReadArticle(reader io.Reader) (article.RawArticle, error) {
	return article.RawArticleFromReader(reader)
}
