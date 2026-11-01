package messageprotocol

import (
	"io"

	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
	article "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/raw_article"
)

// Envía un batch artículos / filas crudas.
// La estructura del envío es [Código del mensaje (batch)] + [Longitud bytes del artículo] + [Bytes del batch serializado].
func WriteArticlesBatch(writer io.Writer, rawArticlesBatch article.RawArticlesBatch) error {
	batchBytes, err := rawArticlesBatch.ToBytes()
	if err != nil {
		return err
	}
	length := len(batchBytes)

	result := []byte{byte(ArticlesBatch)}
	result = append(result, utils.SerializeUint32(uint32(length))...)
	result = append(result, batchBytes...)
	return safeio.SendAll(writer, result)
}

// Lee un batch de artículos / filas crudas.
// Lee [Longitud bytes del artículo] y luego, en base a éste, [Bytes del batch].
func ReadArticlesBatch(reader io.Reader) (article.RawArticlesBatch, error) {
	length, err := utils.ReadUint32(reader)
	if err != nil {
		return article.RawArticlesBatch{}, err
	}
	data, err := safeio.RecvAll(reader, int(length))
	if err != nil {
		return article.RawArticlesBatch{}, err
	}
	batch, err := generic.BatchFromBytes(data, article.RawArticleFromBytes)
	if err != nil {
		return article.RawArticlesBatch{}, err
	}
	return article.RawArticlesBatch(batch), nil
}
