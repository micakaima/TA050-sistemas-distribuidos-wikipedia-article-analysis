package messageprotocol

import (
	"io"

	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
	article "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/raw_article"
)

// Envía un artículo / fila cruda.
// La estructura del envío es [Código del mensaje (artículo)] + [Longitud bytes del artículo] + [Bytes del artículo serializado].
func WriteArticle(writer io.Writer, rawArticle article.RawArticle) error {
	serializedArticleBytes, err := rawArticle.ToBytes()
	if err != nil {
		return err
	}
	length := len(serializedArticleBytes)

	result := []byte{byte(Article)}
	result = append(result, utils.SerializeUint32(uint32(length))...)
	result = append(result, serializedArticleBytes...)
	return safeio.SendAll(writer, result)
}

// Lee un artículo / fila cruda.
// Lee [Longitud bytes del artículo] y luego, en base a éste, [Bytes del artículo].
func ReadArticle(reader io.Reader) (article.RawArticle, error) {
	length, err := utils.ReadUint32(reader)
	if err != nil {
		return nil, err
	}
	data, err := safeio.RecvAll(reader, int(length))
	if err != nil {
		return nil, err
	}
	offset := 0
	return utils.DeserializeMapFromBytes(data, &offset)
}
