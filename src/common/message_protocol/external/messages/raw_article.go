package messageprotocol

import (
	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
	"bytes"
	"io"
)

type RawArticle map[string]interface{}

// Serializa un artículo / fila cruda.
// La estructura de la serialización es [Longitud bytes del artículo] + [Bytes del artículo].
func (article *RawArticle) ToBytes() ([]byte, error) {
	articleBytes, err := utils.SerializeMap(*article)
	if err != nil {
		return nil, err
	}
	length := len(articleBytes)

	result := utils.SerializeUint32(uint32(length))
	result = append(result, articleBytes...)
	return result, nil
}

// Devuelve un artículo / fila cruda a partir de una serie de bytes.
func RawArticleFromBytes(data []byte) (*RawArticle, error) {
	reader := bytes.NewReader(data)
	rawArticle, err := ReadArticle(reader)
	if err != nil {
		return nil, err
	}
	return &rawArticle, nil
}

// Envía un artículo / fila cruda.
// La estructura del envío es [Código del mensaje (artículo)] + [Bytes del artículo serializado],
// donde [Bytes del artículo serializado] = [Longitud bytes del artículo] + [Bytes del artículo].
func WriteArticle(writer io.Writer, rawArticle RawArticle) error {
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
func ReadArticle(reader io.Reader) (RawArticle, error) {
	length, err := utils.ReadUint32(reader)
	if err != nil {
		return nil, err
	}
	data, err := safeio.RecvAll(reader, int(length))
	if err != nil {
		return nil, err
	}
	return utils.DeserializeMap(bytes.NewReader(data))
}
