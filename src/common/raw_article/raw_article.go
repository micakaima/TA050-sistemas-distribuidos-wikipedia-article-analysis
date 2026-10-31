package messageprotocol

import (
	"bytes"
	"io"

	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
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
	rawArticle, err := RawArticleFromReader(reader)
	if err != nil {
		return nil, err
	}
	return &rawArticle, nil
}

// Devuelve un artículo / fila cruda a partir de un reader.
// Lee [Longitud bytes del artículo] y luego, en base a éste, [Bytes del artículo].
// Finalmente deserializa los bytes del artículo.
func RawArticleFromReader(reader io.Reader) (RawArticle, error) {
	_, err := utils.ReadUint32(reader)
	if err != nil {
		return nil, err
	}
	return utils.DeserializeMap(reader)
}
