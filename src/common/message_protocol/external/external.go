package messageprotocol

import (
	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/utils"
	"bytes"
	"io"
)

type MsgType byte

const (
	Article MsgType = iota
	ArticlesBatch
	EOF
	Ack
	Unknown
)

// Serializa un artículo / fila cruda.
// La estructura de la serialización es [Longitud bytes del artículo] + [Bytes del artículo].
func SerializeArticle(rawArticle map[string]any) ([]byte, error) {
	articleBytes, err := utils.SerializeMap(rawArticle)
	if err != nil {
		return nil, err
	}
	length := len(articleBytes)

	result := utils.SerializeUint32(uint32(length))
	result = append(result, articleBytes...)
	return result, nil
}

// Envía un artículo / fila cruda.
// La estructura del envío es [Código del mensaje (artículo)] + [Bytes del artículo serializado],
// donde [Bytes del artículo serializado] = [Longitud bytes del artículo] + [Bytes del artículo].
func WriteArticle(writer io.Writer, rawArticle map[string]any) error {
	serializedArticleBytes, err := SerializeArticle(rawArticle)
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
func ReadArticle(reader io.Reader) (map[string]any, error) {
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

// Lee el código del tipo de mensaje recibido.
func ReadMsgType(reader io.Reader) (MsgType, error) {
	msgTypeByte, err := utils.ReadByte(reader)
	if err != nil {
		return Unknown, err
	}
	msgType := MsgType(msgTypeByte)
	return msgType, nil
}

// Envía un mensaje EOF.
func WriteEOF(writer io.Writer) error {
	result := []byte{byte(EOF)}
	return safeio.SendAll(writer, result)
}

// Envía un mensaje Ack.
func WriteAck(writer io.Writer) error {
	result := []byte{byte(Ack)}
	return safeio.SendAll(writer, result)
}
