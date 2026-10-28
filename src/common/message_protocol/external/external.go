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

func SerializeArticle(rawArticle map[string]any) ([]byte, error) {
	bytes, err := utils.SerializeMap(rawArticle)
	if err != nil {
		return nil, err
	}
	length := len(bytes)

	result := utils.SerializeUint32(uint32(length))
	result = append(result, bytes...)

	return result, nil
}

func WriteArticle(writer io.Writer, rawArticle map[string]any) error {
	bytes, err := utils.SerializeMap(rawArticle)
	if err != nil {
		return err
	}
	length := len(bytes)

	result := []byte{byte(Article)}
	result = append(result, utils.SerializeUint32(uint32(length))...)
	result = append(result, bytes...)

	return safeio.SendAll(writer, result)
}

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

func ReadArticlesBatch(reader io.Reader) ([]map[string]interface{}, error) {
	numRecords, err := utils.ReadUint32(reader)
	if err != nil {
		return nil, err
	}

	batch := []map[string]interface{}{}
	for range numRecords {
		record, err := ReadArticle(reader)
		if err != nil {
			return nil, err
		}
		batch = append(batch, record)
	}
	return batch, nil
}

func ReadMsgType(reader io.Reader) (MsgType, error) {
	msgTypeByte, err := utils.ReadByte(reader)
	if err != nil {
		return Unknown, err
	}
	msgType := MsgType(msgTypeByte)
	return msgType, nil
}

func WriteEOF(writer io.Writer) error {
	result := []byte{byte(EOF)}
	return safeio.SendAll(writer, result)
}

func WriteAck(writer io.Writer) error {
	result := []byte{byte(Ack)}
	return safeio.SendAll(writer, result)
}
