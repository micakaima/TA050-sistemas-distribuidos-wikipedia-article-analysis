package messageprotocol

import (
	"bytes"
	"io"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/record"

	"github.com/parquet-go/parquet-go"
)

type MsgType uint32

const (
	Article MsgType = iota
	EOF
	Ack
	Unknown
)

func serializeRowsToBinary(schema *parquet.Schema, rows []parquet.Row) ([]byte, error) {
	var binaryOutput bytes.Buffer

	writer := parquet.NewWriter(&binaryOutput, schema)
	if _, err := writer.WriteRows(rows); err != nil {
		writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return binaryOutput.Bytes(), nil
}

func WriteRawArticles(writer io.Writer, schema *parquet.Schema, row []parquet.Row) error {
	rowBytes, err := serializeRowsToBinary(schema, row)
	if err != nil {
		return err
	}
	length := len(rowBytes)

	result := SerializeUint32(uint32(Article))
	result = append(result, SerializeUint32(uint32(length))...)
	result = append(result, rowBytes...)
	return SendAll(writer, result)
}

func ReadRawArticle(reader io.Reader) ([]record.Record, error) {
	serializedLength, err := RecvAll(reader, UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	length, err := DeserializeUint32(serializedLength)
	if err != nil {
		return nil, err
	}

	rowBytes, err := RecvAll(reader, int(length))
	if err != nil {
		return nil, err
	}
	records, err := record.ParseParquetBatchToRecords(rowBytes)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func ReadMsgType(reader io.Reader) (MsgType, error) {
	msgTypeSerialized, err := RecvAll(reader, UINT32_SIZE)
	if err != nil {
		return Unknown, err
	}
	msgTypeInt, err := DeserializeUint32(msgTypeSerialized)
	if err != nil {
		return Unknown, err
	}
	msgType := MsgType(msgTypeInt)
	return msgType, nil
}

func WriteEOF(writer io.Writer) error {
	result := SerializeUint32(uint32(EOF))
	return SendAll(writer, result)
}

func WriteAck(writer io.Writer) error {
	result := SerializeUint32(uint32(Ack))
	return SendAll(writer, result)
}
