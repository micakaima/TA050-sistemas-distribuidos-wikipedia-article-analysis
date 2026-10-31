package querysresults

import (
	"bytes"
	"io"

	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type WritableResult interface {
	WriteToFile(filepath string) error
}

type QueryResult struct {
	QueryCode     QueryResultCode
	ResultPayload []byte
}

func (res *QueryResult) ToBytes() ([]byte, error) {
	result := []byte{byte(res.QueryCode)}
	result = append(result, utils.SerializeUint32(uint32(len(res.ResultPayload)))...)
	result = append(result, res.ResultPayload...)
	return result, nil
}

func QueryResultFromBytes(data []byte) (*QueryResult, error) {
	reader := bytes.NewReader(data)
	queryResult, err := QueryResultFromReader(reader)
	if err != nil {
		return nil, err
	}
	return &queryResult, nil
}

func QueryResultFromReader(reader io.Reader) (QueryResult, error) {
	queryCode, err := utils.ReadByte(reader)
	if err != nil {
		return QueryResult{}, err
	}
	payloadLen, err := utils.ReadUint32(reader)
	if err != nil {
		return QueryResult{}, err
	}
	payload, err := safeio.RecvAll(reader, int(payloadLen))

	return QueryResult{
		QueryCode:     QueryResultCode(queryCode),
		ResultPayload: payload,
	}, nil
}
