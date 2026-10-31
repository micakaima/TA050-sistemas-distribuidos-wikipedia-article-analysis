package messageprotocol

import (
	"io"

	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
)

func WriteQueryResult(writer io.Writer, queryResult qresults.QueryResult) error {
	resultContent, err := queryResult.ToBytes()
	if err != nil {
		return err
	}

	result := []byte{byte(QueryResult)}
	result = append(result, resultContent...)
	return safeio.SendAll(writer, result)
}

func ReadQueryResultCode(reader io.Reader) (qresults.QueryResultCode, error) {
	resultCodeByte, err := utils.ReadByte(reader)
	if err != nil {
		return qresults.UnknownCode, err
	}
	resultCode := qresults.QueryResultCode(resultCodeByte)
	return resultCode, nil
}

func ReadQueryResult(reader io.Reader) (qresults.QueryResult, error) {
	return qresults.QueryResultFromReader(reader)
}
