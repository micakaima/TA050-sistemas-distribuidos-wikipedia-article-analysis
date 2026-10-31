package messageprotocol

import (
	"io"

	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
)

// Envía el resultado de una query.
// La estructura del envío es [Código del mensaje (query result)] + [Largo del resultado] + [Bytes del resultado serializado].
func WriteQueryResult(writer io.Writer, queryResult qresults.QueryResult) error {
	resultContent, err := queryResult.ToBytes()
	if err != nil {
		return err
	}
	length := len(resultContent)

	result := []byte{byte(QueryResult)}
	result = append(result, utils.SerializeUint32(uint32(length))...)
	result = append(result, resultContent...)
	return safeio.SendAll(writer, result)
}

// Lee el resultado de una query.
// Lee [Largo del resultado] y luego, en base a éste, [Bytes del resultado].
func ReadQueryResult(reader io.Reader) (qresults.QueryResult, error) {
	length, err := utils.ReadUint32(reader)
	if err != nil {
		return qresults.QueryResult{}, err
	}
	data, err := safeio.RecvAll(reader, int(length))
	if err != nil {
		return qresults.QueryResult{}, err
	}
	queryResult, err := qresults.QueryResultFromBytes(data)
	if err != nil {
		return qresults.QueryResult{}, err
	}
	return *queryResult, nil
}
