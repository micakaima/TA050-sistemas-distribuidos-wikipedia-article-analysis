package querysresults

import (
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

// Interfaz para representar los resultados de las querys que finalmente se graban al disco del cliente
// como parte de la respuesta.
type WritableResult interface {
	// Dada una ruta, escribe en ella el resultado total final de la query en cuestión.
	WriteToFile(filepath string) error
}

// Representa un resultado final de una query cualquiera. Incluye el código de la query (1-5),
// y el contenido del resultado en bytes.
type QueryResult struct {
	QueryCode     QueryResultCode
	ResultPayload []byte
}

func (res *QueryResult) ToBytes() ([]byte, error) {
	result := []byte{byte(res.QueryCode)}
	result = append(result, res.ResultPayload...)
	return result, nil
}

func QueryResultFromBytes(data []byte) (*QueryResult, error) {
	offset := 0
	queryCode, err := utils.ReadByteFromBytes(data, &offset)
	if err != nil {
		return nil, err
	}
	return &QueryResult{
		QueryCode:     QueryResultCode(queryCode),
		ResultPayload: data[offset:],
	}, nil
}
