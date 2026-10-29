package messageprotocol

import (
	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/errors"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const UINT32_SIZE = 4
const UINT64_SIZE = 8
const INT64_SIZE = 8
const FLOAT64_SIZE = 8
const BOOL_SIZE = 1

type ValueType byte

const (
	TypeNil ValueType = iota
	TypeString
	TypeInt64
	TypeUint64
	TypeFloat64
	TypeBool
	TypeMap
	TypeList
)

func appendLenght(data []byte) []byte {
	length := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(length, uint32(len(data)))
	return append(length, data...)
}

func SerializeString(value string) []byte {
	data := []byte(value)
	return appendLenght(data)
}

func DeserializeString(data []byte) (string, error) {
	if len(data) < UINT32_SIZE {
		return "", fmt.Errorf(errors.ValueTooShortError)
	}
	offset := UINT32_SIZE
	length := binary.BigEndian.Uint32(data)
	if len(data) < offset+int(length) {
		return "", fmt.Errorf(errors.ValueTooShortError)
	}
	return string(data[offset : offset+int(length)]), nil
}

func SerializeUint32(value uint32) []byte {
	result := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(result, value)
	return result
}

func DeserializeUint32(data []byte) (uint32, error) {
	if len(data) < UINT32_SIZE {
		return 0, fmt.Errorf(errors.ValueTooShortError)
	}
	return binary.BigEndian.Uint32(data), nil
}

func SerializeInt64(value int64) []byte {
	result := make([]byte, INT64_SIZE)
	binary.BigEndian.PutUint64(result, uint64(value))
	return result
}

func DeserializeInt64(data []byte) (int64, error) {
	if len(data) < INT64_SIZE {
		return 0, fmt.Errorf(errors.ValueTooShortError)
	}
	valUint64 := binary.BigEndian.Uint64(data)
	return int64(valUint64), nil
}

func SerializeUint64(value uint64) []byte {
	result := make([]byte, UINT64_SIZE)
	binary.BigEndian.PutUint64(result, value)
	return result
}

func DeserializeUint64(data []byte) (uint64, error) {
	if len(data) < UINT64_SIZE {
		return 0, fmt.Errorf(errors.ValueTooShortError)
	}
	return binary.BigEndian.Uint64(data), nil
}

func SerializeFloat64(value float64) []byte {
	result := make([]byte, FLOAT64_SIZE)
	binary.BigEndian.PutUint64(result, math.Float64bits(value))
	return result
}

func DeserializeFloat64(data []byte) (float64, error) {
	if len(data) < FLOAT64_SIZE {
		return 0, fmt.Errorf(errors.ValueTooShortError)
	}
	bits := binary.BigEndian.Uint64(data)
	return math.Float64frombits(bits), nil
}

func SerializeBool(value bool) []byte {
	if value {
		return []byte{1}
	}
	return []byte{0}
}

func DeserializeBool(data []byte) (bool, error) {
	if len(data) < 1 {
		return false, fmt.Errorf(errors.ValueTooShortError)
	}
	return data[0] == 1, nil
}

// Serializa una fila / campo con subcampos crudo.
// La estructura de serialización es [Cant. Subcampos] + [Nombre Subcampo1] + [Bytes Subcampo1 serializado] + ...
func SerializeMap(m map[string]any) ([]byte, error) {
	result := SerializeUint32(uint32(len(m)))
	for k, v := range m {
		result = append(result, SerializeString(k)...)
		valBytes, err := serializeValue(v)
		if err != nil {
			return nil, err
		}
		result = append(result, valBytes...)
	}
	return result, nil
}

// Serializa un valor correspondiente a un campo / subcampo.
// Cada campo incluye primero el tipo de campo que es (string, bool, list, etc.), luego:
//   - A los strings se los serializa anteponiendo la longitud del string, y posteriormente la cadena.
//   - A los valores numéricos se los serializa con endianess Big-Endian.
//   - A los campos con subcampos se los serializa recursivamente.
//   - A los campos tipo lista, se los serializa anteponiendo la longitud de la lista, y posteriormente los ítems serialziados.
func serializeValue(val interface{}) ([]byte, error) {
	if val == nil {
		return []byte{byte(TypeNil)}, nil
	}

	switch v := val.(type) {
	case string:
		res := []byte{byte(TypeString)}
		return append(res, SerializeString(v)...), nil

	case int64:
		res := []byte{byte(TypeInt64)}
		return append(res, SerializeInt64(v)...), nil

	case uint64:
		res := []byte{byte(TypeUint64)}
		return append(res, SerializeUint64(v)...), nil

	case float64:
		res := []byte{byte(TypeFloat64)}
		return append(res, SerializeFloat64(v)...), nil

	case bool:
		b := byte(0)
		if v {
			b = 1
		}
		return []byte{byte(TypeBool), b}, nil

	case map[string]any:
		mapBytes, err := SerializeMap(v)
		if err != nil {
			return nil, err
		}
		res := []byte{byte(TypeMap)}
		return append(res, mapBytes...), nil

	case []any:
		res := []byte{byte(TypeList)}
		res = append(res, SerializeUint32(uint32(len(v)))...)

		for _, item := range v {
			itemBytes, err := serializeValue(item)
			if err != nil {
				return nil, err
			}
			res = append(res, itemBytes...)
		}
		return res, nil

	default:
		fmt.Println("UNKNOWN TYPE CODE: ", v)
		return nil, fmt.Errorf(errors.UnknownTypeCode)
	}
}

// Deserializa una fila / campo con subcampos crudo.
// Primero lee la cantidad de columnas / subcampos y luego, en base a esta cantidad,
// por cada subcampo lee su nombre, y deserializa su valor correspondiente.
func DeserializeMap(reader io.Reader) (map[string]any, error) {
	numColumns, err := ReadUint32(reader)
	if err != nil {
		return nil, err
	}

	m := make(map[string]interface{}, numColumns)
	for i := uint32(0); i < numColumns; i++ {
		key, err := ReadString(reader)
		if err != nil {
			return nil, err
		}
		val, err := deserializeValue(reader)
		if err != nil {
			return nil, err
		}
		m[key] = val
	}
	return m, nil
}

// Deserializa un valor correspondiente a un campo / subcampo.
// Primero lee el tipo de valor que es (string, bool, list, etc.), y luego deserializa el valor
// según el tipo, análogamente a cómo se serializó.
func deserializeValue(reader io.Reader) (any, error) {
	valTypeByte, err := ReadByte(reader)
	if err != nil {
		return nil, err
	}
	valType := ValueType(valTypeByte)

	switch valType {
	case TypeNil:
		return nil, nil

	case TypeString:
		return ReadString(reader)

	case TypeInt64:
		return ReadInt64(reader)

	case TypeUint64:
		return ReadUint64(reader)

	case TypeFloat64:
		return ReadFloat64(reader)

	case TypeBool:
		b, err := ReadByte(reader)
		if err != nil {
			return nil, err
		}
		return b == 1, nil

	case TypeMap:
		return DeserializeMap(reader)

	case TypeList:
		count, err := ReadUint32(reader)
		if err != nil {
			return nil, err
		}
		list := make([]interface{}, count)
		for i := uint32(0); i < count; i++ {
			item, err := deserializeValue(reader)
			if err != nil {
				return nil, err
			}
			list[i] = item
		}
		return list, nil

	default:
		return nil, fmt.Errorf(errors.UnknownTypeCode)
	}
}
