package messageprotocol

import (
	"encoding/binary"
	"fmt"
	"io"

	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

func ReadString(reader io.Reader) (string, error) {
	length, err := ReadUint32(reader)
	if err != nil {
		return "", err
	}
	stringBytes, err := safeio.RecvAll(reader, int(length))
	if err != nil {
		return "", err
	}
	return string(stringBytes), nil
}

func ReadUint32(reader io.Reader) (uint32, error) {
	numBytes, err := safeio.RecvAll(reader, UINT32_SIZE)
	if err != nil {
		return 0, err
	}
	return DeserializeUint32(numBytes)
}

func ReadInt64(reader io.Reader) (int64, error) {
	numBytes, err := safeio.RecvAll(reader, INT64_SIZE)
	if err != nil {
		return 0, err
	}
	return DeserializeInt64(numBytes)
}

func ReadUint64(reader io.Reader) (uint64, error) {
	numBytes, err := safeio.RecvAll(reader, UINT64_SIZE)
	if err != nil {
		return 0, err
	}
	return DeserializeUint64(numBytes)
}

func ReadFloat64(reader io.Reader) (float64, error) {
	numBytes, err := safeio.RecvAll(reader, FLOAT64_SIZE)
	if err != nil {
		return 0, err
	}
	return DeserializeFloat64(numBytes)
}

func ReadByte(reader io.Reader) (byte, error) {
	b, err := safeio.RecvAll(reader, 1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func ReadStringFromBytes(data []byte, offset *int) (string, error) {
	length, err := ReadUint32FromBytes(data, offset)
	if err != nil {
		return "", err
	}
	if *offset+int(length) > len(data) {
		return "", fmt.Errorf(errors.ValueTooShortError)
	}
	s := string(data[*offset : *offset+int(length)])
	*offset += int(length)
	return s, nil
}

func ReadUint32FromBytes(data []byte, offset *int) (uint32, error) {
	if *offset+UINT32_SIZE > len(data) {
		return 0, fmt.Errorf(errors.ValueTooShortError)
	}
	num := binary.BigEndian.Uint32(data[*offset : *offset+UINT32_SIZE])
	*offset += UINT32_SIZE
	return num, nil
}

func ReadInt64FromBytes(data []byte, offset *int) (int64, error) {
	if *offset+INT64_SIZE > len(data) {
		return 0, fmt.Errorf(errors.ValueTooShortError)
	}
	num := int64(binary.BigEndian.Uint64(data[*offset : *offset+INT64_SIZE]))
	*offset += INT64_SIZE
	return num, nil
}

func ReadUint64FromBytes(data []byte, offset *int) (uint64, error) {
	if *offset+UINT64_SIZE > len(data) {
		return 0, fmt.Errorf(errors.ValueTooShortError)
	}
	num := binary.BigEndian.Uint64(data[*offset : *offset+UINT64_SIZE])
	*offset += UINT64_SIZE
	return num, nil
}

func ReadFloat64FromBytes(data []byte, offset *int) (float64, error) {
	if *offset+FLOAT64_SIZE > len(data) {
		return 0, fmt.Errorf(errors.ValueTooShortError)
	}
	bits := binary.BigEndian.Uint64(data[*offset : *offset+FLOAT64_SIZE])
	*offset += FLOAT64_SIZE
	return float64(bits), nil
}

func ReadByteFromBytes(data []byte, offset *int) (byte, error) {
	if *offset >= len(data) {
		return 0, fmt.Errorf(errors.ValueTooShortError)
	}
	b := data[*offset]
	*offset++
	return b, nil
}

func ReadImageBytesFromBytes(data []byte, offset *int) ([]byte, error) {
	length, err := ReadUint32FromBytes(data, offset)
	if err != nil {
		return []byte{}, err
	}
	if *offset+int(length) > len(data) {
		return []byte{}, fmt.Errorf(errors.ValueTooShortError)
	}
	imgBytes := data[*offset : *offset+int(length)]
	*offset += int(length)
	return imgBytes, nil
}

func GetKeyAsString(m map[string]any, key string) string {
	val, ok := m[key]
	if !ok || val == nil {
		return ""
	}
	return fmt.Sprintf("%v", val)
}

func GetKeyAsInt64(m map[string]any, key string) int64 {
	val, ok := m[key]
	if !ok || val == nil {
		return 0
	}
	switch v := val.(type) {
	case int64:
		return v
	case uint64:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func GetSubMap(m map[string]any, key string) map[string]interface{} {
	val, ok := m[key]
	if !ok || val == nil {
		return nil
	}
	subMap, ok := val.(map[string]interface{})
	if !ok {
		return nil
	}
	return subMap
}
