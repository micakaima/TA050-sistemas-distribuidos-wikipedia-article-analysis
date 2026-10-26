package messageprotocol

import (
	"encoding/binary"
	"fmt"
)

const UINT32_SIZE = 4
const UINT64_SIZE = 8
const BOOL_SIZE = 1

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
		return "", fmt.Errorf(ValueTooShortError)
	}
	offset := UINT32_SIZE
	length := binary.BigEndian.Uint32(data)
	if len(data) < offset+int(length) {
		return "", fmt.Errorf(ValueTooShortError)
	}
	return string(data[offset : offset+int(length)]), nil
}

func SerializeUint32(value uint32) []byte {
	data := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(data, value)
	return data
}

func DeserializeUint32(data []byte) (uint32, error) {
	if len(data) < UINT32_SIZE {
		return 0, fmt.Errorf(ValueTooShortError)
	}
	return binary.BigEndian.Uint32(data), nil
}

func SerializeUint64(value int64) []byte {
	out := make([]byte, UINT64_SIZE)
	binary.BigEndian.PutUint64(out, uint64(value))
	return out
}

func DeserializeUint64(data []byte) (uint64, error) {
	if len(data) < UINT64_SIZE {
		return 0, fmt.Errorf(ValueTooShortError)
	}
	return binary.BigEndian.Uint64(data), nil
}

func SerializeBool(value bool) []byte {
	if value {
		return []byte{1}
	}
	return []byte{0}
}

func DeserializeBool(data []byte) (bool, error) {
	if len(data) < 1 {
		return false, fmt.Errorf(ValueTooShortError)
	}
	return data[0] == 1, nil
}
