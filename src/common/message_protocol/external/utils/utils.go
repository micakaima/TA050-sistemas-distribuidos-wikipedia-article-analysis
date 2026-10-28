package messageprotocol

import (
	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	"io"
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
