package messageprotocol

import (
	"io"

	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type ExternalMsgCode byte

const (
	Article ExternalMsgCode = iota
	ArticlesBatch
	EOF
	Ack
	QueryResult
	Unknown
)

// Lee el código del tipo de mensaje recibido.
func ReadMsgCode(reader io.Reader) (ExternalMsgCode, error) {
	msgCodeByte, err := utils.ReadByte(reader)
	if err != nil {
		return Unknown, err
	}
	msgType := ExternalMsgCode(msgCodeByte)
	return msgType, nil
}
