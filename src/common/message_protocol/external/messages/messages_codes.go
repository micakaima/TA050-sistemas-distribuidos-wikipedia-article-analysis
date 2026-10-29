package messageprotocol

import (
	"io"

	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type MsgCode byte

const (
	Article MsgCode = iota
	ArticlesBatch
	EOF
	Ack
	Unknown
)

// Lee el código del tipo de mensaje recibido.
func ReadMsgCode(reader io.Reader) (MsgCode, error) {
	msgCodeByte, err := utils.ReadByte(reader)
	if err != nil {
		return Unknown, err
	}
	msgType := MsgCode(msgCodeByte)
	return msgType, nil
}
