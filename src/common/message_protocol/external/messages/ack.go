package messageprotocol

import (
	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	"io"
)

// Envía un mensaje Ack.
func WriteAck(writer io.Writer) error {
	result := []byte{byte(Ack)}
	return safeio.SendAll(writer, result)
}
