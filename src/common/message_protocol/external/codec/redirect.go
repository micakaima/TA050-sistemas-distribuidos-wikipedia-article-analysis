package messageprotocol

import (
	"io"

	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

// Envía un mensaje de redirección.
// La estructura del envío es [Código del mensaje (redirección)] + [ServerHost] + [ServerPort].
func WriteRedirect(writer io.Writer, serverHost string, serverPort int) error {
	result := []byte{byte(Redirect)}
	result = append(result, utils.SerializeString(serverHost)...)
	result = append(result, utils.SerializeUint32(uint32(serverPort))...)
	return safeio.SendAll(writer, result)
}

// Lee un mensaje de redirección.
func ReadRedirect(reader io.Reader) (string, int, error) {
	serverHost, err := utils.ReadString(reader)
	if err != nil {
		return "", 0, err
	}
	serverPort, err := utils.ReadUint32(reader)
	if err != nil {
		return "", 0, err
	}
	return serverHost, int(serverPort), nil
}
