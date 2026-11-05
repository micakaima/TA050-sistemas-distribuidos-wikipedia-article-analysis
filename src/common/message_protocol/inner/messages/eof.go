package messageprotocol

type EofMsg struct{}

func (eof *EofMsg) ToBytes() ([]byte, error) {
	return []byte{}, nil
}
