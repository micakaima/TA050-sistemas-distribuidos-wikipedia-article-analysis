package messageprotocol

type SerializableMessage interface {
	ToBytes() ([]byte, error)
}
