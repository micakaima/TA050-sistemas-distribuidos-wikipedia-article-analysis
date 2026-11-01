package messageprotocol

import (
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

// Representa un batch de cualquier tipo de mensaje serializable.
type Batch[T SerializableMessage] struct {
	Items []T
}

// Crea un batch con ítems ya definidos.
func NewBatch[T SerializableMessage](items []T) Batch[T] {
	return Batch[T]{Items: items}
}

// Crea un batch vacío pero con capacidad conocida reservada de antemano.
func NewEmptyBatch[T SerializableMessage](len int) Batch[T] {
	return Batch[T]{Items: make([]T, 0, len)}
}

func (b *Batch[T]) Add(item T) {
	b.Items = append(b.Items, item)
}

func (b *Batch[T]) Len() int {
	return len(b.Items)
}

// Serializa un batch de cualquier tipo de mensaje serializable.
// La estructura de serialización es [Cant. de elems del batch] + [Bytes de cada ítem en particular].
func (b *Batch[T]) ToBytes() ([]byte, error) {
	result := utils.SerializeUint32(uint32(len(b.Items)))
	for _, msg := range b.Items {
		msgBytes, err := msg.ToBytes()
		if err != nil {
			return nil, err
		}
		result = append(result, msgBytes...)
	}
	return result, nil
}

// Deserializa un batch de cualquier tipo de mensaje serializable.
// Recibe una función que especifica cómo deserializar el tipo de dato que conforma los ítems del batch
// (Ej. si es un batch de un MsgX, se debe incluír una función que deserialize un MsgX).
func BatchFromBytes[T SerializableMessage](data []byte, deserializeItemFn func([]byte, *int) (T, error)) (Batch[T], error) {
	offset := 0
	numItems, err := utils.ReadUint32FromBytes(data, &offset)
	if err != nil {
		return Batch[T]{}, err
	}
	items := make([]T, 0, numItems)
	for range numItems {
		item, err := deserializeItemFn(data, &offset)
		if err != nil {
			return Batch[T]{}, err
		}
		items = append(items, item)
	}
	return Batch[T]{Items: items}, nil
}
