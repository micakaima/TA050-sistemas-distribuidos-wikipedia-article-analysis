package messageprotocol

import (
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

// SharedWordsPartial es el parcial que cada shard envía al joiner con las
// palabras confirmadas en ambos idiomas (en y fr) dentro de su partición.
type SharedWordsPartial struct {
	Words map[string]struct{}
}

func (p *SharedWordsPartial) ToBytes() ([]byte, error) {
	return utils.SerializeSet(p.Words), nil
}

func SharedWordsPartialFromBytes(data []byte) (*SharedWordsPartial, error) {
	offset := 0
	words, err := utils.DeserializeSet(data, &offset)
	if err != nil {
		return nil, err
	}
	return &SharedWordsPartial{Words: words}, nil
}
