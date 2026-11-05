package messageprotocol

import (
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type OldAverage struct {
	Average float64
}

func (r *OldAverage) ToBytes() ([]byte, error) {
	result := utils.SerializeFloat64(r.Average)
	return result, nil
}

func OldAverageFromBytes(data []byte) (*OldAverage, error) {
	average, err := utils.DeserializeFloat64(data)
	if err != nil {
		return nil, err
	}
	return &OldAverage{Average: average}, nil
}
