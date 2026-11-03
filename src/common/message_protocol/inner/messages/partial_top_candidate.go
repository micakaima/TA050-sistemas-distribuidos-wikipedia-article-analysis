package messageprotocol

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type PartialTopCandidate struct {
	ArticleID string
	Word      string
}

func (c *PartialTopCandidate) ToBytes() ([]byte, error) {
	result := utils.SerializeString(c.ArticleID)
	result = append(result, utils.SerializeString(c.Word)...)
	return result, nil
}

func PartialTopCandidateFromBytes(data []byte, offset *int) (*PartialTopCandidate, error) {
	articleID, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	word, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &PartialTopCandidate{
		ArticleID: articleID,
		Word:      word,
	}, nil
}

func NewEmptyPartialTopCandidateBatch(len int) generic.Batch[*PartialTopCandidate] {
	return generic.NewEmptyBatch[*PartialTopCandidate](len)
}

func PartialTopCandidateBatchFromBytes(data []byte) (*generic.Batch[*PartialTopCandidate], error) {
	batch, err := generic.BatchFromBytes(data, PartialTopCandidateFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}
