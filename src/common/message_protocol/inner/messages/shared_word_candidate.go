package messageprotocol

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type SharedWordCandidate struct {
	ArticleID  string
	InLanguage string
	Word       string
}

func (c *SharedWordCandidate) ToBytes() ([]byte, error) {
	result := utils.SerializeString(c.ArticleID)
	result = append(result, utils.SerializeString(c.InLanguage)...)
	result = append(result, utils.SerializeString(c.Word)...)
	return result, nil
}

func SharedWordCandidateFromBytes(data []byte, offset *int) (*SharedWordCandidate, error) {
	articleID, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	inLanguage, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	word, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &SharedWordCandidate{
		ArticleID:  articleID,
		InLanguage: inLanguage,
		Word:       word,
	}, nil
}

func NewEmptySharedWordCandidateBatch(len int) generic.Batch[*SharedWordCandidate] {
	return generic.NewEmptyBatch[*SharedWordCandidate](len)
}

func SharedWordCandidateBatchFromBytes(data []byte) (*generic.Batch[*SharedWordCandidate], error) {
	batch, err := generic.BatchFromBytes(data, SharedWordCandidateFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}
