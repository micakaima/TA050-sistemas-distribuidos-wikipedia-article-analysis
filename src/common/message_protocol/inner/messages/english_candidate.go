package messageprotocol

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type EnglishCandidate struct {
	ArticleID    string
	DateModified int64
	InLanguage   string
	URL          string
}

func (candidate *EnglishCandidate) ToBytes() ([]byte, error) {
	result := utils.SerializeString(candidate.ArticleID)
	result = append(result, utils.SerializeInt64(candidate.DateModified)...)
	result = append(result, utils.SerializeString(candidate.InLanguage)...)
	result = append(result, utils.SerializeString(candidate.URL)...)
	return result, nil
}

func EnglishCandidateFromBytes(data []byte, offset *int) (*EnglishCandidate, error) {
	articleID, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	dateModified, err := utils.ReadInt64FromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	inLanguage, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	url, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &EnglishCandidate{ArticleID: articleID, DateModified: dateModified, InLanguage: inLanguage, URL: url}, nil
}

func NewEmptyEnglishCandidateBatch(length int) generic.Batch[*EnglishCandidate] {
	return generic.NewEmptyBatch[*EnglishCandidate](length)
}

func EnglishCandidateBatchFromBytes(data []byte) (*generic.Batch[*EnglishCandidate], error) {
	batch, err := generic.BatchFromBytes(data, EnglishCandidateFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}
