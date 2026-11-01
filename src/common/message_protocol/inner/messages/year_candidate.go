package messageprotocol

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type YearCandidate struct {
	ArticleID    string
	DateModified int64
	ImageURL     string
	InLanguage   string
	Name         string
	Sections     string
	URL          string
}

func (y *YearCandidate) ToBytes() ([]byte, error) {
	result := utils.SerializeString(y.ArticleID)
	result = append(result, utils.SerializeInt64(y.DateModified)...)
	result = append(result, utils.SerializeString(y.ImageURL)...)
	result = append(result, utils.SerializeString(y.InLanguage)...)
	result = append(result, utils.SerializeString(y.Name)...)
	result = append(result, utils.SerializeString(y.Sections)...)
	result = append(result, utils.SerializeString(y.URL)...)
	return result, nil
}

func YearCandidateFromBytes(data []byte, offset *int) (*YearCandidate, error) {
	articleID, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	dateModified, err := utils.ReadInt64FromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	imageUrl, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	inLanguage, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	name, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	sections, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	url, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &YearCandidate{
		ArticleID:    articleID,
		DateModified: dateModified,
		ImageURL:     imageUrl,
		InLanguage:   inLanguage,
		Name:         name,
		Sections:     sections,
		URL:          url,
	}, nil
}

func NewEmptyYearCandidateBatch(len int) generic.Batch[*YearCandidate] {
	return generic.NewEmptyBatch[*YearCandidate](len)
}

func YearCandidateBatchFromBytes(data []byte) (*generic.Batch[*YearCandidate], error) {
	batch, err := generic.BatchFromBytes(data, YearCandidateFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}
