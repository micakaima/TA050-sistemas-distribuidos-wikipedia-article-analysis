package messageprotocol

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type BioCandidate struct {
	ArticleID string
	ImageURL  string
	Name      string
	Sections  string
	URL       string
}

func (candidate *BioCandidate) ToBytes() ([]byte, error) {
	result := utils.SerializeString(candidate.ArticleID)
	result = append(result, utils.SerializeString(candidate.ImageURL)...)
	result = append(result, utils.SerializeString(candidate.Name)...)
	result = append(result, utils.SerializeString(candidate.Sections)...)
	result = append(result, utils.SerializeString(candidate.URL)...)
	return result, nil
}

func BioCandidateFromBytes(data []byte, offset *int) (*BioCandidate, error) {
	articleID, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	imageURL, err := utils.ReadStringFromBytes(data, offset)
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
	return &BioCandidate{ArticleID: articleID, ImageURL: imageURL, Name: name, Sections: sections, URL: url}, nil
}

func NewEmptyBioCandidateBatch(length int) generic.Batch[*BioCandidate] {
	return generic.NewEmptyBatch[*BioCandidate](length)
}

func BioCandidateBatchFromBytes(data []byte) (*generic.Batch[*BioCandidate], error) {
	batch, err := generic.BatchFromBytes(data, BioCandidateFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}
