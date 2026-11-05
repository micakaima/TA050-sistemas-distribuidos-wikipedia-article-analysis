package messageprotocol

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type ImageCandidate struct {
	ArticleID string
	ImageURL  string
	Name      string
	URL       string
}

func (candidate *ImageCandidate) ToBytes() ([]byte, error) {
	result := utils.SerializeString(candidate.ArticleID)
	result = append(result, utils.SerializeString(candidate.ImageURL)...)
	result = append(result, utils.SerializeString(candidate.Name)...)
	result = append(result, utils.SerializeString(candidate.URL)...)
	return result, nil
}

func ImageCandidateFromBytes(data []byte, offset *int) (*ImageCandidate, error) {
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
	url, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &ImageCandidate{ArticleID: articleID, ImageURL: imageURL, Name: name, URL: url}, nil
}

func NewEmptyImageCandidateBatch(length int) generic.Batch[*ImageCandidate] {
	return generic.NewEmptyBatch[*ImageCandidate](length)
}

func ImageCandidateBatchFromBytes(data []byte) (*generic.Batch[*ImageCandidate], error) {
	batch, err := generic.BatchFromBytes(data, ImageCandidateFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}
