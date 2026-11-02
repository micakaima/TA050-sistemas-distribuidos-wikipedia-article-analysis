package querysresults

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type Q1Result struct {
	ArticleID    string
	DateModified int64
	URL          string
}

func (result *Q1Result) ToBytes() ([]byte, error) {
	bytes := utils.SerializeString(result.ArticleID)
	bytes = append(bytes, utils.SerializeInt64(result.DateModified)...)
	bytes = append(bytes, utils.SerializeString(result.URL)...)
	return bytes, nil
}

func Q1ResultFromBytes(data []byte, offset *int) (*Q1Result, error) {
	articleID, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	dateModified, err := utils.ReadInt64FromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	url, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &Q1Result{ArticleID: articleID, DateModified: dateModified, URL: url}, nil
}

type Q2Result struct {
	ArticleID  string
	Name       string
	URL        string
	ImageBytes []byte
	Error      string
}

func (result *Q2Result) ToBytes() ([]byte, error) {
	bytes := utils.SerializeString(result.ArticleID)
	bytes = append(bytes, utils.SerializeString(result.Name)...)
	bytes = append(bytes, utils.SerializeString(result.URL)...)
	bytes = append(bytes, utils.SerializeString(string(result.ImageBytes))...)
	bytes = append(bytes, utils.SerializeString(result.Error)...)
	return bytes, nil
}

func Q2ResultFromBytes(data []byte, offset *int) (*Q2Result, error) {
	articleID, err := utils.ReadStringFromBytes(data, offset)
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
	imageString, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	errorMessage, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &Q2Result{ArticleID: articleID, Name: name, URL: url, ImageBytes: []byte(imageString), Error: errorMessage}, nil
}

func NewEmptyQ1ResultBatch(length int) generic.Batch[*Q1Result] {
	return generic.NewEmptyBatch[*Q1Result](length)
}

func Q1ResultBatchFromBytes(data []byte) (*generic.Batch[*Q1Result], error) {
	batch, err := generic.BatchFromBytes(data, Q1ResultFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}

func NewEmptyQ2ResultBatch(length int) generic.Batch[*Q2Result] {
	return generic.NewEmptyBatch[*Q2Result](length)
}

func Q2ResultBatchFromBytes(data []byte) (*generic.Batch[*Q2Result], error) {
	batch, err := generic.BatchFromBytes(data, Q2ResultFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}
