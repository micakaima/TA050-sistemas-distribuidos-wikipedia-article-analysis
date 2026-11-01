package messageprotocol

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type RawAbstract struct {
	ArticleID    string
	DateModified int64
	InLanguage   string
	URL          string
	Abstract     string
}

func (r *RawAbstract) ToBytes() ([]byte, error) {
	result := utils.SerializeString(r.ArticleID)
	result = append(result, utils.SerializeInt64(r.DateModified)...)
	result = append(result, utils.SerializeString(r.InLanguage)...)
	result = append(result, utils.SerializeString(r.URL)...)
	result = append(result, utils.SerializeString(r.Abstract)...)
	return result, nil
}

func RawAbstractFromBytes(data []byte, offset *int) (*RawAbstract, error) {
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
	abstract, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &RawAbstract{
		ArticleID:    articleID,
		DateModified: dateModified,
		InLanguage:   inLanguage,
		URL:          url,
		Abstract:     abstract,
	}, nil
}

func NewEmptyRawAbstractBatch(len int) generic.Batch[*RawAbstract] {
	return generic.NewEmptyBatch[*RawAbstract](len)
}

func RawAbstractBatchFromBytes(data []byte) (*generic.Batch[*RawAbstract], error) {
	batch, err := generic.BatchFromBytes(data, RawAbstractFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}
