package messageprotocol

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type WordCount struct {
	ArticleID    string
	DateModified int64
	URL          string
	WordCount    uint32
}

func (e *WordCount) ToBytes() ([]byte, error) {
	result := utils.SerializeString(e.ArticleID)
	result = append(result, utils.SerializeInt64(e.DateModified)...)
	result = append(result, utils.SerializeString(e.URL)...)
	result = append(result, utils.SerializeUint32(e.WordCount)...)
	return result, nil
}

func WordCountFromBytes(data []byte, offset *int) (*WordCount, error) {
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
	wordCount, err := utils.ReadUint32FromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &WordCount{
		ArticleID:    articleID,
		DateModified: dateModified,
		URL:          url,
		WordCount:    wordCount,
	}, nil
}

func NewEmptyWordCountBatch(len int) generic.Batch[*WordCount] {
	return generic.NewEmptyBatch[*WordCount](len)
}

func WordCountBatchFromBytes(data []byte) (*generic.Batch[*WordCount], error) {
	batch, err := generic.BatchFromBytes(data, WordCountFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}
