package messageprotocol

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type NewArticleWordCount struct {
	ArticleID string
	URL       string
	WordCount uint32
}

func (e *NewArticleWordCount) ToBytes() ([]byte, error) {
	result := utils.SerializeString(e.ArticleID)
	result = append(result, utils.SerializeString(e.URL)...)
	result = append(result, utils.SerializeUint32(e.WordCount)...)
	return result, nil
}

func NewArticleWordCountFromBytes(data []byte, offset *int) (*NewArticleWordCount, error) {
	articleID, err := utils.ReadStringFromBytes(data, offset)
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
	return &NewArticleWordCount{
		ArticleID: articleID,
		URL:       url,
		WordCount: wordCount,
	}, nil
}

func NewEmptyNewArticleWordCountBatch(len int) generic.Batch[*NewArticleWordCount] {
	return generic.NewEmptyBatch[*NewArticleWordCount](len)
}

func NewArticleWordCountBatchFromBytes(data []byte) (*generic.Batch[*NewArticleWordCount], error) {
	batch, err := generic.BatchFromBytes(data, NewArticleWordCountFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}
