package messageprotocol

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type OldArticleWordCount struct {
	ArticleID string
	WordCount uint32
}

func (e *OldArticleWordCount) ToBytes() ([]byte, error) {
	result := utils.SerializeString(e.ArticleID)
	result = append(result, utils.SerializeUint32(e.WordCount)...)
	return result, nil
}

func OldArticleWordCountFromBytes(data []byte, offset *int) (*OldArticleWordCount, error) {
	articleID, err := utils.ReadStringFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	wordCount, err := utils.ReadUint32FromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &OldArticleWordCount{
		ArticleID: articleID,
		WordCount: wordCount,
	}, nil
}

func NewEmptyOldArticleWordCountBatch(len int) generic.Batch[*OldArticleWordCount] {
	return generic.NewEmptyBatch[*OldArticleWordCount](len)
}

func OldArticleWordCountBatchFromBytes(data []byte) (*generic.Batch[*OldArticleWordCount], error) {
	batch, err := generic.BatchFromBytes(data, OldArticleWordCountFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}
