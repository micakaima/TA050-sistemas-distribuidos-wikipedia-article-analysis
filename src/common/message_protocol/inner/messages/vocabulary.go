package messageprotocol

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

type ExtractedVocabulary struct {
	ArticleID    string
	DateModified int64
	InLanguage   string
	Vocabulary   map[string]struct{}
	WordCount    uint32
}

func (e *ExtractedVocabulary) ToBytes() ([]byte, error) {
	result := utils.SerializeString(e.ArticleID)
	result = append(result, utils.SerializeInt64(e.DateModified)...)
	result = append(result, utils.SerializeString(e.InLanguage)...)
	result = append(result, utils.SerializeSet(e.Vocabulary)...)
	result = append(result, utils.SerializeUint32(e.WordCount)...)
	return result, nil
}

func ExtractedVocabularyFromBytes(data []byte, offset *int) (*ExtractedVocabulary, error) {
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
	vocabulary, err := utils.DeserializeSet(data, offset)
	if err != nil {
		return nil, err
	}
	wordCount, err := utils.ReadUint32FromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	return &ExtractedVocabulary{
		ArticleID:    articleID,
		DateModified: dateModified,
		InLanguage:   inLanguage,
		Vocabulary:   vocabulary,
		WordCount:    wordCount,
	}, nil
}

func NewEmptyExtractedVocabularyBatch(len int) generic.Batch[*ExtractedVocabulary] {
	return generic.NewEmptyBatch[*ExtractedVocabulary](len)
}

func ExtractedVocabularyBatchFromBytes(data []byte) (*generic.Batch[*ExtractedVocabulary], error) {
	batch, err := generic.BatchFromBytes(data, ExtractedVocabularyFromBytes)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}
