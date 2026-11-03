package messageprotocol

type InnerMsgCode byte

const (
	ArticlesBatchCode InnerMsgCode = iota
	YearCandidateCode
	RawAbstractCode
	VocabularyCode
	WordCountCode
	OldArticleCountCode
	NewArticleCountCode
	AverageCode
	EOFCode
	ResultCode
	EnglishCandidateCode
	BioCandidateCode
	ImageCandidateCode
	Q1ResultCode
	Q2ResultCode
	SharedWordCandidateCode
	SharedWordsPartialCode
)
