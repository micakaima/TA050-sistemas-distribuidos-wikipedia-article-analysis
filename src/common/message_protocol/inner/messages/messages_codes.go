package messageprotocol

type InnerMsgCode byte

const (
	ArticlesBatchCode InnerMsgCode = iota
	YearCandidateCode
	RawAbstractCode
	VocabularyCode
	WordCountCode
	EOFCode
	ResultCode
)
