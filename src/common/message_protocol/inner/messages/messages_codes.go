package messageprotocol

type InnerMsgCode byte

const (
	ArticlesBatchCode InnerMsgCode = iota
	EOFCode
	ResultCode
)
