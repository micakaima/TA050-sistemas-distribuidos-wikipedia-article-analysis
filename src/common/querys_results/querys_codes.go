package querysresults

type QueryResultCode byte

const (
	Q1Code QueryResultCode = iota + 1
	Q2Code
	Q3Code
	Q4Code
	Q5Code
	UnknownCode
)
