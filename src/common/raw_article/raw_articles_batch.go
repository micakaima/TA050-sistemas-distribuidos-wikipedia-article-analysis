package messageprotocol

import (
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/record"
)

type RawArticlesBatch = generic.Batch[*RawArticle]

func NewEmptyArticlesBatch(len int) RawArticlesBatch {
	return RawArticlesBatch(generic.NewEmptyBatch[*RawArticle](len))
}

// Convierte un batch de artículos crudos a una lista de Records.
func ArticlesBatchToRecords(batch RawArticlesBatch) []record.Record {
	records := []record.Record{}
	for _, article := range batch.Items {
		records = append(records, RawArticleToRecord(*article))
	}
	return records
}
