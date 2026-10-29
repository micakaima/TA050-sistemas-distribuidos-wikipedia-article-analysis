package messageprotocol

import (
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/record"
)

// Convierte un artículo crudo a una estructura Record,
// quedándose con sólo los campos que éste último incluye.
func RawArticleToRecord(m map[string]any) (record.Record, error) {
	var rec record.Record

	rec.ArticleID = utils.GetKeyAsString(m, "identifier")
	rec.Abstract = utils.GetKeyAsString(m, "abstract")
	rec.DateModified = utils.GetKeyAsInt64(m, "date_modified")
	rec.Name = utils.GetKeyAsString(m, "name")
	rec.Sections = utils.GetKeyAsString(m, "sections")
	rec.URL = utils.GetKeyAsString(m, "url")

	if imgStruct := utils.GetSubMap(m, "image"); imgStruct != nil {
		rec.Image.ContentURL = utils.GetKeyAsString(imgStruct, "content_url")
	}
	if imgStruct := utils.GetSubMap(m, "in_language"); imgStruct != nil {
		rec.InLanguage.Identifier = utils.GetKeyAsString(imgStruct, "identifier")
	}

	return rec, nil
}
