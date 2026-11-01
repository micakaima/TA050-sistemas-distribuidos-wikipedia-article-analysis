package messageprotocol

import (
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/record"
)

type RawArticle map[string]interface{}

// Serializa un artículo / fila cruda.
func (article *RawArticle) ToBytes() ([]byte, error) {
	return utils.SerializeMap(*article)
}

// Devuelve un artículo / fila cruda a partir de una serie de bytes.
func RawArticleFromBytes(data []byte, offset *int) (*RawArticle, error) {
	rawArticleMap, err := utils.DeserializeMapFromBytes(data, offset)
	if err != nil {
		return nil, err
	}
	rawArticle := RawArticle(rawArticleMap)
	return &rawArticle, nil
}

// Convierte un artículo crudo a una estructura Record,
// quedándose con sólo los campos que éste último incluye.
func RawArticleToRecord(rawArticle RawArticle) record.Record {
	var rec record.Record

	rec.ArticleID = utils.GetKeyAsString(rawArticle, "identifier")
	rec.Abstract = utils.GetKeyAsString(rawArticle, "abstract")
	rec.DateModified = utils.GetKeyAsInt64(rawArticle, "date_modified")
	rec.Name = utils.GetKeyAsString(rawArticle, "name")
	rec.Sections = utils.GetKeyAsString(rawArticle, "sections")
	rec.URL = utils.GetKeyAsString(rawArticle, "url")

	if imgStruct := utils.GetSubMap(rawArticle, "image"); imgStruct != nil {
		rec.Image.ContentURL = utils.GetKeyAsString(imgStruct, "content_url")
	}
	if imgStruct := utils.GetSubMap(rawArticle, "in_language"); imgStruct != nil {
		rec.InLanguage.Identifier = utils.GetKeyAsString(imgStruct, "identifier")
	}

	return rec
}
