package messageprotocol

import (
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
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
