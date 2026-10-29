package logic

import "time"
import "strings"
import "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/record"

func FilterByQuery(records []record.Record) (q1Toq2 []record.YearCandidate, q3Toq5 []record.Record) {
	q1Toq2 = make([]record.YearCandidate, 0, len(records))
	q3Toq5 = make([]record.Record, 0, len(records))
 
	for _, r := range records {
		q1Toq2 = append(q1Toq2, record.YearCandidate{
			ArticleID: r.ArticleID,
			DateModified: r.DateModified,
			ImageURL: r.Image.ContentURL,
			InLanguage: r.InLanguage.Identifier,
			Name: r.Name,
			Sections: r.Sections,
			URL: r.URL,
		}) 

		q3Toq5Rec := r
		q3Toq5Rec.Image.ContentURL = ""
		q3Toq5Rec.Name = ""
		q3Toq5Rec.Sections = ""
		q3Toq5 = append(q3Toq5, q3Toq5Rec)
	}
	return q1Toq2, q3Toq5
}


func FilterByYear(records []record.YearCandidate, year int) ([]record.EnglishCandidate, []record.BioCandidate) {
	enFiltered := make([]record.EnglishCandidate, 0, len(records))
	bioFiltered := make([]record.BioCandidate, 0, len(records))
	limitYear := time.Date(year+1,1,1,0,0,0,0, time.UTC).UnixMilli()
	for _, r := range records {
		if r.DateModified < limitYear {
			enFiltered = append(enFiltered, record.EnglishCandidate {
				ArticleID: r.ArticleID,
				DateModified: r.DateModified,
				InLanguage: r.InLanguage,
				URL: r.URL,
			})
			bioFiltered = append(bioFiltered, record.BioCandidate {
				ArticleID: r.ArticleID,
				ImageURL: r.ImageURL,
				Name: r.Name,
				Sections: r.Sections,
				URL: r.URL,
			})
		}
	}
	return enFiltered, bioFiltered
}

func FilterByEnglish(records []record.EnglishCandidate) []record.Q1Result {
	result := make([]record.Q1Result, 0, len(records))
	for _, r := range records {
		if r.InLanguage == "en" {
			result = append(result, record.Q1Result {
				ArticleID: r.ArticleID,
				DateModified: r.DateModified,
				URL: r.URL,
			})
		}
	}
	return result
}

func FilterByBio(records []record.BioCandidate) []record.BioMetadata {
	filtered := make([]record.BioMetadata, 0, len(records))
	for _, r := range records {
		sectionsLower := strings.ToLower(r.Sections)
		if strings.Contains(sectionsLower, "biography") || strings.Contains(sectionsLower, "biographie") {
			filtered = append(filtered, record.BioMetadata {
				ArticleID: r.ArticleID,
				ImageURL: r.ImageURL,
				Name: r.Name,
				URL: r.URL,
			})
		}
	}
	return filtered
}