package logic

import "time"
import "strings"
import "src/common/record"

func FilterByQuery(records []record.Record) (q1Toq2 []record.Record, q3Toq5 []record.Record) {
	q1Toq2 = make([]record.Record, 0, len(records))
	q3Toq5 = make([]record.Record, 0, len(records))
 
	for _, r := range records {
		q1Toq2Rec := r
		q1Toq2Rec.Abstract = ""
		q1Toq2 = append(q1Toq2, q1Toq2Rec)

		q3Toq5Rec := r
		q3Toq5Rec.Image.ContentURL = ""
		q3Toq5Rec.Name = ""
		q3Toq5Rec.Sections = ""
		q3Toq5 = append(q3Toq5, q3Toq5Rec)
	}
	return q1Toq2, q3Toq5
}


func FilterByYear(records []record.Record, year int) []record.Record {
	filtered := make([]record.Record, 0, len(records))
	limitYear := time.Date(year+1,1,1,0,0,0,0, time.UTC).UnixMilli()
	for _, r := range records {
		if r.DateModified < limitYear {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func FilterByEnglish(records []record.Record) []record.Record {
	filtered := make([]record.Record, 0, len(records))
	for _, r := range records {
		if r.InLanguage.Identifier == "en" {

			// capaz esto moverlo a otra parte
			r.Abstract = ""
			r.Image.ContentURL = ""
			r.Sections = ""
			r.Name = ""
			r.InLanguage.Identifier = ""
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func FilterByBio(records []record.Record) []record.Record {
	filtered := make([]record.Record, 0, len(records))
	for _, r := range records {
		sectionsLower := strings.ToLower(r.Sections)
		if strings.Contains(sectionsLower, "biography") || strings.Contains(sectionsLower, "biographie") {
			// capaz esto moverlo a otra parte
			r.Abstract = ""
			r.DateModified = 0
			r.Sections = ""
			r.InLanguage.Identifier = ""
			filtered = append(filtered, r)
		}
	}
	return filtered
}