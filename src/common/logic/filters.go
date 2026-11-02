package logic

import (
	"strings"
	"time"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
)

func FilterYearCandidates(candidates generic.Batch[*messages.YearCandidate], year int) (generic.Batch[*messages.EnglishCandidate], generic.Batch[*messages.BioCandidate]) {
	englishCandidates := messages.NewEmptyEnglishCandidateBatch(candidates.Len())
	bioCandidates := messages.NewEmptyBioCandidateBatch(candidates.Len())
	limitYear := time.Date(year+1, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

	for _, candidate := range candidates.Items {
		if candidate.DateModified >= limitYear {
			continue
		}
		englishCandidates.Add(&messages.EnglishCandidate{
			ArticleID:    candidate.ArticleID,
			DateModified: candidate.DateModified,
			InLanguage:   candidate.InLanguage,
			URL:          candidate.URL,
		})
		bioCandidates.Add(&messages.BioCandidate{
			ArticleID: candidate.ArticleID,
			ImageURL:  candidate.ImageURL,
			Name:      candidate.Name,
			Sections:  candidate.Sections,
			URL:       candidate.URL,
		})
	}

	return englishCandidates, bioCandidates
}

func FilterEnglishCandidates(candidates generic.Batch[*messages.EnglishCandidate]) generic.Batch[*qresults.Q1Result] {
	results := qresults.NewEmptyQ1ResultBatch(candidates.Len())
	for _, candidate := range candidates.Items {
		if candidate.InLanguage == "en" {
			results.Add(&qresults.Q1Result{ArticleID: candidate.ArticleID, DateModified: candidate.DateModified, URL: candidate.URL})
		}
	}
	return results
}

func FilterBioCandidates(candidates generic.Batch[*messages.BioCandidate]) generic.Batch[*messages.ImageCandidate] {
	filtered := messages.NewEmptyImageCandidateBatch(candidates.Len())
	for _, candidate := range candidates.Items {
		sectionsLower := strings.ToLower(candidate.Sections)
		if strings.Contains(sectionsLower, "biography") || strings.Contains(sectionsLower, "biographie") {
			filtered.Add(&messages.ImageCandidate{
				ArticleID: candidate.ArticleID,
				ImageURL:  candidate.ImageURL,
				Name:      candidate.Name,
				URL:       candidate.URL,
			})
		}
	}
	return filtered
}
