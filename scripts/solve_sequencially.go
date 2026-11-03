package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	download "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/logic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	qresult "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
	article "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/raw_article"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/record"

	"github.com/bbalet/stopwords"
	"github.com/parquet-go/parquet-go"
)

const PUNCTUATION_SYMBOLS = "-.,():;!?[]{}'\""
const SPLITTER_YEAR = 2020
const QUERY1_OUTPUT_FILENAME = "query_1_result.txt"
const QUERY2_OUTPUT_FILENAME = "query_2_result.txt"
const QUERY3_OUTPUT_FILENAME = "query_3_result.txt"
const QUERY4_OUTPUT_FILENAME = "query_4_result.txt"
const QUERY5_OUTPUT_FILENAME = "query_5_result.txt"

func parseArguments() (string, string, int, error) {
	args := os.Args[1:]
	if len(args) != 3 {
		return "", "", 0, fmt.Errorf("[ERROR] Not enough arguments: arg1=INPUT_DIR, arg2=OUTPUT_DIR, arg3=NUM_ROWS")
	}
	inputDir := args[0]
	outputDir := args[1]
	numRows, err := strconv.Atoi(args[2])
	if err != nil {
		return "", "", 0, fmt.Errorf("[ERROR] NUM_ROWS must be a valid number")
	}
	return inputDir, outputDir, numRows, nil
}

func main() {
	inputDir, outputDir, numRows, err := parseArguments()
	if err != nil {
		fmt.Println(err)
		return
	}
	records, err := obtainAllRecords(inputDir, numRows)
	if err != nil {
		fmt.Println(err)
		return
	}

	solveQ1ToQ2(records, outputDir)
	solveQ3ToQ5(records, numRows, outputDir)
}

func solveQ1ToQ2(records []record.Record, outputDir string) {
	english2020Articles := obtainEnglish2020Articles(records)
	outputPathQ1 := filepath.Join(outputDir, QUERY1_OUTPUT_FILENAME)
	saveQ1Result(outputPathQ1, english2020Articles)

	obtainBio2020Articles, err := obtainBio2020Articles(records)
	if err != nil {
		fmt.Println(err)
		return
	}
	outputPathQ2 := filepath.Join(outputDir, QUERY2_OUTPUT_FILENAME)
	saveQ2Result(outputPathQ2, obtainBio2020Articles)
}

func solveQ3ToQ5(records []record.Record, numRows int, outputDir string) {
	rawAbstracts := obtainRawAbstracts(records, numRows)
	cleanVocabularies := obtainCleanVocabularies(rawAbstracts, numRows)

	solveQ3(cleanVocabularies, outputDir)
	solveQ4(cleanVocabularies, outputDir)
	solveQ5(cleanVocabularies, numRows, outputDir)
}

func solveQ3(cleanVocabularies []messages.ExtractedVocabulary, outputDir string) {
	enWords := make(map[string]struct{})
	frWords := make(map[string]struct{})
	for _, v := range cleanVocabularies {
		var target map[string]struct{}
		switch v.InLanguage {
		case "en":
			target = enWords
		case "fr":
			target = frWords
		default:
			continue
		}
		for word := range v.Vocabulary {
			target[word] = struct{}{}
		}
	}

	sharedWords := make([]string, 0)
	for word := range enWords {
		if _, ok := frWords[word]; ok {
			sharedWords = append(sharedWords, word)
		}
	}
	sort.Strings(sharedWords)

	outputPath := filepath.Join(outputDir, QUERY3_OUTPUT_FILENAME)
	saveQ3Result(outputPath, sharedWords)
}

func solveQ4(cleanVocabularies []messages.ExtractedVocabulary, outputDir string) {
	wordCounts := make(map[string]uint32)
	for _, v := range cleanVocabularies {
		for word := range v.Vocabulary {
			wordCounts[word]++
		}
	}

	topWords := make([]*qresult.WordFrequency, 0, len(wordCounts))
	for word, count := range wordCounts {
		topWords = append(topWords, &qresult.WordFrequency{Word: word, Count: count})
	}
	sort.Slice(topWords, func(i, j int) bool {
		if topWords[i].Count != topWords[j].Count {
			return topWords[i].Count > topWords[j].Count // count desc
		}
		return topWords[i].Word < topWords[j].Word // word asc
	})
	if len(topWords) > 20 {
		topWords = topWords[:20]
	}

	outputPath := filepath.Join(outputDir, QUERY4_OUTPUT_FILENAME)
	saveQ4Result(outputPath, topWords)
}

func solveQ5(cleanVocabularies []messages.ExtractedVocabulary, numRows int, outputDir string) {
	wordCounts := obtainWordCounts(cleanVocabularies, numRows)
	oldAverage := obtainOldArticlesAverage(wordCounts)
	aboveAverageArticles := obtainAboveAverageArticles(oldAverage, wordCounts)

	outputPath := filepath.Join(outputDir, QUERY5_OUTPUT_FILENAME)
	saveQ5Result(outputPath, aboveAverageArticles)
}

func obtainAllRecords(inputDir string, numRows int) ([]record.Record, error) {
	files, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, fmt.Errorf("[ERROR] While reading INPUT_DIR")
	}
	records := make([]record.Record, 0, 2*numRows)
	for _, file := range files {
		fullPath := filepath.Join(inputDir, file.Name())
		obtainedRecords, err := obtainRecords(fullPath, numRows)
		if err != nil {
			return nil, fmt.Errorf("[ERROR] While obtaining records")
		}
		records = append(records, obtainedRecords...)
	}
	return records, nil
}

func obtainRecords(path string, numRows int) ([]record.Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := parquet.NewReader(file)
	defer reader.Close()

	records := make([]record.Record, 0, numRows)
	for {
		rawArticle := make(article.RawArticle)
		if err := reader.Read(&rawArticle); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		records = append(records, article.RawArticleToRecord(rawArticle))
	}
	return records, nil
}

func obtainEnglish2020Articles(records []record.Record) []qresult.English2020Article {
	english2020Articles := make([]qresult.English2020Article, 0)
	for _, r := range records {
		year := time.UnixMilli(r.DateModified).Year()
		if year <= SPLITTER_YEAR && r.InLanguage.Identifier == "en" {
			english2020Articles = append(english2020Articles, qresult.English2020Article{
				ArticleID:    r.ArticleID,
				DateModified: r.DateModified,
				URL:          r.URL,
			})
		}
	}
	return english2020Articles
}

func obtainBio2020Articles(records []record.Record) ([]qresult.ImageArticle, error) {
	imagesCandidates := messages.NewEmptyImageCandidateBatch(0)
	for _, r := range records {
		year := time.UnixMilli(r.DateModified).Year()
		sections := strings.ToLower(r.Sections)
		if year <= SPLITTER_YEAR && (strings.Contains(sections, "biography") || strings.Contains(sections, "biographie")) {
			imagesCandidates.Add(&messages.ImageCandidate{
				ArticleID: r.ArticleID,
				ImageURL:  r.Image.ContentURL,
				Name:      r.Name,
				URL:       r.URL,
			})
		}
	}
	q2Result := download.DownloadImage(imagesCandidates)
	bio2020Articles := make([]qresult.ImageArticle, 0, q2Result.ResultArticles.Len())
	for _, a := range q2Result.ResultArticles.Items {
		bio2020Articles = append(bio2020Articles, *a)
	}
	return bio2020Articles, nil
}

func obtainRawAbstracts(records []record.Record, numRows int) []messages.RawAbstract {
	rawAbstracts := make([]messages.RawAbstract, 0, numRows*2)
	for _, r := range records {
		rawAbstracts = append(rawAbstracts, messages.RawAbstract{
			ArticleID:    r.ArticleID,
			DateModified: r.DateModified,
			InLanguage:   r.InLanguage.Identifier,
			URL:          r.URL,
			Abstract:     r.Abstract,
		})
	}
	return rawAbstracts
}

func obtainCleanVocabularies(rawAbstracts []messages.RawAbstract, numRows int) []messages.ExtractedVocabulary {
	vocabularies := make([]messages.ExtractedVocabulary, 0, numRows*2)
	for _, abs := range rawAbstracts {
		absVocabulary := extractCleanVocabulary(abs.Abstract, abs.InLanguage)
		cleanWordCount := len(absVocabulary)
		vocabularies = append(vocabularies, messages.ExtractedVocabulary{
			ArticleID:    abs.ArticleID,
			DateModified: abs.DateModified,
			InLanguage:   abs.InLanguage,
			URL:          abs.URL,
			Vocabulary:   absVocabulary,
			WordCount:    uint32(cleanWordCount),
		})
	}
	return vocabularies
}

func extractCleanVocabulary(abstract string, lang string) map[string]struct{} {
	cleanVocabulary := make(map[string]struct{})
	for _, word := range strings.Fields(abstract) {
		lowercaseWord := strings.ToLower(word)
		cleanWord := strings.Trim(stopwords.CleanString(lowercaseWord, lang, false), PUNCTUATION_SYMBOLS)
		if _, exists := cleanVocabulary[cleanWord]; !exists && len(cleanWord) > 0 && cleanWord != " " {
			cleanVocabulary[cleanWord] = struct{}{}
		}
	}
	return cleanVocabulary
}

func obtainWordCounts(vocabularies []messages.ExtractedVocabulary, numRows int) []messages.WordCount {
	wordCounts := make([]messages.WordCount, 0, numRows*2)
	for _, v := range vocabularies {
		wordCounts = append(wordCounts, messages.WordCount{
			ArticleID:    v.ArticleID,
			DateModified: v.DateModified,
			URL:          v.URL,
			WordCount:    v.WordCount,
		})
	}
	return wordCounts
}

func obtainOldArticlesAverage(wordCounts []messages.WordCount) float64 {
	totalSum := uint32(0)
	totalCant := 0
	for _, wc := range wordCounts {
		year := time.UnixMilli(wc.DateModified).Year()
		if year <= SPLITTER_YEAR {
			totalSum += wc.WordCount
			totalCant++
		}
	}
	if totalCant == 0 {
		return 0
	}
	return float64(totalSum) / float64(totalCant)
}

func obtainAboveAverageArticles(oldAverage float64, wordCounts []messages.WordCount) []qresult.AboveAverageArticle {
	aboveAverageArticles := make([]qresult.AboveAverageArticle, 0)
	for _, wc := range wordCounts {
		year := time.UnixMilli(wc.DateModified).Year()
		if year > SPLITTER_YEAR && float64(wc.WordCount) > oldAverage {
			aboveAverageArticles = append(aboveAverageArticles, qresult.AboveAverageArticle{
				ArticleID: wc.ArticleID,
				URL:       wc.URL,
				WordCount: wc.WordCount,
			})
		}
	}
	return aboveAverageArticles
}

func createPath(path string) (*os.File, error) {
	dir := filepath.Dir(path)
	if err := os.RemoveAll(path); err != nil {
		return nil, fmt.Errorf("[ERROR] While removing original path")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("[ERROR] While creating path for output")
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("[ERROR] While creating file for output")
	}
	return file, nil
}

func saveQ1Result(path string, english2020Articles []qresult.English2020Article) {
	file, err := createPath(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	for _, a := range english2020Articles {
		if _, err := file.WriteString(a.ToRow()); err != nil {
			fmt.Println("[ERROR] While writing Q1 result row")
			return
		}
	}
}

func saveQ2Result(path string, bio2020Articles []qresult.ImageArticle) {
	file, err := createPath(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	for _, a := range bio2020Articles {
		if _, err := file.WriteString(a.ToRow()); err != nil {
			fmt.Println("[ERROR] While writing Q2 result row")
			return
		}
	}
}

func saveQ3Result(path string, sharedWords []string) {
	file, err := createPath(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	for _, w := range sharedWords {
		if _, err := file.WriteString(w + "\n"); err != nil {
			fmt.Println("[ERROR] While writing Q3 result row")
			return
		}
	}
}

func saveQ4Result(path string, topWords []*qresult.WordFrequency) {
	file, err := createPath(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	for _, w := range topWords {
		if _, err := file.WriteString(w.ToRow()); err != nil {
			fmt.Println("[ERROR] While writing Q4 result row")
			return
		}
	}
}

func saveQ5Result(path string, aboveAverageArticles []qresult.AboveAverageArticle) {
	file, err := createPath(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	for _, a := range aboveAverageArticles {
		if _, err := file.WriteString(a.ToRow()); err != nil {
			fmt.Println("[ERROR] While writing Q5 result row")
			return
		}
	}
}
