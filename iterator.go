package jtl

// import "strconv"

// // import stats "github.com/blorticus-go/statistics"

// // SummaryStatistics are summarized values for data series in a JTL data source.
// // type SummaryStatistics struct {
// // 	Mean                        float64
// // 	Median                      float64
// // 	Maximum                     float64
// // 	Minimum                     float64
// // 	PopulationStandardDeviation float64
// // 	ValueAt5thPercentile        float64
// // 	ValueAt95thPercentile       float64
// // }

// // SummarizableCategory describe data elements from a JTL data source that can be
// // summarized into a SummaryStatstics set.  Specifically, a Dimension is a potentially
// // keyable value from the data source.  For example, for the Dimension RequestURLs,
// // the key is the request URL as a string.
// type SummarizableCategory int

// const (
// 	// Aggregate is unkeyed and represents the summary across all data rows.
// 	Aggregate SummarizableCategory = iota

// 	// Aggregate is keyed by RequestURL string.
// 	RequestURLs

// 	// RequestSizes is keyed by the Request body size in bytes as an int64 (the value -1 represents the special
// 	// case where the request was not successful).
// 	RequestSizes

// 	// ResponseSizes is keyed by the Response message size in bytes as an int64 (as in RequestSizes).
// 	ResponseSizes

// 	// ResponseCodes is keyed by the HTTP Response Code as an int (as in RequestSizes).
// 	ResponseCodes

// 	// ErrorMessages is keyed by the error message text as a string.
// 	ErrorMessages
// )

// var SummarizableCategoryAsString = map[SummarizableCategory]string{
// 	Aggregate:     "Aggregate",
// 	RequestURLs:   "RequestURLs",
// 	RequestSizes:  "RequestSizes",
// 	ResponseSizes: "ResponseSizes",
// 	ResponseCodes: "ResponseCodes",
// 	ErrorMessages: "ErrorMessages",
// }

// // type desiredCategoryTable struct {
// // 	Aggregate     bool
// // 	RequestURLs   bool
// // 	RequestSizes  bool
// // 	ResponseSizes bool
// // 	ResponseCodes bool
// // 	ErrorMessages bool
// // }

// // func desiredCategoriesTableFromCategoryList(dimensions ...SummarizableCategory) *desiredCategoryTable {
// // 	table := &desiredCategoryTable{}

// // 	for _, dimension := range dimensions {
// // 		switch dimension {
// // 		case Aggregate:
// // 			table.Aggregate = true
// // 		case RequestURLs:
// // 			table.RequestURLs = true
// // 		case RequestSizes:
// // 			table.RequestSizes = true
// // 		case ResponseCodes:
// // 			table.ResponseCodes = true
// // 		case ErrorMessages:
// // 			table.Aggregate = true
// // 		}
// // 	}

// // 	return table
// // }

// type SummaryDimensions struct {
// 	TimeToFirstByte *SummaryStatistics
// 	TimeToLastByte  *SummaryStatistics
// }

// // summarizedCategoryAndKey is the value returned by each call to SummarizationIterator's Next
// // method.
// type SummarizedCategoryAndKey struct {
// 	Category                   SummarizableCategory
// 	Key                        interface{}
// 	StatisticsFor              *SummaryDimensions
// 	CountOfRequests            uint
// 	NumberOfSuccessfulRequests int64 // -1 if this success flag isn't in data source
// }

// func (dk *SummarizedCategoryAndKey) KeyAsString() string {
// 	switch k := dk.Key.(type) {
// 	case string:
// 		return k
// 	case int64:
// 		return strconv.FormatInt(k, 10)
// 	case uint64:
// 		return strconv.FormatUint(k, 10)
// 	}

// 	return ""
// }

// // SummarizationIterator provides a (unsynchronized) iterator over a set of summarizable
// // sets.  Summarizable sets consist of one or more tuples of
// // [category, key].  For example, if the category is RequestURLs, then each key
// // is a unique Request URL.
// type SummarizationIterator struct {
// 	summarizationsInOrder            []*SummarizedCategoryAndKey
// 	indexOfLastSummarizationProvided int
// }

// func newSummarizationIterator(summarizationsInOrder []*SummarizedCategoryAndKey) *SummarizationIterator {
// 	return &SummarizationIterator{
// 		summarizationsInOrder:            summarizationsInOrder,
// 		indexOfLastSummarizationProvided: -1,
// 	}
// }

// // Next returns the next Category/Key combination.  The order is guaranteed to be per-category and
// // sorted appropriately by the key's native type.  For example, if the data source has the four
// // response sizes 100, 1000, 2000, 10000, then the first SummarizedDimensionKey returned that
// // has Dimension of ResponseSizes with have the Key 100, and the next three calls to Next()
// // will return ResponsesSizes/1000, ResponseSizes/2000 and ResponseSizes/10000, in that order.
// // Return nil when there are all category/key combinations have been provided.
// func (iterator *SummarizationIterator) Next() *SummarizedCategoryAndKey {
// 	indexOfNextSummarization := iterator.indexOfLastSummarizationProvided + 1

// 	if indexOfNextSummarization < len(iterator.summarizationsInOrder) {
// 		iterator.indexOfLastSummarizationProvided = indexOfNextSummarization
// 		return iterator.summarizationsInOrder[indexOfNextSummarization]
// 	}

// 	return nil
// }
