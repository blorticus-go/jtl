package jtl

import "strconv"

// import stats "github.com/blorticus-go/statistics"

// SummaryStatistics are summarized values for data series in a JTL data source.
type SummaryStatistics struct {
	Mean                        float64
	Median                      float64
	Maximum                     float64
	Minimum                     float64
	PopulationStandardDeviation float64
	ValueAt5thPercentile        float64
	ValueAt95thPercentile       float64
}

// SummarizableDimensions describe data elements from a JTL data source that can be
// summarized into a SummaryStatstics set.  Specifically, a Dimension is a potentially
// keyable value from the data source.  For example, for the Dimension RequestURLs,
// the key is the request URL as a string.
type SummarizableDimension int

const (
	// Aggregate is unkeyed and represents the summary across all data rows.
	Aggregate SummarizableDimension = iota

	// Aggregate is keyed by RequestURL string.
	RequestURLs

	// RequestSizes is keyed by the Request body size in bytes as an int64 (the value -1 represents the special
	// case where the request was not successful).
	RequestSizes

	// ResponseSizes is keyed by the Response message size in bytes as an int64 (as in RequestSizes).
	ResponseSizes

	// ResponseCodes is keyed by the HTTP Response Code as an int (as in RequestSizes).
	ResponseCodes

	// ErrorMessages is keyed by the error message text as a string.
	ErrorMessages
)

var SummarizableDimensionAsString = map[SummarizableDimension]string{
	Aggregate:     "Aggregate",
	RequestURLs:   "RequestURLs",
	RequestSizes:  "RequestSizes",
	ResponseSizes: "ResponseSizes",
	ResponseCodes: "ResponseCodes",
	ErrorMessages: "ErrorMessages",
}

// SummarizedDimensionKey is the value returned by each call to SummarizationIterator's Next
// method.
type SummarizedDimensionKey struct {
	Dimension  SummarizableDimension
	Key        interface{}
	Statistics *SummaryStatistics
}

func (dk *SummarizedDimensionKey) KeyAsString() string {
	switch k := dk.Key.(type) {
	case string:
		return k
	case int64:
		return strconv.FormatInt(k, 10)
	case uint64:
		return strconv.FormatUint(k, 10)
	}

	return ""
}

// SummarizationIterator provides a (unsynchronized) iterator over a set of summarizable
// dimension sets.  Summarizable dimension sets consist of one or more tuples of
// [dimension_type, key].  For example, if the dimension_type is RequestURLs, then each key
// is a unique Request URL.
type SummarizationIterator struct{}

// Next returns the next Dimension/Key combination.  The order is guaranteed to be per-dimension and
// sorted appropriately by the key's native type.  For example, if the data source has the four
// response sizes 100, 1000, 2000, 10000, then the first SummarizedDimensionKey returned that
// has Dimension of ResponseSizes with have the Key 100, and the next three calls to Next()
// will return ResponsesSizes/1000, ResponseSizes/2000 and ResponseSizes/10000, in that order.
// Return nil when there are all dimension/key combinations have been provided.
func (iterator *SummarizationIterator) Next() *SummarizedDimensionKey {
	return nil
}

// type SummaryStatisticsForFloat64DataSet struct {
// 	mean                        float64
// 	median                      float64
// 	maximum                     float64
// 	minimum                     float64
// 	populationStandardDeviation float64
// 	valueAt5thPercentile        float64
// 	valueAt95thPercentile       float64
// 	individualDataPoints        []float64
// }

// func (s *SummaryStatisticsForFloat64DataSet) Mean() float64 {
// 	return s.mean
// }

// func (s *SummaryStatisticsForFloat64DataSet) Median() float64 {
// 	return s.median
// }

// func (s *SummaryStatisticsForFloat64DataSet) Maximum() float64 {
// 	return s.maximum
// }

// func (s *SummaryStatisticsForFloat64DataSet) Minimum() float64 {
// 	return s.minimum
// }

// func (s *SummaryStatisticsForFloat64DataSet) PopulationStandardDeviation() float64 {
// 	return s.populationStandardDeviation
// }

// func (s *SummaryStatisticsForFloat64DataSet) ValueAt5thPercentile() float64 {
// 	return s.valueAt5thPercentile
// }

// func (s *SummaryStatisticsForFloat64DataSet) ValueAt95thPercentile() float64 {
// 	return s.valueAt95thPercentile
// }

// func (s *SummaryStatisticsForFloat64DataSet) computeStatsFromRawDataSet() error {
// 	if len(s.individualDataPoints) == 0 {
// 		s.mean, s.median, s.maximum, s.minimum, s.populationStandardDeviation = 0, 0, 0, 0, 0
// 		return nil
// 	}

// 	sampleSet, err := stats.MakeStatisticalSampleSetFrom(s.individualDataPoints)
// 	if err != nil {
// 		return err
// 	}

// 	s.mean, s.median, s.maximum, s.minimum, s.populationStandardDeviation = sampleSet.Mean(), sampleSet.Median(), sampleSet.Maximum(), sampleSet.Minimum(), sampleSet.PopulationStdev()

// 	return nil
// }

// type SummaryStatisticsForTPSData struct {
// 	mean                               float64
// 	median                             float64
// 	minimum                            float64
// 	maximum                            float64
// 	populationStandardDeviation        float64
// 	valueAt5thPercentile               float64
// 	valueAt95thPercentile              float64
// 	countOfSamplesByOneSecondTimestamp map[uint64]uint64
// }

// func (s *SummaryStatisticsForTPSData) Mean() float64 {
// 	return s.mean
// }

// func (s *SummaryStatisticsForTPSData) Median() float64 {
// 	return s.median
// }

// func (s *SummaryStatisticsForTPSData) Maximum() float64 {
// 	return s.maximum
// }

// func (s *SummaryStatisticsForTPSData) Minimum() float64 {
// 	return s.minimum
// }

// func (s *SummaryStatisticsForTPSData) PopulationStandardDeviation() float64 {
// 	return s.populationStandardDeviation
// }

// func (s *SummaryStatisticsForTPSData) ValueAt5thPercentile() float64 {
// 	return s.valueAt5thPercentile
// }

// func (s *SummaryStatisticsForTPSData) ValueAt95thPercentile() float64 {
// 	return s.valueAt95thPercentile
// }

// func (s *SummaryStatisticsForTPSData) computeStatsFromRawDataSet(unixEpochSecondsAtTestStart uint64, unixEpochSecondsAtEndOfTest uint64) error {
// 	if len(s.countOfSamplesByOneSecondTimestamp) == 0 || int64(unixEpochSecondsAtEndOfTest)-int64(unixEpochSecondsAtTestStart) <= 0 {
// 		s.mean, s.median, s.maximum, s.minimum, s.populationStandardDeviation = 0, 0, 0, 0, 0
// 		return nil
// 	}

// 	tpsAtEachSecondInterval := make([]float64, unixEpochSecondsAtEndOfTest-unixEpochSecondsAtTestStart+1)

// 	timestamp := unixEpochSecondsAtTestStart
// 	for i := 0; timestamp < unixEpochSecondsAtEndOfTest; i++ {
// 		tpsAtEachSecondInterval[i] = float64(s.countOfSamplesByOneSecondTimestamp[timestamp])
// 		timestamp++
// 	}

// 	sampleSet, err := stats.MakeStatisticalSampleSetFrom(tpsAtEachSecondInterval)
// 	if err != nil {
// 		return err
// 	}

// 	s.mean, s.median, s.maximum, s.minimum, s.populationStandardDeviation = sampleSet.Mean(), sampleSet.Median(), sampleSet.Maximum(), sampleSet.Minimum(), sampleSet.PopulationStdev()

// 	return nil
// }

// type SummaryInformation struct {
// 	TotalNumberOfRequests     uint
// 	NumberOfSuccssfulRequests int // -1 if cannot be computed
// }

// type SummaryContainer struct {
// 	Information                       *SummaryInformation
// 	TimeToFirstBytes                  *SummaryStatisticsForFloat64DataSet
// 	TimeToLastByte                    *SummaryStatisticsForFloat64DataSet
// 	MovingWindowTransactionsPerSecond *SummaryStatisticsForTPSData
// }

// type AllSummarizedDimensions struct {
// 	Overall      *SummaryContainer
// 	MovingTPS    *SummaryContainer
// 	RequestSize  map[uint64]*SummaryContainer
// 	ResponseSize map[uint64]*SummaryContainer
// 	RequestURL   map[string]*SummaryContainer
// }
