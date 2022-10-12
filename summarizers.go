package jtl

import stats "github.com/blorticus-go/statistics"

type SummaryStatistics interface {
	Mean() float64
	Median() float64
	Maximum() float64
	Minimum() float64
	PopulationStandardDeviation() float64
	ValueAt5thPercentile() float64
	ValueAt95thPercentile() float64
}

type SummaryStatisticsForFloat64DataSet struct {
	mean                        float64
	median                      float64
	maximum                     float64
	minimum                     float64
	populationStandardDeviation float64
	valueAt5thPercentile        float64
	valueAt95thPercentile       float64
	individualDataPoints        []float64
}

func (s *SummaryStatisticsForFloat64DataSet) Mean() float64 {
	return s.mean
}

func (s *SummaryStatisticsForFloat64DataSet) Median() float64 {
	return s.median
}

func (s *SummaryStatisticsForFloat64DataSet) Maximum() float64 {
	return s.maximum
}

func (s *SummaryStatisticsForFloat64DataSet) Minimum() float64 {
	return s.minimum
}

func (s *SummaryStatisticsForFloat64DataSet) PopulationStandardDeviation() float64 {
	return s.populationStandardDeviation
}

func (s *SummaryStatisticsForFloat64DataSet) ValueAt5thPercentile() float64 {
	return s.valueAt5thPercentile
}

func (s *SummaryStatisticsForFloat64DataSet) ValueAt95thPercentile() float64 {
	return s.valueAt95thPercentile
}

func (s *SummaryStatisticsForFloat64DataSet) computeStatsFromRawDataSet() error {
	if len(s.individualDataPoints) == 0 {
		s.mean, s.median, s.maximum, s.minimum, s.populationStandardDeviation = 0, 0, 0, 0, 0
		return nil
	}

	sampleSet, err := stats.MakeStatisticalSampleSetFrom(s.individualDataPoints)
	if err != nil {
		return err
	}

	s.mean, s.median, s.maximum, s.minimum, s.populationStandardDeviation = sampleSet.Mean(), sampleSet.Median(), sampleSet.Maximum(), sampleSet.Minimum(), sampleSet.PopulationStdev()

	return nil
}

type SummaryStatisticsForTPSData struct {
	mean                               float64
	median                             float64
	minimum                            float64
	maximum                            float64
	populationStandardDeviation        float64
	valueAt5thPercentile               float64
	valueAt95thPercentile              float64
	countOfSamplesByOneSecondTimestamp map[uint64]uint64
}

func (s *SummaryStatisticsForTPSData) Mean() float64 {
	return s.mean
}

func (s *SummaryStatisticsForTPSData) Median() float64 {
	return s.median
}

func (s *SummaryStatisticsForTPSData) Maximum() float64 {
	return s.maximum
}

func (s *SummaryStatisticsForTPSData) Minimum() float64 {
	return s.minimum
}

func (s *SummaryStatisticsForTPSData) PopulationStandardDeviation() float64 {
	return s.populationStandardDeviation
}

func (s *SummaryStatisticsForTPSData) ValueAt5thPercentile() float64 {
	return s.valueAt5thPercentile
}

func (s *SummaryStatisticsForTPSData) ValueAt95thPercentile() float64 {
	return s.valueAt95thPercentile
}

func (s *SummaryStatisticsForTPSData) computeStatsFromRawDataSet(unixEpochSecondsAtTestStart uint64, unixEpochSecondsAtEndOfTest uint64) error {
	if len(s.countOfSamplesByOneSecondTimestamp) == 0 || int64(unixEpochSecondsAtEndOfTest)-int64(unixEpochSecondsAtTestStart) <= 0 {
		s.mean, s.median, s.maximum, s.minimum, s.populationStandardDeviation = 0, 0, 0, 0, 0
		return nil
	}

	tpsAtEachSecondInterval := make([]float64, unixEpochSecondsAtEndOfTest-unixEpochSecondsAtTestStart+1)

	timestamp := unixEpochSecondsAtTestStart
	for i := 0; timestamp < unixEpochSecondsAtEndOfTest; i++ {
		tpsAtEachSecondInterval[i] = float64(s.countOfSamplesByOneSecondTimestamp[timestamp])
		timestamp++
	}

	sampleSet, err := stats.MakeStatisticalSampleSetFrom(tpsAtEachSecondInterval)
	if err != nil {
		return err
	}

	s.mean, s.median, s.maximum, s.minimum, s.populationStandardDeviation = sampleSet.Mean(), sampleSet.Median(), sampleSet.Maximum(), sampleSet.Minimum(), sampleSet.PopulationStdev()

	return nil
}

type SummaryInformation struct {
	TotalNumberOfRequests     uint
	NumberOfSuccssfulRequests int // -1 if cannot be computed
}

type SummaryContainer struct {
	Information                       *SummaryInformation
	TimeToFirstBytes                  *SummaryStatisticsForFloat64DataSet
	TimeToLastByte                    *SummaryStatisticsForFloat64DataSet
	MovingWindowTransactionsPerSecond *SummaryStatisticsForTPSData
}

type AllSummarizedDimensions struct {
	Overall      *SummaryContainer
	MovingTPS    *SummaryContainer
	RequestSize  map[uint64]*SummaryContainer
	ResponseSize map[uint64]*SummaryContainer
	RequestURL   map[string]*SummaryContainer
}
