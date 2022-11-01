package jtl

import (
	"sort"
	"strconv"

	stats "github.com/blorticus-go/statistics"
)

// ColumnUniqueValueSummary provides information about keys from a column type.  The Key matches the appropriate type
// as found in a DataRow.
type ColumnUniqueValueSummary struct {
	Column                     ColumnType
	Key                        interface{}
	NumberOfMatchingRequests   uint
	NumberOfSuccessfulRequests int64 // -1 if this dimension is not possible
	TimeToFirstByteStatistics  *SummaryStatistics
	TimeToLastByteStatistics   *SummaryStatistics

	keyAsAString string
}

// KeyAsAString produces a stringified version of the typed Key.
func (summary *ColumnUniqueValueSummary) KeyAsAString() string {
	return summary.keyAsAString
}

// AggregateSummary provides aggregate summary information.
type AggregateSummary struct {
	TimestampOfFirstDataEntryAsUnixEpochMs uint64
	TimestampOfLastDataEntryAsUnixEpochMs  uint64
	AverageTPSRate                         float64
	NumberOfMatchingRequests               uint
	NumberOfSuccessfulRequests             int64 // -1 if this dimension is not possible
	TimeToFirstByteStatistics              *SummaryStatistics
	TimeToLastByteStatistics               *SummaryStatistics
}

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

func SummaryStatisticsFromDataSeries(series []float64) (*SummaryStatistics, error) {
	if len(series) == 0 {
		return nil, nil
	}

	s, err := stats.MakeStatisticalSampleSetFrom(series)
	if err != nil {
		return nil, err
	}

	return &SummaryStatistics{
		Mean:                        s.Mean(),
		Median:                      s.Median(),
		Maximum:                     s.Maximum(),
		Minimum:                     s.Minimum(),
		PopulationStandardDeviation: s.PopulationStdev(),
		ValueAt5thPercentile:        s.ValueNearestPercentile(5),
		ValueAt95thPercentile:       s.ValueNearestPercentile(95),
	}, nil
}

// Summarizer consumes a DataSource and when requested, generates Summary data based on all JTL data rows,
// and optionally, based on keys for individual columns.
type Summarizer struct {
	dataSource                  DataSource
	uniqueColumnValuesSummaries map[ColumnType][]*ColumnUniqueValueSummary
	metaColumnSummaries         map[ColumnType]*SummaryStatistics
	aggregateSummary            *AggregateSummary
}

// NewSummarizerForDataSource returns a new Summarizer for the attached DataSource.
func NewSummarizerForDataSource(dataSource DataSource) *Summarizer {
	return &Summarizer{
		dataSource:                  dataSource,
		uniqueColumnValuesSummaries: make(map[ColumnType][]*ColumnUniqueValueSummary),
		metaColumnSummaries:         make(map[ColumnType]*SummaryStatistics),
		aggregateSummary:            nil,
	}
}

// PreComputeAggregateSummaryAndSummariesForColumns walks through the DataSource rows and creates Summary entries
// for all rows (the Aggregate), as well as for each unique value from each identified column.  If a column is
// not present in the DataSource, it is ignored.  If a column or the aggregate is pre-computed, then a call
// for a Summary of that type (via AggregateSummary(), SummariesForTheColumn() and SortedSummariesForTheColumn())
// will not require the matching Summaries to get generated.  If it is not done here, then the individual
// Summary/Summaries calls will require the Summarizer to walk through the DataSource the first time a column
// (and the aggregate) Summary/Summaries method is called.  By pre-computing, only one walk through the DataSource
// is necessary.
func (summarizer *Summarizer) PreComputeAggregateSummaryAndSummariesForColumns(columns ...ColumnType) error {
	tracker := newUniqueColumnValueTracker(summarizer.dataSource)

	iteratingTrackerUpdater := NewSummarizationIterator(tracker).
		WhichShouldSummarizeStatsFor().
		AggregateData().
		TheColumns(columns...)

	iteratingTrackerUpdater.ProcessAllRowsInTheDatasource(summarizer.dataSource)

	var err error

	if summarizer.aggregateSummary, err = summarizer.extractAggregateDataSummaryFromTracker(tracker); err != nil {
		return err
	}

	if summarizer.uniqueColumnValuesSummaries, err = summarizer.convertTrackerDataToMapOfUniqueColumnValues(tracker); err != nil {
		return err
	}

	if summarizer.metaColumnSummaries, err = summarizer.convertTrackerDataToMapOfMetaColumnSummaries(tracker); err != nil {
		return err
	}

	return nil
}

// AggregateSummary returns a Summary for all rows in the DataSource
func (summarizer *Summarizer) AggregateSummary() (*AggregateSummary, error) {
	if summarizer.aggregateSummary == nil {
		tracker := newUniqueColumnValueTracker(summarizer.dataSource)

		iteratingTrackerUpdater := NewSummarizationIterator(tracker).
			WhichShouldSummarizeStatsFor().
			AggregateData()

		iteratingTrackerUpdater.ProcessAllRowsInTheDatasource(summarizer.dataSource)

		aggregateSummary, err := summarizer.extractAggregateDataSummaryFromTracker(tracker)
		if err != nil {
			return nil, err
		}

		summarizer.aggregateSummary = aggregateSummary
	}

	return summarizer.aggregateSummary, nil
}

// SummariesForTheColumn returns a Summary list, with one element for each unique value in the named column.
// The Summary applies only to the rows with a column that matches the Summary.Key.  Use SummariesForTheMetaColumn
// to retrieve summaries for a MetaColumn.
func (summarizer *Summarizer) SummariesForTheColumn(column ColumnType) ([]*ColumnUniqueValueSummary, error) {
	if summarizer.dataSource.DoesNotHaveTheColumn(column) {
		return nil, nil
	}

	if summarizer.uniqueColumnValuesSummaries[column] == nil {
		tracker := newUniqueColumnValueTracker(summarizer.dataSource)

		iteratingTrackerUpdater := NewSummarizationIterator(tracker).
			WhichShouldSummarizeStatsFor().
			TheColumns(column)

		iteratingTrackerUpdater.ProcessAllRowsInTheDatasource(summarizer.dataSource)

		mapOfUniqueColumnValuesSummaries, err := summarizer.convertTrackerDataToMapOfUniqueColumnValues(tracker)
		if err != nil {
			return nil, err
		}

		summarizer.uniqueColumnValuesSummaries[column] = mapOfUniqueColumnValuesSummaries[column]
	}

	return summarizer.uniqueColumnValuesSummaries[column], nil
}

// SummariesForTheMetaColumn returns a StatisticSummary for the named meta column.
func (summarizer *Summarizer) SummaryForTheMetaColumn(column ColumnType) (*SummaryStatistics, error) {
	if summarizer.dataSource.DoesNotHaveTheColumnsNecessaryFor(column) {
		return nil, nil
	}

	if summarizer.metaColumnSummaries[column] == nil {
		tracker := newUniqueColumnValueTracker(summarizer.dataSource)

		iteratingTrackerUpdater := NewSummarizationIterator(tracker).
			WhichShouldSummarizeStatsFor().
			TheColumns(column)

		iteratingTrackerUpdater.ProcessAllRowsInTheDatasource(summarizer.dataSource)

		mapOfMetaColumnSummaries, err := summarizer.convertTrackerDataToMapOfMetaColumnSummaries(tracker)
		if err != nil {
			return nil, err
		}

		summarizer.metaColumnSummaries = mapOfMetaColumnSummaries
	}

	return summarizer.metaColumnSummaries[column], nil
}

func (summarizer *Summarizer) extractAggregateDataSummaryFromTracker(tracker *uniqueColumnValueTracker) (*AggregateSummary, error) {
	aggregateSummary := &AggregateSummary{
		NumberOfMatchingRequests: tracker.aggregateData.numberOfRequests,
	}

	dataRows := summarizer.dataSource.Rows()

	if len(dataRows) == 0 {
		aggregateSummary.AverageTPSRate = 0
		aggregateSummary.TimestampOfFirstDataEntryAsUnixEpochMs = 0
		aggregateSummary.TimestampOfLastDataEntryAsUnixEpochMs = 0
	} else {
		timestampOfFirstEntry := dataRows[0].TimestampAsUnixEpochMs
		timestampOfLastEntry := dataRows[len(dataRows)-1].TimestampAsUnixEpochMs
		aggregateSummary.TimestampOfFirstDataEntryAsUnixEpochMs = timestampOfFirstEntry
		aggregateSummary.TimestampOfLastDataEntryAsUnixEpochMs = timestampOfLastEntry

		timespanOfDataRowsInSeconds := (timestampOfLastEntry - timestampOfFirstEntry) / 1000
		if timespanOfDataRowsInSeconds <= 0 {
			aggregateSummary.AverageTPSRate = 0
		} else {
			aggregateSummary.AverageTPSRate = float64(tracker.aggregateData.numberOfRequests) / float64(timespanOfDataRowsInSeconds)
		}
	}

	if ttfbSummary, err := SummaryStatisticsFromDataSeries(tracker.aggregateData.timeToFirstByteSet); err != nil {
		return nil, err
	} else {
		aggregateSummary.TimeToFirstByteStatistics = ttfbSummary
	}

	if ttlbSummary, err := SummaryStatisticsFromDataSeries(tracker.aggregateData.timeToLastByteSet); err != nil {
		return nil, err
	} else {
		aggregateSummary.TimeToLastByteStatistics = ttlbSummary
	}

	if summarizer.dataSource.HasTheColumn(Column.RequestWasSuccesful) {
		aggregateSummary.NumberOfSuccessfulRequests = int64(tracker.aggregateData.numberOfSuccessfulRequests)
	} else {
		aggregateSummary.NumberOfSuccessfulRequests = -1
	}

	return aggregateSummary, nil
}

func (summarizer *Summarizer) convertTrackerDataToMapOfUniqueColumnValues(tracker *uniqueColumnValueTracker) (map[ColumnType][]*ColumnUniqueValueSummary, error) {
	summaryMap := make(map[ColumnType][]*ColumnUniqueValueSummary)

	for _, collectedDataForColumnAndValue := range tracker.DataForCollectedUniqueColumnValues() {
		summary := &ColumnUniqueValueSummary{
			Column:                   collectedDataForColumnAndValue.Column,
			Key:                      collectedDataForColumnAndValue.UniqueValueForColumn,
			NumberOfMatchingRequests: collectedDataForColumnAndValue.Collector.numberOfRequests,
		}

		switch t := summary.Key.(type) {
		case string:
			summary.keyAsAString = t
		case uint64:
			summary.keyAsAString = strconv.FormatUint(t, 10)
		case int:
			summary.keyAsAString = strconv.FormatInt(int64(t), 10)
		case bool:
			summary.keyAsAString = strconv.FormatBool(t)
		}

		if ttfbSummary, err := SummaryStatisticsFromDataSeries(collectedDataForColumnAndValue.Collector.timeToFirstByteSet); err != nil {
			return nil, err
		} else {
			summary.TimeToFirstByteStatistics = ttfbSummary
		}

		if ttlbSummary, err := SummaryStatisticsFromDataSeries(collectedDataForColumnAndValue.Collector.timeToLastByteSet); err != nil {
			return nil, err
		} else {
			summary.TimeToLastByteStatistics = ttlbSummary
		}

		if summarizer.dataSource.HasTheColumn(Column.RequestWasSuccesful) {
			summary.NumberOfSuccessfulRequests = int64(collectedDataForColumnAndValue.Collector.numberOfSuccessfulRequests)
		} else {
			summary.NumberOfSuccessfulRequests = -1
		}

		summaryMap[collectedDataForColumnAndValue.Column] = append(summaryMap[collectedDataForColumnAndValue.Column], summary)
	}

	return summaryMap, nil
}

func (summarizer *Summarizer) convertTrackerDataToMapOfMetaColumnSummaries(tracker *uniqueColumnValueTracker) (map[ColumnType]*SummaryStatistics, error) {
	mapOfResponseCountByTimestamp := tracker.TransactionsPerSecond()

	if mapOfResponseCountByTimestamp != nil {
		dataSet := make([]float64, 0, len(mapOfResponseCountByTimestamp))

		allAccountedTimestamps := Keys(mapOfResponseCountByTimestamp)
		sort.Slice(allAccountedTimestamps, func(i, j int) bool { return allAccountedTimestamps[i] < allAccountedTimestamps[j] })

		if len(allAccountedTimestamps) == 0 {
			return nil, nil
		}

		dataSet = append(dataSet, float64(mapOfResponseCountByTimestamp[allAccountedTimestamps[0]]))
		lastProcessedTimestamp := allAccountedTimestamps[0]

		for _, nextTimestampInKeySet := range allAccountedTimestamps[1:] {
			// one second intervals without a value are implicitly zero transactions in that one second period
			for ts := lastProcessedTimestamp + 1; ts < nextTimestampInKeySet; ts++ {
				dataSet = append(dataSet, 0)
			}

			dataSet = append(dataSet, float64(mapOfResponseCountByTimestamp[nextTimestampInKeySet]))
			lastProcessedTimestamp = nextTimestampInKeySet
		}

		summaryStats, err := SummaryStatisticsFromDataSeries(dataSet)
		if err != nil {
			return nil, err
		}

		return map[ColumnType]*SummaryStatistics{
			MetaColumn.MovingTransactionsPerSecond: summaryStats,
		}, nil
	}

	return nil, nil
}

func Keys[M ~map[K]V, K comparable, V any](m M) []K {
	r := make([]K, len(m))
	i := 0
	for k := range m {
		r[i] = k
		i++
	}

	return r
}

type dataCollector struct {
	numberOfRequests           uint
	numberOfSuccessfulRequests uint
	timeToFirstByteSet         []float64
	timeToLastByteSet          []float64
}

func (c *dataCollector) AddDataFromRow(row *DataRow, columnsInRowInclude *TableOfColumns) *dataCollector {
	c.numberOfRequests++

	if columnsInRowInclude.SuccessFlag {
		c.numberOfSuccessfulRequests++
	}

	if columnsInRowInclude.TimeToFirstByte {
		c.timeToFirstByteSet = append(c.timeToFirstByteSet, float64(row.TimeToFirstByte))
	}

	if columnsInRowInclude.TimeToLastByte {
		c.timeToLastByteSet = append(c.timeToLastByteSet, float64(row.TimeToLastByte))
	}

	return c
}

type uniqueColumnValueTracker struct {
	columnsInDataSource                *TableOfColumns
	collectedDataForUniqueColumnValues map[ColumnType]map[interface{}]*dataCollector
	aggregateData                      *dataCollector
	mapOfResponsesReceivedEachSecond   map[uint64]uint
}

func newUniqueColumnValueTracker(forDataSource DataSource) *uniqueColumnValueTracker {
	allColumnsInDataSource := forDataSource.AllAvailableColumns()

	return &uniqueColumnValueTracker{
		columnsInDataSource:                &allColumnsInDataSource,
		collectedDataForUniqueColumnValues: make(map[ColumnType]map[interface{}]*dataCollector),
		aggregateData:                      new(dataCollector),
		mapOfResponsesReceivedEachSecond:   make(map[uint64]uint),
	}
}

func (u *uniqueColumnValueTracker) AddRowStatsToColumnAndValue(column ColumnType, columnValue interface{}, row *DataRow) {
	if u.columnsInDataSource.IncludesTheColumn(column) {
		if u.collectedDataForUniqueColumnValues[column] == nil {
			u.collectedDataForUniqueColumnValues[column] = make(map[interface{}]*dataCollector)
		}

		if u.collectedDataForUniqueColumnValues[column][columnValue] == nil {
			u.collectedDataForUniqueColumnValues[column][columnValue] = new(dataCollector).AddDataFromRow(row, u.columnsInDataSource)
		} else {
			u.collectedDataForUniqueColumnValues[column][columnValue].AddDataFromRow(row, u.columnsInDataSource)
		}
	}
}

func (u *uniqueColumnValueTracker) AddRowStatsToAggregate(row *DataRow) {
	u.aggregateData.AddDataFromRow(row, u.columnsInDataSource)
}

func (u *uniqueColumnValueTracker) IncrementTransactionCounterForSecondTimestamp(timestamp uint64) {
	u.mapOfResponsesReceivedEachSecond[timestamp]++
}

type uniqueColumnValueCollectedData struct {
	Column               ColumnType
	UniqueValueForColumn interface{}
	Collector            *dataCollector
}

func (u *uniqueColumnValueTracker) DataForCollectedUniqueColumnValues() []*uniqueColumnValueCollectedData {
	d := make([]*uniqueColumnValueCollectedData, 0, 10)

	for column, uniqueValueMap := range u.collectedDataForUniqueColumnValues {
		for uniqueValue, valueCollector := range uniqueValueMap {
			d = append(d, &uniqueColumnValueCollectedData{column, uniqueValue, valueCollector})
		}
	}

	return d
}

func (u *uniqueColumnValueTracker) CollectedAggregateData() *dataCollector {
	return u.aggregateData
}

func (u *uniqueColumnValueTracker) TransactionsPerSecond() map[uint64]uint {
	return u.mapOfResponsesReceivedEachSecond
}

type rowElementProcessor func(tracker *uniqueColumnValueTracker, row *DataRow)

type summarizationIterator struct {
	tracker                    *uniqueColumnValueTracker
	processorsForEachRowOfData []rowElementProcessor
}

func NewSummarizationIterator(trackerToWhichStatsShouldBeAdded *uniqueColumnValueTracker) *summarizationIterator {
	return &summarizationIterator{
		tracker:                    trackerToWhichStatsShouldBeAdded,
		processorsForEachRowOfData: make([]rowElementProcessor, 0, 10),
	}
}

func (iterator *summarizationIterator) WhichShouldSummarizeStatsFor() *summarizationIterator {
	return iterator
}

func (iterator *summarizationIterator) ShouldSummarizeStatsFor() *summarizationIterator {
	return iterator
}

func (iterator *summarizationIterator) AggregateData() *summarizationIterator {
	iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
		tracker.AddRowStatsToAggregate(row)
	})

	return iterator
}

func (iterator *summarizationIterator) TheColumns(columnsThatShouldBeProcessed ...ColumnType) *summarizationIterator {
	for _, column := range columnsThatShouldBeProcessed {
		switch column {
		case Column.AllThreads:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.AllThreads, row.AllThreads, row)
			})
		case Column.ConnectTime:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.ConnectTime, row.ConnectTimeInMs, row)
			})
		case Column.DataType:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.DataType, row.DataType, row)
			})
		case Column.FailureMessage:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.FailureMessage, row.FailureMessage, row)
			})
		case Column.GroupThreads:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.GroupThreads, row.GroupThreads, row)
			})
		case Column.IdleTime:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.IdleTime, row.IdleTimeInMs, row)
			})
		case Column.RequestBodySizeInBytes:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.RequestBodySizeInBytes, row.RequestBodySizeInBytes, row)
			})
		case Column.RequestURL:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.RequestURL, row.RequestURLString, row)
			})
		case Column.RequestWasSuccesful:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.RequestWasSuccesful, row.RequestWasSuccessful, row)
			})
		case Column.ResponseBytesReceived:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.ResponseBytesReceived, row.ResponseBytesReceived, row)
			})
		case Column.ResponseCodeOrErrorMessage:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.ResponseCodeOrErrorMessage, row.ResponseCodeOrErrorMessage, row)
			})
		case Column.ResponseMessage:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.ResponseMessage, row.ResponseMessage, row)
			})
		case Column.ResultLabel:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.ResultLabel, row.SampleResultLabel, row)
			})
		case Column.ThreadName:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.ThreadName, row.ThreadNameText, row)
			})
		case Column.TimeToFirstByte:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.TimeToFirstByte, row.TimeToFirstByte, row)
			})
		case Column.TimeToLastByte:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.TimeToLastByte, row.TimeToLastByte, row)
			})
		case Column.Timestamp:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.AddRowStatsToColumnAndValue(Column.Timestamp, row.TimestampAsUnixEpochMs, row)
			})
		case MetaColumn.MovingTransactionsPerSecond:
			iterator.processorsForEachRowOfData = append(iterator.processorsForEachRowOfData, func(tracker *uniqueColumnValueTracker, row *DataRow) {
				tracker.IncrementTransactionCounterForSecondTimestamp(row.TimestampAsUnixEpochMs / 1000)
			})
		}
	}

	return iterator
}

func (iterator *summarizationIterator) ProcessTheRow(row *DataRow) {
	for _, processor := range iterator.processorsForEachRowOfData {
		processor(iterator.tracker, row)
	}
}

func (iterator *summarizationIterator) ProcessAllRowsInTheDatasource(source DataSource) {
	for _, row := range source.Rows() {
		iterator.ProcessTheRow(row)
	}
}
