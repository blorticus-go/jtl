package jtl

import (
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
	AverageTPSRate                         uint64
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
	aggregateSummary            *AggregateSummary
}

// NewSummarizerForDataSource returns a new Summarizer for the attached DataSource.
func NewSummarizerForDataSource(dataSource DataSource) *Summarizer {
	return &Summarizer{
		dataSource: dataSource,
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
func (summarizer *Summarizer) PreComputeAggregateSummaryAndSummariesForColumns(column ...ColumnType) error {
	mapOfSummariesForUniqueColumnValues, aggregateSummary, err := summarizeFromADataSource(summarizer.dataSource, true, makeColumnLookupTableFromColumnTypeSet(column...))
	if err != nil {
		return err
	}

	summarizer.uniqueColumnValuesSummaries = mapOfSummariesForUniqueColumnValues
	summarizer.aggregateSummary = aggregateSummary

	return nil
}

// AggregateSummary returns a Summary for all rows in the DataSource
func (summarizer *Summarizer) AggregateSummary() (*AggregateSummary, error) {
	if summarizer.aggregateSummary == nil {
		if _, aggregateSummary, err := summarizeFromADataSource(summarizer.dataSource, true, &TableOfColumns{}); err != nil {
			return nil, err
		} else {
			summarizer.aggregateSummary = aggregateSummary
		}
	}

	return summarizer.aggregateSummary, nil
}

// SummariesForTheColumn returns a Summary list, with one element for each unique value in the named column.
// The Summary applies only to the row with a column that matches the Summary.Key.
func (summarizer *Summarizer) SummariesForTheColumn(column ColumnType) ([]*ColumnUniqueValueSummary, error) {
	if !summarizer.dataSource.HasTheColumn(column) {
		return nil, nil
	}

	if summarizer.uniqueColumnValuesSummaries[column] == nil {
		mapOfSummariesForUniqueColumnValues, _, err := summarizeFromADataSource(summarizer.dataSource, false, makeColumnLookupTableFromColumnTypeSet(column))
		if err != nil {
			return nil, err
		}

		summarizer.uniqueColumnValuesSummaries[column] = mapOfSummariesForUniqueColumnValues[column]
	}

	return summarizer.uniqueColumnValuesSummaries[column], nil
}

// SortedSummariesForTheColumn is the same as SummariesForColumn(), but it returns the Summary list sorted
// by Summary.Key in ascending order.  The sorting is type-appropriate.  That is, numbers are sorted
// in numerical order, while strings are sorted in lexical order.
func (summarizer *Summarizer) SortedSummariesForTheColumn(column ColumnType) ([]*ColumnUniqueValueSummary, error) {
	return nil, nil
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
}

func newUniqueColumnValueTracker(forDataSource DataSource) *uniqueColumnValueTracker {
	t := forDataSource.AllAvailableColumns()

	return &uniqueColumnValueTracker{
		columnsInDataSource:                &t,
		collectedDataForUniqueColumnValues: make(map[ColumnType]map[interface{}]*dataCollector),
		aggregateData:                      new(dataCollector),
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

func summarizeFromADataSource(dataSource DataSource, aggregateStatsShouldBeComputed bool, wantColumnStatsFor *TableOfColumns) (summaryForUniqueColumnValues map[ColumnType][]*ColumnUniqueValueSummary, aggregateSummary *AggregateSummary, err error) {
	tracker := newUniqueColumnValueTracker(dataSource)

	for _, row := range dataSource.Rows() {
		if aggregateStatsShouldBeComputed {
			tracker.AddRowStatsToAggregate(row)
		}

		if wantColumnStatsFor.AllThreads {
			tracker.AddRowStatsToColumnAndValue(Column.AllThreads, row.AllThreads, row)
		}
		if wantColumnStatsFor.ConnectTime {
			tracker.AddRowStatsToColumnAndValue(Column.ConnectTime, row.ConnectTimeInMs, row)
		}
		if wantColumnStatsFor.DataType {
			tracker.AddRowStatsToColumnAndValue(Column.DataType, row.DataType, row)
		}
		if wantColumnStatsFor.FailureMessage {
			tracker.AddRowStatsToColumnAndValue(Column.FailureMessage, row.FailureMessage, row)
		}
		if wantColumnStatsFor.GroupThreads {
			tracker.AddRowStatsToColumnAndValue(Column.GroupThreads, row.GroupThreads, row)
		}
		if wantColumnStatsFor.IdleTime {
			tracker.AddRowStatsToColumnAndValue(Column.IdleTime, row.IdleTimeInMs, row)
		}
		if wantColumnStatsFor.RequestBodySizeInBytes {
			tracker.AddRowStatsToColumnAndValue(Column.RequestBodySizeInBytes, row.RequestBodySizeInBytes, row)
		}
		if wantColumnStatsFor.RequestURL {
			tracker.AddRowStatsToColumnAndValue(Column.RequestURL, row.RequestURLString, row)
		}
		if wantColumnStatsFor.ResponseBytesReceived {
			tracker.AddRowStatsToColumnAndValue(Column.ResponseBytesReceived, row.ResponseBytesReceived, row)
		}
		if wantColumnStatsFor.ResponseCodeOrErrorMessage {
			tracker.AddRowStatsToColumnAndValue(Column.ResponseCodeOrErrorMessage, row.ResponseCodeOrErrorMessage, row)
		}
		if wantColumnStatsFor.ResponseMessage {
			tracker.AddRowStatsToColumnAndValue(Column.ResponseMessage, row.ResponseMessage, row)
		}
		if wantColumnStatsFor.ResultLabel {
			tracker.AddRowStatsToColumnAndValue(Column.ResultLabel, row.SampleResultLabel, row)
		}
		if wantColumnStatsFor.SuccessFlag {
			tracker.AddRowStatsToColumnAndValue(Column.RequestWasSuccesful, row.RequestWasSuccessful, row)
		}
		if wantColumnStatsFor.ThreadName {
			tracker.AddRowStatsToColumnAndValue(Column.ThreadName, row.ThreadNameText, row)
		}
		if wantColumnStatsFor.TimeToFirstByte {
			tracker.AddRowStatsToColumnAndValue(Column.TimeToFirstByte, row.TimeToFirstByte, row)
		}
		if wantColumnStatsFor.TimeToLastByte {
			tracker.AddRowStatsToColumnAndValue(Column.TimeToLastByte, row.TimeToLastByte, row)
		}
		if wantColumnStatsFor.TimestampAsUnixEpochMs {
			tracker.AddRowStatsToColumnAndValue(Column.Timestamp, row.TimestampAsUnixEpochMs, row)
		}
	}

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
			return nil, nil, err
		} else {
			summary.TimeToFirstByteStatistics = ttfbSummary
		}

		if ttlbSummary, err := SummaryStatisticsFromDataSeries(collectedDataForColumnAndValue.Collector.timeToLastByteSet); err != nil {
			return nil, nil, err
		} else {
			summary.TimeToLastByteStatistics = ttlbSummary
		}

		if dataSource.HasTheColumn(Column.RequestWasSuccesful) {
			summary.NumberOfSuccessfulRequests = int64(collectedDataForColumnAndValue.Collector.numberOfSuccessfulRequests)
		} else {
			summary.NumberOfSuccessfulRequests = -1
		}

		summaryMap[collectedDataForColumnAndValue.Column] = append(summaryMap[collectedDataForColumnAndValue.Column], summary)
	}

	if aggregateStatsShouldBeComputed {
		aggregateSummary := &AggregateSummary{
			NumberOfMatchingRequests: tracker.aggregateData.numberOfRequests,
		}

		dataRows := dataSource.Rows()

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
				aggregateSummary.AverageTPSRate = uint64(tracker.aggregateData.numberOfRequests) / timespanOfDataRowsInSeconds
			}
		}

		if ttfbSummary, err := SummaryStatisticsFromDataSeries(tracker.aggregateData.timeToFirstByteSet); err != nil {
			return nil, nil, err
		} else {
			aggregateSummary.TimeToFirstByteStatistics = ttfbSummary
		}

		if ttlbSummary, err := SummaryStatisticsFromDataSeries(tracker.aggregateData.timeToLastByteSet); err != nil {
			return nil, nil, err
		} else {
			aggregateSummary.TimeToLastByteStatistics = ttlbSummary
		}

		if dataSource.HasTheColumn(Column.RequestWasSuccesful) {
			aggregateSummary.NumberOfSuccessfulRequests = int64(tracker.aggregateData.numberOfSuccessfulRequests)
		} else {
			aggregateSummary.NumberOfSuccessfulRequests = -1
		}

		return summaryMap, aggregateSummary, nil
	}

	return summaryMap, nil, nil
}
