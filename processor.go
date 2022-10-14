package jtl

// import (
// 	"io"
// 	"log"
// 	"sort"

// 	stats "github.com/blorticus-go/statistics"
// )

// // import (
// // 	"bufio"
// // 	"fmt"
// // 	"io"
// // 	"strconv"
// // 	"strings"
// // )

// // LoggerSet contains two loggers: one for detailed, verbose output, and the other for Informational output.
// // If a logger is set to nil, that type of output won't be generated.
// type LoggerSet struct {
// 	VerboseLogging       *log.Logger
// 	InformationalLogging *log.Logger
// }

// // Processor is an entity that consumes a JTL data source and can perform analysis of the data in the source.
// type Processor struct {
// 	loggerSet                            *LoggerSet
// 	columnTypesInOrder                   []ColumnType
// 	columnsInDataSourceLookupTable       *TableOfColumns
// 	dataRows                             []*DataRow
// 	mapOfCategoryToInOrderCategoryAndKey map[SummarizableCategory][]*SummarizedCategoryAndKey
// }

// // NewProcessor creates a new, empty Processor.
// func NewProcessor() *Processor {
// 	return &Processor{
// 		loggerSet: nil,
// 	}
// }

// // UsingLogger attaches a LoggerSet to the Processor.
// func (processor *Processor) UsingLoggers(loggerSet *LoggerSet) *Processor {
// 	processor.loggerSet = loggerSet
// 	return processor
// }

// // PopulateFromCsvData consumes JTL in CSV format and sets that as the data source for the Processor methods.
// // The first line must be a JTL CSV header.  For each subsequent row, the number of columns must match the
// // number of header columns.
// func (processor *Processor) PopulateFromCsvData(reader io.Reader) (*Processor, error) {
// 	// scanner := bufio.NewScanner(reader)

// 	// var err error

// 	// if !scanner.Scan() {
// 	// 	if scanner.Err() != nil {
// 	// 		return nil, scanner.Err()
// 	// 	}
// 	// 	return nil, fmt.Errorf("no header found")
// 	// }

// 	// processor.columnTypesInOrder, processor.columnsInDataSourceLookupTable, err = extractDataSourceColumnsFromCsvHeader(scanner.Text())
// 	// if err != nil {
// 	// 	return nil, err
// 	// }

// 	// processor.dataRows = make([]*DataRow, 0, 100)

// 	// for scanner.Scan() {
// 	// 	dataRow, err := extractDataFromCsvRow(scanner.Text(), &processor.columnTypesInOrder)
// 	// 	if err != nil {
// 	// 		return nil, err
// 	// 	}
// 	// 	processor.dataRows = append(processor.dataRows, dataRow)
// 	// }

// 	// if scanner.Err() != nil {
// 	// 	return nil, scanner.Err()
// 	// }

// 	return processor, nil
// }

// // SummarizationIteratorFor returns a SummerizationIterator, which can be used to walk through SummaryData available
// // from the data source.  The dimensions describe the types
// func (processor *Processor) SummarizationIteratorFor(dimensions ...SummarizableCategory) (*SummarizationIterator, error) {
// 	if processor.mapOfCategoryToInOrderCategoryAndKey == nil {
// 		m, err := generateAllPossibleSummariesFromDataRows(processor.dataRows, processor.columnsInDataSourceLookupTable)
// 		if err != nil {
// 			return nil, err
// 		}
// 		processor.mapOfCategoryToInOrderCategoryAndKey = m
// 	}

// 	summarizations := make([]*SummarizedCategoryAndKey, 0, 10)

// 	for _, dimension := range dimensions {
// 		summarizations = append(summarizations, processor.mapOfCategoryToInOrderCategoryAndKey[dimension]...)
// 	}

// 	return newSummarizationIterator(summarizations), nil
// }

// // DataColumns returns the data columns present in the data source in the same order that they were presented (e.g., for CSV
// // data source, it is the same order as the header column names).
// func (processor *Processor) DataColumns() []ColumnType {
// 	return processor.columnTypesInOrder
// }

// // DataRows returns each row of data from the data source as a standard struct in the same order as they are found in the
// // data source.
// func (processor *Processor) DataRows() []*DataRow {
// 	return processor.dataRows
// }

// // func extractDataSourceColumnsFromCsvHeader(headerLineWithoutNewline string) ([]ColumnType, *tableOfColumnsInDataSource, error) {
// // 	columnNamesAsStrings := strings.Split(headerLineWithoutNewline, ",")
// // 	if len(columnNamesAsStrings) == 0 {
// // 		return nil, nil, fmt.Errorf("no header columns found")
// // 	}

// // 	columnTypesInOrder := make([]ColumnType, len(columnNamesAsStrings))

// // 	for i, headerName := range columnNamesAsStrings {
// // 		if columnType, columnNameIsInMap := CsvHeaderLabelToColumnType[headerName]; !columnNameIsInMap {
// // 			return nil, nil, fmt.Errorf("csv file header (%s) not understood", headerName)
// // 		} else {
// // 			columnTypesInOrder[i] = columnType
// // 		}
// // 	}

// // 	columnsLookupTable := columnLookupTableFromColumnTypeSet(columnTypesInOrder)

// // 	return columnTypesInOrder, columnsLookupTable, nil
// // }

// type aggregationContainer struct {
// 	RequestCount           uint
// 	SuccessfulRequestCount int64
// 	TimeToFirstByteSet     []float64
// 	TimeToLastByteSet      []float64

// 	columnsInDataSourceInclude *TableOfColumns
// }

// func newAggregationContainer(columnsInDataSourceInclude *TableOfColumns) *aggregationContainer {
// 	c := &aggregationContainer{
// 		columnsInDataSourceInclude: columnsInDataSourceInclude,
// 	}

// 	if columnsInDataSourceInclude.SuccessFlag {
// 		c.SuccessfulRequestCount = 0
// 	}

// 	if columnsInDataSourceInclude.TimeToFirstByte {
// 		c.TimeToFirstByteSet = make([]float64, 0, 100)
// 		c.TimeToLastByteSet = make([]float64, 0, 100)
// 	}

// 	return c
// }

// func (container *aggregationContainer) updateBasedOnDataRow(row *DataRow) *aggregationContainer {
// 	container.RequestCount++

// 	if container.columnsInDataSourceInclude.SuccessFlag {
// 		container.SuccessfulRequestCount++
// 	}

// 	if container.columnsInDataSourceInclude.TimeToFirstByte {
// 		container.TimeToFirstByteSet = append(container.TimeToFirstByteSet, float64(row.TimeToFirstByte))
// 	}

// 	if container.columnsInDataSourceInclude.TimeToLastByte {
// 		container.TimeToLastByteSet = append(container.TimeToFirstByteSet, float64(row.TimeToLastByte))
// 	}

// 	return container
// }

// type aggregationContainerFactory struct {
// 	columnsInDataSource *TableOfColumns
// }

// func (factory *aggregationContainerFactory) MakeContainer() *aggregationContainer {
// 	return newAggregationContainer(factory.columnsInDataSource)
// }

// func (factory *aggregationContainerFactory) NewContainerIfNil(container *aggregationContainer) *aggregationContainer {
// 	if container == nil {
// 		container = newAggregationContainer(factory.columnsInDataSource)
// 	}
// 	return container
// }

// func generateAllPossibleSummariesFromDataRows(dataRows []*DataRow, availableColumnsInclude *TableOfColumns) (mapOfCategoryAndKeySortedByKeyAscending map[SummarizableCategory][]*SummarizedCategoryAndKey, err error) {
// 	statsForAggregate := newAggregationContainer(availableColumnsInclude)

// 	statsForStringKeyedCategories := map[SummarizableCategory]map[string]*aggregationContainer{
// 		ErrorMessages: make(map[string]*aggregationContainer),
// 		RequestURLs:   make(map[string]*aggregationContainer),
// 		ResponseCodes: make(map[string]*aggregationContainer),
// 	}

// 	statsForInt64KeyedCategories := map[SummarizableCategory]map[int64]*aggregationContainer{
// 		RequestSizes:  make(map[int64]*aggregationContainer),
// 		ResponseSizes: make(map[int64]*aggregationContainer),
// 	}

// 	containerMaker := &aggregationContainerFactory{availableColumnsInclude}

// 	for _, row := range dataRows {
// 		statsForAggregate.updateBasedOnDataRow(row)

// 		if availableColumnsInclude.RequestURL {
// 			statsForStringKeyedCategories[RequestURLs][row.RequestURLString] = containerMaker.NewContainerIfNil(statsForStringKeyedCategories[RequestURLs][row.RequestURLString]).updateBasedOnDataRow(row)
// 		}

// 		if availableColumnsInclude.RequestBodySizeInBytes {
// 			statsForInt64KeyedCategories[RequestSizes][int64(row.RequestBodySizeInBytes)] = containerMaker.NewContainerIfNil(statsForInt64KeyedCategories[RequestSizes][int64(row.RequestBodySizeInBytes)]).updateBasedOnDataRow(row)
// 		}

// 		if availableColumnsInclude.ResponseBytesReceived {
// 			statsForInt64KeyedCategories[ResponseSizes][int64(row.ResponseBytesReceived)] = containerMaker.NewContainerIfNil(statsForInt64KeyedCategories[ResponseSizes][int64(row.ResponseBytesReceived)]).updateBasedOnDataRow(row)
// 		}

// 		if availableColumnsInclude.ResponseCodeOrErrorMessage {
// 			statsForStringKeyedCategories[ResponseCodes][row.ResponseCodeOrErrorMessage] = containerMaker.NewContainerIfNil(statsForStringKeyedCategories[ResponseCodes][row.ResponseCodeOrErrorMessage]).updateBasedOnDataRow(row)
// 		}

// 		if availableColumnsInclude.FailureMessage {
// 			statsForStringKeyedCategories[ErrorMessages][row.FailureMessage] = containerMaker.NewContainerIfNil(statsForStringKeyedCategories[ErrorMessages][row.FailureMessage]).updateBasedOnDataRow(row)
// 		}
// 	}

// 	mapOfCategoryAndKeySortedByKeyAscending = make(map[SummarizableCategory][]*SummarizedCategoryAndKey)

// 	for category, keyMap := range statsForStringKeyedCategories {
// 		if len(keyMap) > 0 {
// 			summarizedSet := make([]*SummarizedCategoryAndKey, len(keyMap))

// 			keySet := make([]string, 0, len(keyMap))
// 			for k := range keyMap {
// 				keySet = append(keySet, k)
// 			}
// 			sort.Strings(keySet)

// 			for i, sortedKey := range keySet {
// 				aggregationContainer := keyMap[sortedKey]

// 				timeToFirstByteStats, err := stats.MakeStatisticalSampleSetFrom(aggregationContainer.TimeToFirstByteSet)
// 				if err != nil {
// 					return nil, err
// 				}

// 				timeToLastByteStats, err := stats.MakeStatisticalSampleSetFrom(aggregationContainer.TimeToLastByteSet)
// 				if err != nil {
// 					return nil, err
// 				}

// 				summarizedSet[i] = &SummarizedCategoryAndKey{
// 					Category:                   category,
// 					Key:                        sortedKey,
// 					CountOfRequests:            aggregationContainer.RequestCount,
// 					NumberOfSuccessfulRequests: aggregationContainer.SuccessfulRequestCount,
// 					StatisticsFor: &SummaryDimensions{
// 						TimeToFirstByte: &SummaryStatistics{
// 							Mean:                        timeToFirstByteStats.Mean(),
// 							Median:                      timeToFirstByteStats.Median(),
// 							Maximum:                     timeToFirstByteStats.Maximum(),
// 							Minimum:                     timeToFirstByteStats.Minimum(),
// 							PopulationStandardDeviation: timeToFirstByteStats.PopulationStdev(),
// 							ValueAt5thPercentile:        timeToFirstByteStats.ValueNearestPercentile(5),
// 							ValueAt95thPercentile:       timeToFirstByteStats.ValueNearestPercentile(95),
// 						},
// 						TimeToLastByte: &SummaryStatistics{
// 							Mean:                        timeToLastByteStats.Mean(),
// 							Median:                      timeToLastByteStats.Median(),
// 							Maximum:                     timeToLastByteStats.Maximum(),
// 							Minimum:                     timeToLastByteStats.Minimum(),
// 							PopulationStandardDeviation: timeToLastByteStats.PopulationStdev(),
// 							ValueAt5thPercentile:        timeToLastByteStats.ValueNearestPercentile(5),
// 							ValueAt95thPercentile:       timeToLastByteStats.ValueNearestPercentile(95),
// 						},
// 					},
// 				}
// 			}

// 			mapOfCategoryAndKeySortedByKeyAscending[category] = summarizedSet
// 		}
// 	}

// 	return mapOfCategoryAndKeySortedByKeyAscending, nil
// }
