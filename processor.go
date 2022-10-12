package jtl

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Processor struct {
	dataRows                   []*DataRow
	columnsPresentInDataSource columnsPresent

	overallSummary          *SummaryContainer
	summaryByMovingTPS      *SummaryContainer
	summariesByRequestSize  map[uint64]*SummaryContainer
	summariesByResponseSize map[uint64]*SummaryContainer
	summariesByRequestURL   map[string]*SummaryContainer

	setOfAllSummarizers *AllSummarizedDimensions
}

func NewProcessorFromCsvData(reader io.Reader) (*Processor, error) {
	scanner := bufio.NewScanner(reader)

	if !scanner.Scan() {
		if scanner.Err() != nil {
			return nil, scanner.Err()
		}
		return nil, fmt.Errorf("no header found")
	}

	inOrderListOfColumnsByType, columnsPresentInHeaders, err := extractColumnTypesFromHeader(scanner.Text())
	if err != nil {
		return nil, err
	}

	dataRows := make([]*DataRow, 0, 100)

	for scanner.Scan() {
		dataRow, err := extractDataFromCsvRow(scanner.Text(), &inOrderListOfColumnsByType)
		if err != nil {
			return nil, err
		}
		dataRows = append(dataRows, dataRow)
	}

	return &Processor{
		dataRows:                   dataRows,
		columnsPresentInDataSource: *columnsPresentInHeaders,
	}, nil
}

func (processor *Processor) DataRows() []*DataRow {
	return processor.dataRows
}

type computableDimensions struct {
	SuccessCount        bool
	MovingTPSStatistics bool
	TTFBStatistics      bool
	TTLBStatistics      bool
}

type trackingKeys struct {
	RequestSize  bool
	ResponseSize bool
	ResponseCode bool
	URL          bool
}

func (processor *Processor) SummarizedData() (*AllSummarizedDimensions, error) {
	if processor.setOfAllSummarizers != nil {
		return processor.setOfAllSummarizers, nil
	}

	canCompute := &computableDimensions{
		SuccessCount:        processor.columnsPresentInDataSource.SuccessFlag,
		MovingTPSStatistics: processor.columnsPresentInDataSource.TimestampAsUnixEpochMs,
		TTFBStatistics:      processor.columnsPresentInDataSource.TimeToFirstByte,
		TTLBStatistics:      processor.columnsPresentInDataSource.TimeToLastByte,
	}

	canProvideInfoBy := &trackingKeys{
		RequestSize:  processor.columnsPresentInDataSource.RequestBodySizeInBytes,
		ResponseSize: processor.columnsPresentInDataSource.ResponseBytesReceived,
		ResponseCode: processor.columnsPresentInDataSource.ResponseCode,
		URL:          processor.columnsPresentInDataSource.RequestURLString,
	}

	overallSummaries := makeSummaryContainer(canCompute)

	summaryByRequestSize := make(map[int]*SummaryContainer)
	summaryByResponseSize := make(map[int]*SummaryContainer)
	summaryByResponseCode := make(map[int]*SummaryContainer)
	summaryByURL := make(map[string]*SummaryContainer)

	var requestSizeSummaryElement, responseSizeSummaryElement, responseCodeSummary, urlSummaryElement *SummaryContainer
	var dimensionIsAlreadyInTheMap bool

	for _, row := range processor.dataRows {
		updateElementBasedOnDataRow(row, canCompute, overallSummaries)

		if canProvideInfoBy.RequestSize {
			if requestSizeSummaryElement, dimensionIsAlreadyInTheMap = summaryByRequestSize[row.RequestBodySizeInBytes]; !dimensionIsAlreadyInTheMap {
				requestSizeSummaryElement = makeSummaryContainer(canCompute)
				summaryByRequestSize[row.RequestBodySizeInBytes] = requestSizeSummaryElement
			}

			updateElementBasedOnDataRow(row, canCompute, requestSizeSummaryElement)
		}

		if canProvideInfoBy.ResponseSize {
			if responseSizeSummaryElement, dimensionIsAlreadyInTheMap = summaryByResponseSize[row.ResponseBytesReceived]; !dimensionIsAlreadyInTheMap {
				responseSizeSummaryElement = makeSummaryContainer(canCompute)
				summaryByResponseSize[row.ResponseBytesReceived] = responseSizeSummaryElement
			}

			updateElementBasedOnDataRow(row, canCompute, responseSizeSummaryElement)
		}

		if canProvideInfoBy.ResponseCode {
			if responseCodeSummary, dimensionIsAlreadyInTheMap = summaryByResponseCode[row.ResponseCode]; !dimensionIsAlreadyInTheMap {
				responseCodeSummary = makeSummaryContainer(canCompute)
				summaryByResponseCode[row.ResponseCode] = responseCodeSummary
			}

			updateElementBasedOnDataRow(row, canCompute, responseCodeSummary)
		}

		if canProvideInfoBy.URL {
			if urlSummaryElement, dimensionIsAlreadyInTheMap = summaryByURL[row.RequestURLString]; !dimensionIsAlreadyInTheMap {
				urlSummaryElement = makeSummaryContainer(canCompute)
				summaryByURL[row.RequestURLString] = urlSummaryElement
			}

			updateElementBasedOnDataRow(row, canCompute, urlSummaryElement)
		}
	}

	processor.setOfAllSummarizers = &AllSummarizedDimensions{
		Overall: processor.overallSummary,
	}

	if canProvideInfoBy.RequestSize {
		processor.setOfAllSummarizers.RequestSize = processor.summariesByRequestSize
	}

	if canProvideInfoBy.ResponseSize {
		processor.setOfAllSummarizers.ResponseSize = processor.summariesByResponseSize
	}

	if canProvideInfoBy.URL {
		processor.setOfAllSummarizers.RequestURL = processor.summariesByRequestURL
	}

	return processor.setOfAllSummarizers, nil
}

func updateElementBasedOnDataRow(row *DataRow, canCompute *computableDimensions, container *SummaryContainer) {
	container.Information.TotalNumberOfRequests++

	if canCompute.SuccessCount {
		if row.RequestWasSuccessful {
			container.Information.NumberOfSuccssfulRequests++
		}
	}

	if canCompute.TTFBStatistics {
		container.TimeToFirstBytes.individualDataPoints = append(container.TimeToFirstBytes.individualDataPoints, float64(row.TimeToFirstByte))
	}

	if canCompute.TTLBStatistics {
		container.TimeToLastByte.individualDataPoints = append(container.TimeToFirstBytes.individualDataPoints, float64(row.TimeToLastByte))
	}

	if canCompute.MovingTPSStatistics {
		timestampAsUnixEpochSeconds := row.TimestampAsUnixEpochMs / 1000
		if count, thisTimestampIsInTheMap := container.MovingWindowTransactionsPerSecond.countOfSamplesByOneSecondTimestamp[timestampAsUnixEpochSeconds]; !thisTimestampIsInTheMap {
			container.MovingWindowTransactionsPerSecond.countOfSamplesByOneSecondTimestamp[timestampAsUnixEpochSeconds] = 1
		} else {
			container.MovingWindowTransactionsPerSecond.countOfSamplesByOneSecondTimestamp[timestampAsUnixEpochSeconds] = count + 1
		}
	}

}

func makeSummaryContainer(thingsThatCanBeComputedInclude *computableDimensions) *SummaryContainer {
	container := &SummaryContainer{
		Information: &SummaryInformation{
			TotalNumberOfRequests:     0,
			NumberOfSuccssfulRequests: -1,
		},
	}

	if thingsThatCanBeComputedInclude.SuccessCount {
		container.Information.NumberOfSuccssfulRequests = 0
	}

	if thingsThatCanBeComputedInclude.MovingTPSStatistics {
		container.MovingWindowTransactionsPerSecond = &SummaryStatisticsForTPSData{
			countOfSamplesByOneSecondTimestamp: make(map[uint64]uint64),
		}
	}

	if thingsThatCanBeComputedInclude.TTFBStatistics {
		container.TimeToFirstBytes = &SummaryStatisticsForFloat64DataSet{
			individualDataPoints: make([]float64, 0, 100),
		}
	}

	if thingsThatCanBeComputedInclude.TTLBStatistics {
		container.TimeToLastByte = &SummaryStatisticsForFloat64DataSet{
			individualDataPoints: make([]float64, 0, 100),
		}
	}

	return container
}

func extractColumnTypesFromHeader(headerLineWithoutNewline string) ([]EntryDataType, *columnsPresent, error) {
	columnNamesAsStrings := strings.Split(headerLineWithoutNewline, ",")
	if len(columnNamesAsStrings) == 0 {
		return nil, nil, fmt.Errorf("no header columns found")
	}

	dataTypes := make([]EntryDataType, len(columnNamesAsStrings))
	var columnsPresentInHeader columnsPresent

	for i, columnName := range columnNamesAsStrings {
		switch columnName {
		case "timeStamp":
			dataTypes[i] = Timestamp
			columnsPresentInHeader.TimestampAsUnixEpochMs = true
		case "elapsed":
			dataTypes[i] = TimeToLastByte
			columnsPresentInHeader.TimeToLastByte = true
		case "label":
			dataTypes[i] = ResultLabel
			columnsPresentInHeader.SampleResultLabel = true
		case "responseCode":
			dataTypes[i] = ResponseCode
			columnsPresentInHeader.ResponseCode = true
		case "responseMessage":
			dataTypes[i] = ResponseMessage
			columnsPresentInHeader.ResponseMessage = true
		case "threadName":
			dataTypes[i] = ThreadName
			columnsPresentInHeader.ThreadNameText = true
		case "dataType":
			dataTypes[i] = DataType
			columnsPresentInHeader.DataType = true
		case "success":
			dataTypes[i] = SuccessFlag
			columnsPresentInHeader.SuccessFlag = true
		case "failureMessage":
			dataTypes[i] = FailureMessage
			columnsPresentInHeader.FailureMessage = true
		case "bytes":
			dataTypes[i] = ResponseBytesReceived
			columnsPresentInHeader.ResponseBytesReceived = true
		case "sentBytes":
			dataTypes[i] = RequestBodySizeInBytes
			columnsPresentInHeader.RequestBodySizeInBytes = true
		case "grpThreads":
			dataTypes[i] = GroupThreads
			columnsPresentInHeader.GroupThreads = true
		case "allThreads":
			dataTypes[i] = AllThreads
			columnsPresentInHeader.AllThreads = true
		case "URL":
			dataTypes[i] = RequestURL
			columnsPresentInHeader.RequestURLString = true
		case "Latency":
			dataTypes[i] = TimeToFirstByte
			columnsPresentInHeader.TimeToFirstByte = true
		case "IdleTime":
			dataTypes[i] = IdleTime
			columnsPresentInHeader.IdleTimeInMs = true
		case "Connect":
			dataTypes[i] = ConnectTime
			columnsPresentInHeader.ConnectTimeInMs = true
		default:
			return nil, nil, fmt.Errorf("unrecognized column name (%s) in header", columnName)
		}
	}

	return dataTypes, &columnsPresentInHeader, nil
}

func extractDataFromCsvRow(rowTextWithoutNewline string, columnTypesInOrder *[]EntryDataType) (*DataRow, error) {
	columnsAsText := strings.Split(rowTextWithoutNewline, ",")
	if len(columnsAsText) != len(*columnTypesInOrder) {
		return nil, fmt.Errorf("data row contains (%d) columns, but expected (%d) columns", len(columnsAsText), len(*columnTypesInOrder))
	}

	dataRow := &DataRow{}

	for i, columnType := range *columnTypesInOrder {
		columnStringValue := columnsAsText[i]

		var err error

		switch columnType {
		case Timestamp:
			if dataRow.TimestampAsUnixEpochMs, err = strconv.ParseUint(columnStringValue, 10, 64); err != nil {
				return nil, fmt.Errorf("cannot convert timeStamp column (%s) to uint", columnStringValue)
			}
		case TimeToLastByte:
			if dataRow.TimeToLastByte, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert elapsed column (%s) to int", columnStringValue)
			}
		case ResultLabel:
			dataRow.SampleResultLabel = columnStringValue
		case ResponseCode:
			if dataRow.ResponseCode, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert responseCode column (%s) to int", columnStringValue)
			}
		case ResponseMessage:
			dataRow.ResponseMessage = columnStringValue
		case ThreadName:
			dataRow.ThreadNameText = columnStringValue
		case DataType:
			dataRow.DataType = columnStringValue
		case SuccessFlag:
			if dataRow.RequestWasSuccessful, err = stringColumnToBool(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert success column (%s) to boolean", columnStringValue)
			}
		case FailureMessage:
			dataRow.FailureMessage = columnStringValue
		case ResponseBytesReceived:
			if dataRow.ResponseBytesReceived, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert bytes column (%s) to int", columnStringValue)
			}
		case RequestBodySizeInBytes:
			if dataRow.RequestBodySizeInBytes, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert sentBytes column (%s) to int", columnStringValue)
			}
		case GroupThreads:
			if dataRow.GroupThreads, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert grpThreads column (%s) to int", columnStringValue)
			}
		case AllThreads:
			if dataRow.AllThreads, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert allThreads column (%s) to int", columnStringValue)
			}
		case RequestURL:
			dataRow.RequestURLString = columnStringValue
		case TimeToFirstByte:
			if dataRow.TimeToFirstByte, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert Latency column (%s) to int", columnStringValue)
			}
		case IdleTime:
			if dataRow.IdleTimeInMs, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert IdleTime column (%s) to int", columnStringValue)
			}
		case ConnectTime:
			if dataRow.ConnectTimeInMs, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert Connect column (%s) to int", columnStringValue)
			}
		}
	}

	return dataRow, nil
}

func stringToIntOrNegativeOneOnEmpty(s string) (int, error) {
	if s == "" {
		return -1, nil
	}

	return strconv.Atoi(s)
}

func stringColumnToBool(s string) (bool, error) {
	if s == "true" {
		return true, nil
	}

	if s == "false" {
		return false, nil
	}

	return false, fmt.Errorf("cannot convert string to boolean")
}
