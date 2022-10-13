package jtl

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
)

// import (
// 	"bufio"
// 	"fmt"
// 	"io"
// 	"strconv"
// 	"strings"
// )

// LoggerSet contains two loggers: one for detailed, verbose output, and the other for Informational output.
// If a logger is set to nil, that type of output won't be generated.
type LoggerSet struct {
	VerboseLogging       *log.Logger
	InformationalLogging *log.Logger
}

// Processor is an entity that consumes a JTL data source and can perform analysis of the data in the source.
type Processor struct {
	loggerSet                      *LoggerSet
	columnTypesInOrder             []ColumnType
	columnsInDataSourceLookupTable *columnsPresent
}

// NewProcessor creates a new, empty Processor.
func NewProcessor() *Processor {
	return &Processor{
		loggerSet: nil,
	}
}

// UsingLogger attaches a LoggerSet to the Processor.
func (processor *Processor) UsingLoggers(loggerSet *LoggerSet) *Processor {
	processor.loggerSet = loggerSet
	return processor
}

// PopulateFromCsvData consumes JTL in CSV format and sets that as the data source for the Processor methods.
// The first line must be a JTL CSV header.  For each subsequent row, the number of columns must match the
// number of header columns.
func (processor *Processor) PopulateFromCsvData(reader io.Reader) (*Processor, error) {
	scanner := bufio.NewScanner(reader)

	var err error

	if !scanner.Scan() {
		if scanner.Err() != nil {
			return nil, scanner.Err()
		}
		return nil, fmt.Errorf("no header found")
	}

	processor.columnTypesInOrder, processor.columnsInDataSourceLookupTable, err = extractDataSourceColumnsFromCsvHeader(scanner.Text())
	if err != nil {
		return nil, err
	}

	dataRows := make([]*DataRow, 0, 100)

	for scanner.Scan() {
		dataRow, err := extractDataFromCsvRow(scanner.Text(), &processor.columnTypesInOrder)
		if err != nil {
			return nil, err
		}
		dataRows = append(dataRows, dataRow)
	}

	if scanner.Err() != nil {
		return nil, scanner.Err()
	}

	return processor, nil
}

// SummarizationIteratorFor returns a SummerizationIterator, which can be used to walk through SummaryData available
// from the data source.  The dimensions describe the types
func (processor *Processor) SummarizationIteratorFor(dimensions ...SummarizableDimension) (*SummarizationIterator, error) {
	return nil, nil
}

// DataColumns returns the data columns present in the data source in the same order that they were presented (e.g., for CSV
// data source, it is the same order as the header column names).
func (processor *Processor) DataColumns() []*ColumnType {
	return nil
}

// DataRows returns each row of data from the data source as a standard struct in the same order as they are found in the
// data source.
func (processor *Processor) DataRows() []*DataRow {
	return nil
}

func extractDataSourceColumnsFromCsvHeader(headerLineWithoutNewline string) ([]ColumnType, *columnsPresent, error) {
	columnNamesAsStrings := strings.Split(headerLineWithoutNewline, ",")
	if len(columnNamesAsStrings) == 0 {
		return nil, nil, fmt.Errorf("no header columns found")
	}

	columnTypesInOrder := make([]ColumnType, len(columnNamesAsStrings))

	for i, headerName := range columnNamesAsStrings {
		if columnType, columnNameIsInMap := CsvHeaderLabelToColumnType[headerName]; !columnNameIsInMap {
			return nil, nil, fmt.Errorf("csv file header (%s) not understood", headerName)
		} else {
			columnTypesInOrder[i] = columnType
		}
	}

	columnsLookupTable := columnLookupTableFromColumnTypeSet(columnTypesInOrder)

	return columnTypesInOrder, columnsLookupTable, nil
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
