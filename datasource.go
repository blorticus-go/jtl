package jtl

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type DataRow struct {
	TimestampAsUnixEpochMs     uint64
	TimeToLastByte             int
	SampleResultLabel          string
	ResponseCodeOrErrorMessage string
	ResponseMessage            string
	ThreadNameText             string
	DataType                   string
	RequestWasSuccessful       bool
	FailureMessage             string
	ResponseBytesReceived      int
	RequestBodySizeInBytes     int
	GroupThreads               int
	AllThreads                 int
	RequestURLString           string
	TimeToFirstByte            int
	IdleTimeInMs               int
	ConnectTimeInMs            int
}

type ColumnType int

var Column = struct {
	Timestamp                  ColumnType
	TimeToLastByte             ColumnType
	ResultLabel                ColumnType
	ResponseCodeOrErrorMessage ColumnType
	ResponseMessage            ColumnType
	ThreadName                 ColumnType
	DataType                   ColumnType
	RequestWasSuccesful        ColumnType
	FailureMessage             ColumnType
	ResponseBytesReceived      ColumnType
	RequestBodySizeInBytes     ColumnType
	GroupThreads               ColumnType
	AllThreads                 ColumnType
	RequestURL                 ColumnType
	TimeToFirstByte            ColumnType
	IdleTime                   ColumnType
	ConnectTime                ColumnType
}{
	Timestamp:                  1,
	TimeToLastByte:             2,
	ResultLabel:                3,
	ResponseCodeOrErrorMessage: 4,
	ResponseMessage:            5,
	ThreadName:                 6,
	DataType:                   7,
	RequestWasSuccesful:        8,
	FailureMessage:             9,
	ResponseBytesReceived:      10,
	RequestBodySizeInBytes:     11,
	GroupThreads:               12,
	AllThreads:                 13,
	RequestURL:                 14,
	TimeToFirstByte:            15,
	IdleTime:                   16,
	ConnectTime:                17,
}

var ColumnTypeAsAstring = map[ColumnType]string{
	Column.Timestamp:                  "Timestamp",
	Column.TimeToLastByte:             "TimeToLastByte",
	Column.ResultLabel:                "ResultLabel",
	Column.ResponseCodeOrErrorMessage: "ResponseCode",
	Column.ResponseMessage:            "ResponseMessage",
	Column.ThreadName:                 "ThreadName",
	Column.DataType:                   "DataType",
	Column.RequestWasSuccesful:        "RequestWasSuccesful",
	Column.FailureMessage:             "FailureMessage",
	Column.ResponseBytesReceived:      "ResponseBytesReceived",
	Column.RequestBodySizeInBytes:     "RequestBodySizeInBytes",
	Column.GroupThreads:               "GroupThreads",
	Column.AllThreads:                 "AllThreads",
	Column.RequestURL:                 "RequestURL",
	Column.TimeToFirstByte:            "TimeToFirstByte",
	Column.IdleTime:                   "IdleTime",
	Column.ConnectTime:                "ConnectTime",
}

var CsvHeaderLabelToColumnType = map[string]ColumnType{
	"timeStamp":       Column.Timestamp,
	"elapsed":         Column.TimeToLastByte,
	"label":           Column.ResultLabel,
	"responseCode":    Column.ResponseCodeOrErrorMessage,
	"responseMessage": Column.ResponseMessage,
	"threadName":      Column.ThreadName,
	"dataType":        Column.DataType,
	"success":         Column.RequestWasSuccesful,
	"failureMessage":  Column.FailureMessage,
	"bytes":           Column.ResponseBytesReceived,
	"sentBytes":       Column.RequestBodySizeInBytes,
	"grpThreads":      Column.GroupThreads,
	"allThreads":      Column.AllThreads,
	"URL":             Column.RequestURL,
	"Latency":         Column.TimeToFirstByte,
	"IdleTime":        Column.IdleTime,
	"Connect":         Column.ConnectTime,
}

type TableOfColumns struct {
	AllThreads                 bool
	ConnectTime                bool
	DataType                   bool
	FailureMessage             bool
	GroupThreads               bool
	IdleTime                   bool
	RequestBodySizeInBytes     bool
	RequestURL                 bool
	ResponseBytesReceived      bool
	ResponseCodeOrErrorMessage bool
	ResponseMessage            bool
	ResultLabel                bool
	SuccessFlag                bool
	ThreadName                 bool
	TimestampAsUnixEpochMs     bool
	TimeToFirstByte            bool
	TimeToLastByte             bool
}

func (table *TableOfColumns) HasTheColumn(column ColumnType) bool {
	switch column {
	case Column.AllThreads:
		return table.AllThreads
	case Column.ConnectTime:
		return table.ConnectTime
	case Column.DataType:
		return table.DataType
	case Column.FailureMessage:
		return table.FailureMessage
	case Column.GroupThreads:
		return table.GroupThreads
	case Column.IdleTime:
		return table.IdleTime
	case Column.RequestBodySizeInBytes:
		return table.RequestBodySizeInBytes
	case Column.RequestURL:
		return table.RequestURL
	case Column.RequestWasSuccesful:
		return table.SuccessFlag
	case Column.ResponseBytesReceived:
		return table.ResponseBytesReceived
	case Column.ResponseCodeOrErrorMessage:
		return table.ResponseCodeOrErrorMessage
	case Column.ResponseMessage:
		return table.ResponseMessage
	case Column.ResultLabel:
		return table.ResultLabel
	case Column.ThreadName:
		return table.ThreadName
	case Column.TimeToFirstByte:
		return table.TimeToFirstByte
	case Column.TimeToLastByte:
		return table.TimeToLastByte
	case Column.Timestamp:
		return table.TimestampAsUnixEpochMs
	}

	return false
}

func (table *TableOfColumns) IncludesTheColumn(column ColumnType) bool {
	return table.HasTheColumn(column)
}

func makeColumnLookupTableFromColumnTypeSet(columnSet ...ColumnType) *TableOfColumns {
	columnsPresentLookupTable := &TableOfColumns{}

	for _, columnType := range columnSet {
		switch columnType {
		case Column.Timestamp:
			columnsPresentLookupTable.TimestampAsUnixEpochMs = true
		case Column.TimeToLastByte:
			columnsPresentLookupTable.TimeToLastByte = true
		case Column.ResultLabel:
			columnsPresentLookupTable.ResultLabel = true
		case Column.ResponseCodeOrErrorMessage:
			columnsPresentLookupTable.ResponseCodeOrErrorMessage = true
		case Column.ResponseMessage:
			columnsPresentLookupTable.ResponseMessage = true
		case Column.ThreadName:
			columnsPresentLookupTable.ThreadName = true
		case Column.DataType:
			columnsPresentLookupTable.DataType = true
		case Column.RequestWasSuccesful:
			columnsPresentLookupTable.SuccessFlag = true
		case Column.FailureMessage:
			columnsPresentLookupTable.FailureMessage = true
		case Column.ResponseBytesReceived:
			columnsPresentLookupTable.ResponseBytesReceived = true
		case Column.RequestBodySizeInBytes:
			columnsPresentLookupTable.RequestBodySizeInBytes = true
		case Column.GroupThreads:
			columnsPresentLookupTable.GroupThreads = true
		case Column.AllThreads:
			columnsPresentLookupTable.AllThreads = true
		case Column.RequestURL:
			columnsPresentLookupTable.RequestURL = true
		case Column.TimeToFirstByte:
			columnsPresentLookupTable.TimeToFirstByte = true
		case Column.IdleTime:
			columnsPresentLookupTable.IdleTime = true
		case Column.ConnectTime:
			columnsPresentLookupTable.ConnectTime = true
		}
	}

	return columnsPresentLookupTable
}

// DataSource represent a normalized view of JTL data
type DataSource interface {
	// Rows returns the data rows that have processed from the concrete source
	Rows() []*DataRow

	// HasTheColumn returns whether a Column of the identified type is present in the concrete source
	HasTheColumn(ColumnType) bool

	// AllAvailableColumns provides a lookup table of rows that are present in this data source
	AllAvailableColumns() TableOfColumns
}

// CsvDataSource is a DataSource for which the concrete source is a CSV stream.  The first line
// must be a header with standardized names for columns.  Subsequent lines must be data rows
// with the same number of columns as the header in the same order.
type CsvDataSource struct {
	rows                []*DataRow
	columnsInDataSource *TableOfColumns
}

type CsvDataRowError struct {
	LineNumber uint
	Error      error
}

// NewDataSourceFromCsv reads a CSV stream, assuming a comma-delimiter between columns and a
// newline between rows.  fatalError is set if the header line cannot be interpretted or
// if the reader stream cannot be processed.  'dataRowsThatCannotBeProcessed' provides a list
// of line numbers that cannot be processed with an error indicating why.  A fatalError will
// return a nil object.  dataRowsThatCannotBeProcessed will not.
func NewDataSourceFromCsv(reader io.Reader) (source *CsvDataSource, dataRowsThatCannotBeProcessed []*CsvDataRowError, fatalError error) {
	scanner := bufio.NewScanner(reader)

	if !scanner.Scan() {
		if scanner.Err() != nil {
			return nil, nil, scanner.Err()
		}
		return nil, nil, fmt.Errorf("no header found")
	}

	columnTypesInOrder, err := extractDataSourceColumnsFromCsvHeader(scanner.Text())
	if err != nil {
		return nil, nil, err
	}

	dataRows := make([]*DataRow, 0, 100)

	dataRowErrors := make([]*CsvDataRowError, 0, 10)

	for rowInSource := uint(1); scanner.Scan(); rowInSource++ {
		dataRow, err := extractDataFromCsvRow(scanner.Text(), &columnTypesInOrder)
		if err != nil {
			dataRowErrors = append(dataRowErrors, &CsvDataRowError{
				LineNumber: rowInSource,
				Error:      err,
			})
		} else {
			dataRows = append(dataRows, dataRow)
		}
	}

	if scanner.Err() != nil {
		return nil, nil, scanner.Err()
	}

	return &CsvDataSource{
		rows:                dataRows,
		columnsInDataSource: makeColumnLookupTableFromColumnTypeSet(columnTypesInOrder...),
	}, dataRowErrors, nil
}

func (dataSource *CsvDataSource) Rows() []*DataRow {
	return dataSource.rows
}

func (dataSource *CsvDataSource) HasTheColumn(column ColumnType) bool {
	return dataSource.columnsInDataSource.HasTheColumn(column)
}

func (dataSource *CsvDataSource) AllAvailableColumns() TableOfColumns {
	return *dataSource.columnsInDataSource
}

func extractDataSourceColumnsFromCsvHeader(headerLineWithoutNewline string) (columnTypesInOrder []ColumnType, err error) {
	columnNamesAsStrings := strings.Split(headerLineWithoutNewline, ",")
	if len(columnNamesAsStrings) == 0 {
		return nil, fmt.Errorf("no header columns found")
	}

	columnTypesInOrder = make([]ColumnType, len(columnNamesAsStrings))

	for i, headerName := range columnNamesAsStrings {
		if columnType, columnNameIsInMap := CsvHeaderLabelToColumnType[headerName]; !columnNameIsInMap {
			return nil, fmt.Errorf("csv file header (%s) not understood", headerName)
		} else {
			columnTypesInOrder[i] = columnType
		}
	}

	return columnTypesInOrder, nil
}

func extractDataFromCsvRow(rowTextWithoutNewline string, columnTypesInOrder *[]ColumnType) (*DataRow, error) {
	columnsAsText := strings.Split(rowTextWithoutNewline, ",")
	if len(columnsAsText) != len(*columnTypesInOrder) {
		return nil, fmt.Errorf("data row contains (%d) columns, but expected (%d) columns", len(columnsAsText), len(*columnTypesInOrder))
	}

	dataRow := &DataRow{}

	for i, columnType := range *columnTypesInOrder {
		columnStringValue := columnsAsText[i]

		var err error

		switch columnType {
		case Column.Timestamp:
			if dataRow.TimestampAsUnixEpochMs, err = strconv.ParseUint(columnStringValue, 10, 64); err != nil {
				return nil, fmt.Errorf("cannot convert timeStamp column (%s) to uint", columnStringValue)
			}
		case Column.TimeToLastByte:
			if dataRow.TimeToLastByte, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert elapsed column (%s) to int", columnStringValue)
			}
		case Column.ResultLabel:
			dataRow.SampleResultLabel = columnStringValue
		case Column.ResponseCodeOrErrorMessage:
			dataRow.ResponseCodeOrErrorMessage = columnStringValue
		case Column.ResponseMessage:
			dataRow.ResponseMessage = columnStringValue
		case Column.ThreadName:
			dataRow.ThreadNameText = columnStringValue
		case Column.DataType:
			dataRow.DataType = columnStringValue
		case Column.RequestWasSuccesful:
			if dataRow.RequestWasSuccessful, err = stringColumnToBool(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert success column (%s) to boolean", columnStringValue)
			}
		case Column.FailureMessage:
			dataRow.FailureMessage = columnStringValue
		case Column.ResponseBytesReceived:
			if dataRow.ResponseBytesReceived, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert bytes column (%s) to int", columnStringValue)
			}
		case Column.RequestBodySizeInBytes:
			if dataRow.RequestBodySizeInBytes, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert sentBytes column (%s) to int", columnStringValue)
			}
		case Column.GroupThreads:
			if dataRow.GroupThreads, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert grpThreads column (%s) to int", columnStringValue)
			}
		case Column.AllThreads:
			if dataRow.AllThreads, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert allThreads column (%s) to int", columnStringValue)
			}
		case Column.RequestURL:
			dataRow.RequestURLString = columnStringValue
		case Column.TimeToFirstByte:
			if dataRow.TimeToFirstByte, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert Latency column (%s) to int", columnStringValue)
			}
		case Column.IdleTime:
			if dataRow.IdleTimeInMs, err = stringToIntOrNegativeOneOnEmpty(columnStringValue); err != nil {
				return nil, fmt.Errorf("cannot convert IdleTime column (%s) to int", columnStringValue)
			}
		case Column.ConnectTime:
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
