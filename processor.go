package jtl

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Processor struct {
	dataRows []*DataRow
}

// timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,failureMessage,bytes,sentBytes,grpThreads,allThreads,URL,Latency,IdleTime,Connect
type DataRow struct {
	TimestampAsUnixEpochMs uint64
	TimeToLastByte         int
	SampleResultLabel      string
	ResponseCode           int
	ResponseMessage        string
	ThreadNameText         string
	DataType               string
	SuccessFlag            bool
	FailureMessage         string
	ResponseBytesReceived  int
	RequestBodySizeInBytes int
	GroupThreads           int
	AllThreads             int
	RequestURLString       string
	TimeToFirstByte        int
	IdleTimeInMs           int
	ConnectTimeInMs        int
}

type EntryDataType int

const (
	Timestamp EntryDataType = iota
	TimeToLastByte
	ResultLabel
	ResponseCode
	ResponseMessage
	ThreadName
	DataType
	SuccessFlag
	FailureMessage
	ResponseBytesReceived
	RequestBodySizeInBytes
	GroupThreads
	AllThreads
	RequestURL
	TimeToFirstByte
	IdleTime
	ConnectTime
)

// var entryDataTypeAsACsvColumnName = map[EntryDataType]string{
// 	Timestamp:              "timeStamp",
// 	TimeToLastByte:         "elapsed",
// 	ResultLabel:            "label",
// 	ResponseCode:           "responseCode",
// 	ResponseMessage:        "responseMessage",
// 	ThreadName:             "threadName",
// 	DataType:               "type",
// 	SuccessFlag:            "success",
// 	FailureMessage:         "failureMessage",
// 	ResponseBytesReceived:  "bytes",
// 	RequestBodySizeInBytes: "sentBytes",
// 	GroupThreads:           "groupThreads",
// 	AllThreads:             "allThreads",
// 	RequestURL:             "URL",
// 	TimeToFirstByte:        "Latency",
// 	IdleTime:               "IdleTime",
// 	ConnectTime:            "Connect",
// }

func NewProcessorFromCsvData(reader io.Reader) (*Processor, error) {
	scanner := bufio.NewScanner(reader)

	if !scanner.Scan() {
		if scanner.Err() != nil {
			return nil, scanner.Err()
		}
		return nil, fmt.Errorf("no header found")
	}

	inOrderListOfColumnsByType, err := extractColumnTypesFromHeader(scanner.Text())
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
		dataRows: dataRows,
	}, nil
}

func (processor *Processor) DataRows() []*DataRow {
	return processor.dataRows
}

func extractColumnTypesFromHeader(headerLineWithoutNewline string) ([]EntryDataType, error) {
	columnNamesAsStrings := strings.Split(headerLineWithoutNewline, ",")
	if len(columnNamesAsStrings) == 0 {
		return nil, fmt.Errorf("no header columns found")
	}

	dataTypes := make([]EntryDataType, len(columnNamesAsStrings))

	for i, columnName := range columnNamesAsStrings {
		switch columnName {
		case "timeStamp":
			dataTypes[i] = Timestamp
		case "elapsed":
			dataTypes[i] = TimeToLastByte
		case "label":
			dataTypes[i] = ResultLabel
		case "responseCode":
			dataTypes[i] = ResponseCode
		case "responseMessage":
			dataTypes[i] = ResponseMessage
		case "threadName":
			dataTypes[i] = ThreadName
		case "dataType":
			dataTypes[i] = DataType
		case "success":
			dataTypes[i] = SuccessFlag
		case "failureMessage":
			dataTypes[i] = FailureMessage
		case "bytes":
			dataTypes[i] = ResponseBytesReceived
		case "sentBytes":
			dataTypes[i] = RequestBodySizeInBytes
		case "grpThreads":
			dataTypes[i] = GroupThreads
		case "allThreads":
			dataTypes[i] = AllThreads
		case "URL":
			dataTypes[i] = RequestURL
		case "Latency":
			dataTypes[i] = TimeToFirstByte
		case "IdleTime":
			dataTypes[i] = IdleTime
		case "Connect":
			dataTypes[i] = ConnectTime
		default:
			return nil, fmt.Errorf("unrecognized column name (%s) in header", columnName)
		}
	}

	return dataTypes, nil
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
			if dataRow.SuccessFlag, err = stringColumnToBool(columnStringValue); err != nil {
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
