package jtl

type DataRow struct {
	TimestampAsUnixEpochMs uint64
	TimeToLastByte         int
	SampleResultLabel      string
	ResponseCode           int
	ResponseMessage        string
	ThreadNameText         string
	DataType               string
	RequestWasSuccessful   bool
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

type ColumnType int

var Column = struct {
	Timestamp              ColumnType
	TimeToLastByte         ColumnType
	ResultLabel            ColumnType
	ResponseCode           ColumnType
	ResponseMessage        ColumnType
	ThreadName             ColumnType
	DataType               ColumnType
	RequestWasSuccesful    ColumnType
	FailureMessage         ColumnType
	ResponseBytesReceived  ColumnType
	RequestBodySizeInBytes ColumnType
	GroupThreads           ColumnType
	AllThreads             ColumnType
	RequestURL             ColumnType
	TimeToFirstByte        ColumnType
	IdleTime               ColumnType
	ConnectTime            ColumnType
}{
	Timestamp:              1,
	TimeToLastByte:         2,
	ResultLabel:            3,
	ResponseCode:           4,
	ResponseMessage:        5,
	ThreadName:             6,
	DataType:               7,
	RequestWasSuccesful:    8,
	FailureMessage:         9,
	ResponseBytesReceived:  10,
	RequestBodySizeInBytes: 11,
	GroupThreads:           12,
	AllThreads:             13,
	RequestURL:             14,
	TimeToFirstByte:        15,
	IdleTime:               16,
	ConnectTime:            17,
}

var ColumnTypeAsAstring = map[ColumnType]string{
	Column.Timestamp:              "Timestamp",
	Column.TimeToLastByte:         "TimeToLastByte",
	Column.ResultLabel:            "ResultLabel",
	Column.ResponseCode:           "ResponseCode",
	Column.ResponseMessage:        "ResponseMessage",
	Column.ThreadName:             "ThreadName",
	Column.DataType:               "DataType",
	Column.RequestWasSuccesful:    "RequestWasSuccesful",
	Column.FailureMessage:         "FailureMessage",
	Column.ResponseBytesReceived:  "ResponseBytesReceived",
	Column.RequestBodySizeInBytes: "RequestBodySizeInBytes",
	Column.GroupThreads:           "GroupThreads",
	Column.AllThreads:             "AllThreads",
	Column.RequestURL:             "RequestURL",
	Column.TimeToFirstByte:        "TimeToFirstByte",
	Column.IdleTime:               "IdleTime",
	Column.ConnectTime:            "ConnectTime",
}

var CsvHeaderLabelToColumnType = map[string]ColumnType{
	"timeStamp":       Column.Timestamp,
	"elapsed":         Column.TimeToLastByte,
	"label":           Column.ResultLabel,
	"responseCode":    Column.ResponseCode,
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

type columnsPresent struct {
	TimestampAsUnixEpochMs bool
	TimeToLastByte         bool
	SampleResultLabel      bool
	ResponseCode           bool
	ResponseMessage        bool
	ThreadName             bool
	DataType               bool
	SuccessFlag            bool
	FailureMessage         bool
	ResponseBytesReceived  bool
	RequestBodySizeInBytes bool
	GroupThreads           bool
	AllThreads             bool
	RequestURL             bool
	TimeToFirstByte        bool
	IdleTime               bool
	ConnectTime            bool
}

func columnLookupTableFromColumnTypeSet(columnSet []ColumnType) *columnsPresent {
	columnsPresentLookupTable := &columnsPresent{}

	for _, columnType := range columnSet {
		switch columnType {
		case Column.Timestamp:
			columnsPresentLookupTable.TimestampAsUnixEpochMs = true
		case Column.TimeToLastByte:
			columnsPresentLookupTable.TimeToLastByte = true
		case Column.ResultLabel:
			columnsPresentLookupTable.SampleResultLabel = true
		case Column.ResponseCode:
			columnsPresentLookupTable.ResponseCode = true
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
