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

type columnsPresent struct {
	TimestampAsUnixEpochMs bool
	TimeToLastByte         bool
	SampleResultLabel      bool
	ResponseCode           bool
	ResponseMessage        bool
	ThreadNameText         bool
	DataType               bool
	SuccessFlag            bool
	FailureMessage         bool
	ResponseBytesReceived  bool
	RequestBodySizeInBytes bool
	GroupThreads           bool
	AllThreads             bool
	RequestURLString       bool
	TimeToFirstByte        bool
	IdleTimeInMs           bool
	ConnectTimeInMs        bool
}
