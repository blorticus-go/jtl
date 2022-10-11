package jtl_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/blorticus-go/jtl"
)

type ProcessorTestCase struct {
	srcText                     string
	expectReadError             bool
	expectedNumberOfDataRows    int
	indicesOfDataRowsToValidate []int
	expectedDataRowValues       []*jtl.DataRow
}

func (testCase *ProcessorTestCase) RunTest() error {
	p, err := jtl.NewProcessorFromCsvData(strings.NewReader(testCase.srcText))
	if err != nil {
		if !testCase.expectReadError {
			return fmt.Errorf("on ReadFromString(), expected no error, got = (%s)", err.Error())
		}
	} else if testCase.expectReadError {
		return fmt.Errorf("on ReadFromString(), expect error, got no error")
	}

	gotDataRows := p.DataRows()

	if len(gotDataRows) != testCase.expectedNumberOfDataRows {
		return fmt.Errorf("expected (%d) data rows, got = (%d)", testCase.expectedNumberOfDataRows, len(gotDataRows))
	}

	for _, i := range testCase.indicesOfDataRowsToValidate {
		if err := compareTwoDataRows(testCase.expectedDataRowValues[i], gotDataRows[i]); err != nil {
			return fmt.Errorf("for data row (%d): %s", i+1, err.Error())
		}
	}

	return nil
}

func TestProcessor(t *testing.T) {
	for testIndex, testCase := range []*ProcessorTestCase{
		{
			srcText:                  jtl_header_only,
			expectedNumberOfDataRows: 0,
		},
		{
			srcText:                     jtl_good_01,
			expectedNumberOfDataRows:    9,
			indicesOfDataRowsToValidate: []int{0},
			expectedDataRowValues: []*jtl.DataRow{
				{1662749136019, 170, "get 1KiB.html", 200, "OK", "Thread Group 1-1", "text", true, "", 1430, 0, 1, 1, "http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html", 162, 0, 100},
			},
		},
	} {
		if err := testCase.RunTest(); err != nil {
			t.Errorf("on test with index (%d): %s", testIndex, err.Error())
		}
	}
}

func compareTwoDataRows(expected, got *jtl.DataRow) error {
	if expected.TimestampAsUnixEpochMs != got.TimestampAsUnixEpochMs {
		return fmt.Errorf("Expected TimestampAsUnixEpochMs (%d), got (%d)", expected.TimestampAsUnixEpochMs, got.TimestampAsUnixEpochMs)
	}
	if expected.TimeToLastByte != got.TimeToLastByte {
		return fmt.Errorf("Expected TimeToLastByte (%d), got (%d)", expected.TimestampAsUnixEpochMs, got.TimestampAsUnixEpochMs)
	}
	if expected.SampleResultLabel != got.SampleResultLabel {
		return fmt.Errorf("Expected SampleResultLabel (%s), got (%s)", expected.SampleResultLabel, got.SampleResultLabel)
	}
	if expected.ResponseCode != got.ResponseCode {
		return fmt.Errorf("Expected ResponseCode (%d), got (%d)", expected.ResponseCode, got.ResponseCode)
	}
	if expected.ResponseMessage != got.ResponseMessage {
		return fmt.Errorf("Expected ResponseMessage (%s), got (%s)", expected.ResponseMessage, got.ResponseMessage)
	}
	if expected.ThreadNameText != got.ThreadNameText {
		return fmt.Errorf("Expected ThreadNameText (%s), got (%s)", expected.ThreadNameText, got.ThreadNameText)
	}
	if expected.DataType != got.DataType {
		return fmt.Errorf("Expected DataType (%s), got (%s)", expected.DataType, got.DataType)
	}
	if expected.SuccessFlag != got.SuccessFlag {
		return fmt.Errorf("Expected SuccessFlag (%t), got (%t)", expected.SuccessFlag, got.SuccessFlag)
	}
	if expected.FailureMessage != got.FailureMessage {
		return fmt.Errorf("Expected FailureMessage (%s), got (%s)", expected.FailureMessage, got.FailureMessage)
	}
	if expected.ResponseBytesReceived != got.ResponseBytesReceived {
		return fmt.Errorf("Expected ResponseBytesReceived (%d), got (%d)", expected.ResponseBytesReceived, got.ResponseBytesReceived)
	}
	if expected.RequestBodySizeInBytes != got.RequestBodySizeInBytes {
		return fmt.Errorf("Expected RequestBodySizeInBytes (%d), got (%d)", expected.RequestBodySizeInBytes, got.RequestBodySizeInBytes)
	}
	if expected.GroupThreads != got.GroupThreads {
		return fmt.Errorf("Expected GroupThreads (%d), got (%d)", expected.GroupThreads, got.GroupThreads)
	}
	if expected.AllThreads != got.AllThreads {
		return fmt.Errorf("Expected AllThreads (%d), got (%d)", expected.AllThreads, got.AllThreads)
	}
	if expected.RequestURLString != got.RequestURLString {
		return fmt.Errorf("Expected RequestURLString (%s), got (%s)", expected.RequestURLString, got.RequestURLString)
	}
	if expected.TimeToFirstByte != got.TimeToFirstByte {
		return fmt.Errorf("Expected TimeToFirstByte (%d), got (%d)", expected.TimeToFirstByte, got.TimeToFirstByte)
	}
	if expected.IdleTimeInMs != got.IdleTimeInMs {
		return fmt.Errorf("Expected IdleTimeInMs (%d), got (%d)", expected.IdleTimeInMs, got.IdleTimeInMs)
	}
	if expected.ConnectTimeInMs != got.ConnectTimeInMs {
		return fmt.Errorf("Expected ConnectTimeInMs (%d), got (%d)", expected.ConnectTimeInMs, got.ConnectTimeInMs)
	}

	return nil
}

var jtl_header_only = `timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,failureMessage,bytes,sentBytes,grpThreads,allThreads,URL,Latency,IdleTime,Connect
`

var jtl_good_01 = `timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,failureMessage,bytes,sentBytes,grpThreads,allThreads,URL,Latency,IdleTime,Connect
1662749136019,170,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,162,0,100
1662749136192,44,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,44,0,1
1662749136237,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,1
1662749136281,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,1
1662749136325,44,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,44,0,1
1662749136370,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
1662749136414,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
1662749136458,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
1662749136502,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0`
