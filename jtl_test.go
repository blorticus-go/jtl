package jtl_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/blorticus-go/jtl"
)

type ProcessorTestCase struct {
	jtlDataAsCsvText            string
	expectErrorOnPopulation     bool
	expectedColumnSet           []jtl.ColumnType
	expectedNumberOfDataRows    int
	indicesOfDataRowsToValidate []int
	expectedDataRowValues       []*jtl.DataRow
	expectedDimensionKeyStats   map[jtl.SummarizableDimension]map[string]*jtl.SummaryStatistics
}

func (testCase *ProcessorTestCase) RunTest() error {
	p, err := jtl.NewProcessor().PopulateFromCsvData(strings.NewReader(testCase.jtlDataAsCsvText))
	if err != nil {
		if !testCase.expectErrorOnPopulation {
			return fmt.Errorf("on PopulateFromCsvData(), expected no error, got = (%s)", err.Error())
		}
		return nil
	} else if testCase.expectErrorOnPopulation {
		return fmt.Errorf("on PopulateFromCsvData(), expected error, got no error")
	}

	if err := compareColumnSets(testCase.expectedColumnSet, p.DataColumns()); err != nil {
		return err
	}

	gotDataRows := p.DataRows()
	if testCase.expectedNumberOfDataRows != len(gotDataRows) {
		return fmt.Errorf("expected (%d) data rows, got (%d)", testCase.expectedNumberOfDataRows, len(p.DataRows()))
	}

	for indexOfExpectedDataRowValues, indexOfGotDataRow := range testCase.indicesOfDataRowsToValidate {
		if err := compareTwoDataRows(testCase.expectedDataRowValues[indexOfExpectedDataRowValues], gotDataRows[indexOfGotDataRow]); err != nil {
			return fmt.Errorf("for data row (%d): %s", indexOfGotDataRow+1, err.Error())
		}
	}

	summarizableDimensions := make([]jtl.SummarizableDimension, 0, len(testCase.expectedDimensionKeyStats))
	for k := range testCase.expectedDimensionKeyStats {
		summarizableDimensions = append(summarizableDimensions, k)
	}

	iterator, err := p.SummarizationIteratorFor(summarizableDimensions...)
	if err != nil {
		return fmt.Errorf("on SummarizationIteratorFor(), expected no error, got = (%s)", err.Error())
	}

	if err := compareIteratorResultsToExpectedValues(testCase.expectedDimensionKeyStats, iterator); err != nil {
		return err
	}

	return nil
}

func compareIteratorResultsToExpectedValues(expected map[jtl.SummarizableDimension]map[string]*jtl.SummaryStatistics, iterator *jtl.SummarizationIterator) error {
	for dk := iterator.Next(); dk != nil; dk = iterator.Next() {
		dimension := dk.Dimension
		dimensionKeyMap := expected[dimension]
		if dimensionKeyMap == nil {
			return fmt.Errorf("expected to not see dimension (%s), got that dimension", jtl.SummarizableDimensionAsString[dimension])
		}

		expectedSummaryStats := dimensionKeyMap[dk.KeyAsString()]
		if expectedSummaryStats == nil {
			return fmt.Errorf("expected to not see key (%s) for dimension (%s), got that key", dk.KeyAsString(), jtl.SummarizableDimensionAsString[dimension])
		}

		gotSummaryStats := dk.Statistics

		if expectedSummaryStats.Mean != gotSummaryStats.Mean {
			return fmt.Errorf("on dimension (%s) key (%s), expected mean (%f), got (%f)", jtl.SummarizableDimensionAsString[dimension], dk.KeyAsString(), expectedSummaryStats.Mean, gotSummaryStats.Mean)
		}
		if expectedSummaryStats.Median != gotSummaryStats.Median {
			return fmt.Errorf("on dimension (%s) key (%s), expected median (%f), got (%f)", jtl.SummarizableDimensionAsString[dimension], dk.KeyAsString(), expectedSummaryStats.Median, gotSummaryStats.Median)
		}
		if expectedSummaryStats.Minimum != gotSummaryStats.Minimum {
			return fmt.Errorf("on dimension (%s) key (%s), expected minimum (%f), got (%f)", jtl.SummarizableDimensionAsString[dimension], dk.KeyAsString(), expectedSummaryStats.Minimum, gotSummaryStats.Minimum)
		}
		if expectedSummaryStats.Maximum != gotSummaryStats.Maximum {
			return fmt.Errorf("on dimension (%s) key (%s), expected maximum (%f), got (%f)", jtl.SummarizableDimensionAsString[dimension], dk.KeyAsString(), expectedSummaryStats.Maximum, gotSummaryStats.Maximum)
		}
		if expectedSummaryStats.PopulationStandardDeviation != gotSummaryStats.PopulationStandardDeviation {
			return fmt.Errorf("on dimension (%s) key (%s), expected pstdev (%f), got (%f)", jtl.SummarizableDimensionAsString[dimension], dk.KeyAsString(), expectedSummaryStats.PopulationStandardDeviation, gotSummaryStats.PopulationStandardDeviation)
		}
		if expectedSummaryStats.ValueAt5thPercentile != gotSummaryStats.ValueAt5thPercentile {
			return fmt.Errorf("on dimension (%s) key (%s), expected 5thpercentile (%f), got (%f)", jtl.SummarizableDimensionAsString[dimension], dk.KeyAsString(), expectedSummaryStats.ValueAt5thPercentile, gotSummaryStats.ValueAt5thPercentile)
		}
		if expectedSummaryStats.ValueAt95thPercentile != gotSummaryStats.ValueAt95thPercentile {
			return fmt.Errorf("on dimension (%s) key (%s), expected 95thpercentile (%f), got (%f)", jtl.SummarizableDimensionAsString[dimension], dk.KeyAsString(), expectedSummaryStats.ValueAt95thPercentile, gotSummaryStats.ValueAt95thPercentile)
		}
	}

	return nil
}

func compareColumnSets(expectedColumnSet []jtl.ColumnType, gotColumnSet []*jtl.ColumnType) error {
	if len(expectedColumnSet) != len(gotColumnSet) {
		return fmt.Errorf("expected (%d) columns, got (%d)", len(expectedColumnSet), len(gotColumnSet))
	}

	for i, expectedColumnType := range expectedColumnSet {
		if *gotColumnSet[i] != expectedColumnType {
			return fmt.Errorf("expected column with index (%d) to be (%s), got (%s)", i, jtl.ColumnTypeAsAstring[expectedColumnType], jtl.ColumnTypeAsAstring[*gotColumnSet[i]])
		}
	}

	return nil
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
	if expected.RequestWasSuccessful != got.RequestWasSuccessful {
		return fmt.Errorf("Expected SuccessFlag (%t), got (%t)", expected.RequestWasSuccessful, got.RequestWasSuccessful)
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

func TestProcessor(t *testing.T) {
	for testIndex, testCase := range []*ProcessorTestCase{
		{
			jtlDataAsCsvText:        jtl_header_only,
			expectErrorOnPopulation: false,
			expectedColumnSet: []jtl.ColumnType{jtl.Column.Timestamp, jtl.Column.TimeToLastByte, jtl.Column.ResultLabel, jtl.Column.ResponseCode, jtl.Column.ResponseMessage,
				jtl.Column.ThreadName, jtl.Column.DataType, jtl.Column.RequestWasSuccesful, jtl.Column.FailureMessage, jtl.Column.ResponseBytesReceived,
				jtl.Column.RequestBodySizeInBytes, jtl.Column.RequestURL, jtl.Column.TimeToFirstByte, jtl.Column.IdleTime, jtl.Column.ConnectTime},
			expectedNumberOfDataRows: 0,
		},
	} {
		if err := testCase.RunTest(); err != nil {
			t.Errorf("on test with index (%d): %s", testIndex, err.Error())
		}
	}
}

var jtl_header_only = `timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,failureMessage,bytes,sentBytes,grpThreads,allThreads,URL,Latency,IdleTime,Connect
`

// var jtl_good_01 = `timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,failureMessage,bytes,sentBytes,grpThreads,allThreads,URL,Latency,IdleTime,Connect
// 1662749136019,170,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,162,0,100
// 1662749136192,44,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,44,0,1
// 1662749136237,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,1
// 1662749136281,43,get 1KiB.html,200,OK,Thread Group 1-2,text,true,,,2122,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,1
// 1662749136325,44,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,44,0,1
// 1662749136370,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
// 1662749136414,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
// 1662749136458,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0
// 1662749136502,43,get 1KiB.html,200,OK,Thread Group 1-1,text,true,,1430,0,1,1,http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html,43,0,0`
