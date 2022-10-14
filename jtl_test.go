package jtl_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/blorticus-go/jtl"
)

type ProcessorTestCase struct {
	jtlDataAsCsvText                       string
	expectFatalErrorOnCsvRead              bool
	numberOfExpectedDataRowErrorsOnCsvRead int
	expectedColumnSet                      jtl.TableOfColumns
	expectedNumberOfDataRows               int
	indicesOfDataRowsToValidate            []int
	expectedDataRowValues                  []*jtl.DataRow
	expectedAggregateSummary               *jtl.AggregateSummary
	expectedSummariesForColumns            map[jtl.ColumnType][]*jtl.ColumnUniqueValueSummary
}

func (testCase *ProcessorTestCase) RunTest() error {
	dataSource, dataRowErrors, fatalError := jtl.NewDataSourceFromCsv(strings.NewReader(testCase.jtlDataAsCsvText))

	if fatalError != nil {
		if !testCase.expectFatalErrorOnCsvRead {
			return fmt.Errorf("on NewDataSourceFromCsv(), expected no fatal error, got = (%s)", fatalError.Error())
		}
		return nil
	} else if testCase.expectFatalErrorOnCsvRead {
		return fmt.Errorf("on NewDataSourceFromCsv(), expected error, got no error")
	}

	if len(dataRowErrors) != testCase.numberOfExpectedDataRowErrorsOnCsvRead {
		return fmt.Errorf("expected (%d) data row read errors, got (%d)", testCase.expectedNumberOfDataRows, len(dataRowErrors))
	}

	if err := compareColumnTables(testCase.expectedColumnSet, dataSource.AllAvailableColumns()); err != nil {
		return err
	}

	gotDataRows := dataSource.Rows()

	if testCase.expectedNumberOfDataRows != len(gotDataRows) {
		return fmt.Errorf("expected (%d) data rows, got (%d)", testCase.expectedNumberOfDataRows, len(gotDataRows))
	}

	for indexOfExpectedDataRowValues, indexOfGotDataRow := range testCase.indicesOfDataRowsToValidate {
		if err := compareTwoDataRows(testCase.expectedDataRowValues[indexOfExpectedDataRowValues], gotDataRows[indexOfGotDataRow]); err != nil {
			return fmt.Errorf("for data row (%d): %s", indexOfGotDataRow+1, err.Error())
		}
	}

	summarizer := jtl.NewSummarizerForDataSource(dataSource)

	expectedColumns := make([]jtl.ColumnType, 0, len(testCase.expectedSummariesForColumns))
	for column := range testCase.expectedSummariesForColumns {
		expectedColumns = append(expectedColumns, column)
	}

	if err := summarizer.PreComputeAggregateSummaryAndSummariesForColumns(expectedColumns...); err != nil {
		return fmt.Errorf("on PreComputeAggregateSummaryAndSummariesForColumns() got error: %s", err.Error())
	}

	gotAggregateSummary, err := summarizer.AggregateSummary()
	if err != nil {
		return fmt.Errorf("on AggregateSummary() from summarizer, got error: %s", err.Error())
	}

	if err := compareAggregateSummary(testCase.expectedAggregateSummary, gotAggregateSummary); err != nil {
		return err
	}

	for expectedColumn := range testCase.expectedSummariesForColumns {
		expectedColumnUniqueValueSummarySet := testCase.expectedSummariesForColumns[expectedColumn]

		gotColumnUniqueValueSumarySet, err := summarizer.SummariesForTheColumn(expectedColumn)
		if err != nil {
			return fmt.Errorf("on SummariesForTheColumn(%s), got error: %s", jtl.ColumnTypeAsAstring[expectedColumn], err.Error())
		}

		if err := compareColumnUniqueValueSummarySets(expectedColumnUniqueValueSummarySet, gotColumnUniqueValueSumarySet); err != nil {
			return fmt.Errorf("on SummariesForTheColumn(%s): %s", jtl.ColumnTypeAsAstring[expectedColumn], err.Error())
		}
	}

	return nil
}

func compareColumnUniqueValueSummarySets(expect []*jtl.ColumnUniqueValueSummary, got []*jtl.ColumnUniqueValueSummary) error {
	if len(expect) != len(got) {
		return fmt.Errorf("expected (%d) unique value summaries, got (%d)", len(expect), len(got))
	}

	return nil
}

func compareColumnTables(expect jtl.TableOfColumns, got jtl.TableOfColumns) error {
	if err := compareSingleColumnTableEntry(expect.AllThreads, got.AllThreads, "AllThreads"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.ConnectTime, got.ConnectTime, "ConnectTime"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.DataType, got.DataType, "DataType"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.FailureMessage, got.FailureMessage, "FailureMessage"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.GroupThreads, got.GroupThreads, "GroupThreads"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.IdleTime, got.IdleTime, "IdleTime"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.RequestBodySizeInBytes, got.RequestBodySizeInBytes, "RequestBodySizeInBytes"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.RequestURL, got.RequestURL, "RequestURL"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.ResponseBytesReceived, got.ResponseBytesReceived, "ResponseBytesReceived"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.ResponseCodeOrErrorMessage, got.ResponseCodeOrErrorMessage, "ResponseCodeOrErrorMessage"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.ResponseMessage, got.ResponseMessage, "ResponseMessage"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.ResultLabel, got.ResultLabel, "ResultLabel"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.SuccessFlag, got.SuccessFlag, "SuccessFlag"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.ThreadName, got.ThreadName, "ThreadName"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.TimeToFirstByte, got.TimeToFirstByte, "TimeToFirstByte"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.TimeToLastByte, got.TimeToLastByte, "TimeToLastByte"); err != nil {
		return err
	}
	if err := compareSingleColumnTableEntry(expect.AllThreads, got.AllThreads, "AllThreads"); err != nil {
		return err
	}

	return nil
}

func compareSingleColumnTableEntry(expect bool, got bool, columnNameAsText string) error {
	if expect {
		if !got {
			return fmt.Errorf("expected column (%s), but no such column", columnNameAsText)
		}
	} else if got {
		return fmt.Errorf("expected no column (%s), but got that column", columnNameAsText)
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
	if expected.ResponseCodeOrErrorMessage != got.ResponseCodeOrErrorMessage {
		return fmt.Errorf("Expected ResponseCode (%s), got (%s)", expected.ResponseCodeOrErrorMessage, got.ResponseCodeOrErrorMessage)
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

func compareAggregateSummary(expect *jtl.AggregateSummary, got *jtl.AggregateSummary) error {
	if expect == nil {
		if got != nil {
			return fmt.Errorf("expected aggregate summary to be nil, is not")
		}
		return nil
	} else if got == nil {
		return fmt.Errorf("expected aggregate summary, got nil")
	}

	if expect.AverageTPSRate != got.AverageTPSRate {
		return fmt.Errorf("expected aggregate summary AverageTPSRate = (%d), got = (%d)", expect.AverageTPSRate, got.AverageTPSRate)
	}
	if expect.TimestampOfFirstDataEntryAsUnixEpochMs != got.TimestampOfFirstDataEntryAsUnixEpochMs {
		return fmt.Errorf("expected aggregate summary TimestampOfFirstDataEntryAsUnixEpochMs = (%d), got = (%d)", expect.TimestampOfFirstDataEntryAsUnixEpochMs, got.TimestampOfFirstDataEntryAsUnixEpochMs)
	}
	if expect.TimestampOfLastDataEntryAsUnixEpochMs != got.TimestampOfLastDataEntryAsUnixEpochMs {
		return fmt.Errorf("expected aggregate summary TimestampOfLastDataEntryAsUnixEpochMs = (%d), got = (%d)", expect.TimestampOfLastDataEntryAsUnixEpochMs, got.TimestampOfLastDataEntryAsUnixEpochMs)
	}

	if expect.NumberOfMatchingRequests != got.NumberOfMatchingRequests {
		return fmt.Errorf("expected aggregate summary NumberOfMatchingRequests = (%d), got (%d)", expect.NumberOfMatchingRequests, got.NumberOfMatchingRequests)
	}
	if expect.NumberOfSuccessfulRequests != got.NumberOfSuccessfulRequests {
		return fmt.Errorf("expected aggregate summary NumberOfSuccessfulRequests = (%d), got (%d)", expect.NumberOfSuccessfulRequests, got.NumberOfSuccessfulRequests)
	}

	if err := compareSummaryStatistics(expect.TimeToFirstByteStatistics, got.TimeToFirstByteStatistics); err != nil {
		return fmt.Errorf("for aggregate summary TimeToFirstByteStatistics: %s", err.Error())
	}
	if err := compareSummaryStatistics(expect.TimeToLastByteStatistics, got.TimeToLastByteStatistics); err != nil {
		return fmt.Errorf("for aggregate summary TimeToLastByteStatistics: %s", err.Error())
	}

	return nil
}

func compareSummaryStatistics(expectedSummaryStats *jtl.SummaryStatistics, gotSummaryStats *jtl.SummaryStatistics) error {
	if expectedSummaryStats == nil {
		if gotSummaryStats != nil {
			return fmt.Errorf("expected no summary stats, got stats")
		}
		return nil
	} else if gotSummaryStats == nil {
		return fmt.Errorf("expected summary stats, got no stats")

	}
	if expectedSummaryStats.Mean != gotSummaryStats.Mean {
		return fmt.Errorf("expected mean (%f), got (%f)", expectedSummaryStats.Mean, gotSummaryStats.Mean)
	}
	if expectedSummaryStats.Median != gotSummaryStats.Median {
		return fmt.Errorf("expected median (%f), got (%f)", expectedSummaryStats.Median, gotSummaryStats.Median)
	}
	if expectedSummaryStats.Minimum != gotSummaryStats.Minimum {
		return fmt.Errorf("expected minimum (%f), got (%f)", expectedSummaryStats.Minimum, gotSummaryStats.Minimum)
	}
	if expectedSummaryStats.Maximum != gotSummaryStats.Maximum {
		return fmt.Errorf("expected maximum (%f), got (%f)", expectedSummaryStats.Maximum, gotSummaryStats.Maximum)
	}
	if expectedSummaryStats.PopulationStandardDeviation != gotSummaryStats.PopulationStandardDeviation {
		return fmt.Errorf("expected pstdev (%f), got (%f)", expectedSummaryStats.PopulationStandardDeviation, gotSummaryStats.PopulationStandardDeviation)
	}
	if expectedSummaryStats.ValueAt5thPercentile != gotSummaryStats.ValueAt5thPercentile {
		return fmt.Errorf("expected 5thpercentile (%f), got (%f)", expectedSummaryStats.ValueAt5thPercentile, gotSummaryStats.ValueAt5thPercentile)
	}
	if expectedSummaryStats.ValueAt95thPercentile != gotSummaryStats.ValueAt95thPercentile {
		return fmt.Errorf("expected 95thpercentile (%f), got (%f)", expectedSummaryStats.ValueAt95thPercentile, gotSummaryStats.ValueAt95thPercentile)
	}

	return nil
}

func TestProcessor(t *testing.T) {
	for testIndex, testCase := range []*ProcessorTestCase{
		{
			jtlDataAsCsvText:          jtl_header_only,
			expectFatalErrorOnCsvRead: false,
			expectedColumnSet: jtl.TableOfColumns{AllThreads: true, ConnectTime: true, DataType: true, FailureMessage: true, GroupThreads: true, IdleTime: true, RequestBodySizeInBytes: true, RequestURL: true, ResponseBytesReceived: true,
				ResponseCodeOrErrorMessage: true, ResponseMessage: true, ResultLabel: true, SuccessFlag: true, ThreadName: true, TimestampAsUnixEpochMs: true, TimeToFirstByte: true, TimeToLastByte: true},
			expectedNumberOfDataRows: 0,
			expectedAggregateSummary: &jtl.AggregateSummary{
				NumberOfMatchingRequests:   0,
				NumberOfSuccessfulRequests: 0,
			},
		},
		{
			jtlDataAsCsvText:          jtl_good_01,
			expectFatalErrorOnCsvRead: false,
			expectedColumnSet: jtl.TableOfColumns{AllThreads: true, ConnectTime: true, DataType: true, FailureMessage: true, GroupThreads: true, IdleTime: true, RequestBodySizeInBytes: true, RequestURL: true, ResponseBytesReceived: true,
				ResponseCodeOrErrorMessage: true, ResponseMessage: true, ResultLabel: true, SuccessFlag: true, ThreadName: true, TimestampAsUnixEpochMs: true, TimeToFirstByte: true, TimeToLastByte: true},
			expectedNumberOfDataRows:    9,
			indicesOfDataRowsToValidate: []int{0, 4, 8},
			expectedDataRowValues: []*jtl.DataRow{
				{1662749136019, 170, "get 1KiB.html", "200", "OK", "Thread Group 1-1", "text", true, "", 1430, 0, 1, 1, "http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html", 162, 0, 100},
				{1662749136325, 44, "get 1KiB.html", "200", "OK", "Thread Group 1-2", "text", true, "", -1, 2122, 1, 2, "http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html", 44, 0, 1},
				{1662749136502, 43, "get 1KiB.html", "200", "OK", "Thread Group 1-1", "text", true, "", 1430, 0, 1, 1, "http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html", 43, 0, 0},
			},
			expectedAggregateSummary: &jtl.AggregateSummary{
				NumberOfMatchingRequests:               9,
				NumberOfSuccessfulRequests:             9,
				TimestampOfFirstDataEntryAsUnixEpochMs: 1662749136019,
				TimestampOfLastDataEntryAsUnixEpochMs:  1662749136502,
				AverageTPSRate:                         0,
				TimeToFirstByteStatistics: &jtl.SummaryStatistics{
					Mean:                        56.44444444444444,
					Median:                      43,
					Maximum:                     162,
					Minimum:                     43,
					PopulationStandardDeviation: 37.321757464606534,
					ValueAt5thPercentile:        43,
					ValueAt95thPercentile:       44,
				},
				TimeToLastByteStatistics: &jtl.SummaryStatistics{
					Mean:                        57.333333333333333,
					Median:                      43,
					Maximum:                     170,
					Minimum:                     43,
					PopulationStandardDeviation: 39.83577398380617,
					ValueAt5thPercentile:        43,
					ValueAt95thPercentile:       44,
				},
			},
			expectedSummariesForColumns: map[jtl.ColumnType][]*jtl.ColumnUniqueValueSummary{
				jtl.Column.RequestURL: {
					&jtl.ColumnUniqueValueSummary{
						Column:                     jtl.Column.RequestURL,
						Key:                        "http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html",
						NumberOfMatchingRequests:   9,
						NumberOfSuccessfulRequests: 9,
						TimeToFirstByteStatistics: &jtl.SummaryStatistics{
							Mean:                        56.44444444444444,
							Median:                      43,
							Maximum:                     162,
							Minimum:                     43,
							PopulationStandardDeviation: 37.321757464606534,
							ValueAt5thPercentile:        43,
							ValueAt95thPercentile:       44,
						},
						TimeToLastByteStatistics: &jtl.SummaryStatistics{
							Mean:                        57.333333333333333,
							Median:                      43,
							Maximum:                     170,
							Minimum:                     43,
							PopulationStandardDeviation: 39.83577398380617,
							ValueAt5thPercentile:        43,
							ValueAt95thPercentile:       44,
						},
					},
				},
			},
		},
	} {
		if err := testCase.RunTest(); err != nil {
			t.Errorf("on test with index (%d): %s", testIndex, err.Error())
		}
	}
}
