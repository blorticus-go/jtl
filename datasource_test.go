package jtl_test

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/blorticus-go/jtl"
)

func TestTableOfColumns(t *testing.T) {
	for testIndex, testCase := range []*TableOfColumnsTestCase{
		{
			Table:           &jtl.TableOfColumns{},
			ExpectedColumns: []jtl.ColumnType{},
		},
		{
			Table: &jtl.TableOfColumns{
				AllThreads:      true,
				GroupThreads:    true,
				ResponseMessage: true,
			},
			ExpectedColumns: []jtl.ColumnType{jtl.Column.AllThreads, jtl.Column.GroupThreads, jtl.Column.ResponseMessage},
		},
		{
			Table: &jtl.TableOfColumns{
				ConnectTime:            true,
				DataType:               true,
				FailureMessage:         true,
				IdleTime:               true,
				RequestBodySizeInBytes: true,
			},
			ExpectedColumns: []jtl.ColumnType{jtl.Column.ConnectTime, jtl.Column.DataType, jtl.Column.FailureMessage, jtl.Column.IdleTime, jtl.Column.RequestBodySizeInBytes},
		},
		{
			Table: &jtl.TableOfColumns{
				RequestURL:                 true,
				ResponseBytesReceived:      true,
				ResponseCodeOrErrorMessage: true,
				ResultLabel:                true,
				SuccessFlag:                true,
				ThreadName:                 true,
				TimestampAsUnixEpochMs:     true,
				TimeToFirstByte:            true,
				TimeToLastByte:             true,
			},
			ExpectedColumns: []jtl.ColumnType{jtl.Column.RequestURL, jtl.Column.ResponseBytesReceived, jtl.Column.ResponseCodeOrErrorMessage, jtl.Column.ResultLabel, jtl.Column.RequestWasSuccesful, jtl.Column.ThreadName,
				jtl.Column.ThreadName, jtl.Column.Timestamp, jtl.Column.TimeToFirstByte, jtl.Column.TimeToLastByte},
		},
	} {
		if err := testCase.RunTest(); err != nil {
			t.Errorf("on test with index (%d): %s", testIndex, err.Error())
		}
	}
}

var AllColumns = jtl.TableOfColumns{
	AllThreads:                 true,
	ConnectTime:                true,
	DataType:                   true,
	FailureMessage:             true,
	GroupThreads:               true,
	IdleTime:                   true,
	RequestBodySizeInBytes:     true,
	RequestURL:                 true,
	ResponseBytesReceived:      true,
	ResponseCodeOrErrorMessage: true,
	ResponseMessage:            true,
	ResultLabel:                true,
	SuccessFlag:                true,
	ThreadName:                 true,
	TimestampAsUnixEpochMs:     true,
	TimeToFirstByte:            true,
	TimeToLastByte:             true,
}

func TestCsvDataSource(t *testing.T) {
	for testIndex, testCase := range []*CsvDataSourceTestCase{
		{
			CsvReader:                 strings.NewReader(""),
			ExpectFatalErrorOnCsvRead: true,
		},
		{
			CsvReader:                 strings.NewReader("a,b,c,d,e\n"),
			ExpectFatalErrorOnCsvRead: true,
		},
		{
			CsvReader:         strings.NewReader(jtl_header_only),
			ExpectedColumnSet: AllColumns,
		},
		{
			CsvReader:                   strings.NewReader(jtl_good_01),
			ExpectedNumberOfDataRows:    9,
			ExpectedColumnSet:           AllColumns,
			IndicesOfDataRowsToValidate: []int{0, 4, 8},
			ExpectedDataRowValues: []*jtl.DataRow{
				{1662749136019, 170, "get 1KiB.html", "200", "OK", "Thread Group 1-1", "text", true, "", 1430, 0, 1, 1, "http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html", 162, 0, 100},
				{1662749136325, 44, "get 1KiB.html", "200", "OK", "Thread Group 1-2", "text", true, "", -1, 2122, 1, 2, "http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html", 44, 0, 1},
				{1662749136502, 43, "get 1KiB.html", "200", "OK", "Thread Group 1-1", "text", true, "", 1430, 0, 1, 1, "http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html", 43, 0, 0},
			},
		},
		{
			CsvReader:                              strings.NewReader(jtl_with_row_errors),
			ExpectFatalErrorOnCsvRead:              false,
			ExpectedColumnSet:                      AllColumns,
			NumberOfExpectedDataRowErrorsOnCsvRead: 11,
			ExpectedNumberOfDataRows:               4,
			IndicesOfDataRowsToValidate:            []int{0},
			ExpectedDataRowValues: []*jtl.DataRow{
				{1662749136370, 43, "get 1KiB.html", "200", "OK", "Thread Group 1-1", "text", true, "", 1430, 0, 1, 1, "http://nginx.cgam-perf-server-no-sidecar.svc/static/1KiB.html", 43, 0, 0},
			},
		},
	} {
		if err := testCase.RunTest(); err != nil {
			t.Errorf("on test with index (%d): %s", testIndex, err.Error())
		}
	}
}

type CsvDataSourceTestCase struct {
	CsvReader                              io.Reader
	ExpectFatalErrorOnCsvRead              bool
	NumberOfExpectedDataRowErrorsOnCsvRead int
	ExpectedColumnSet                      jtl.TableOfColumns
	ExpectedNumberOfDataRows               int
	IndicesOfDataRowsToValidate            []int
	ExpectedDataRowValues                  []*jtl.DataRow
}

func (testCase *CsvDataSourceTestCase) RunTest() error {
	dataSource, dataRowErrors, fatalError := jtl.NewDataSourceFromCsv(testCase.CsvReader)

	if fatalError != nil {
		if !testCase.ExpectFatalErrorOnCsvRead {
			return fmt.Errorf("on NewDataSourceFromCsv(), expected no fatal error, got = (%s)", fatalError.Error())
		}
		return nil
	} else if testCase.ExpectFatalErrorOnCsvRead {
		return fmt.Errorf("on NewDataSourceFromCsv(), expected error, got no error")
	}

	if len(dataRowErrors) != testCase.NumberOfExpectedDataRowErrorsOnCsvRead {
		return fmt.Errorf("expected (%d) data row read errors, got (%d)", testCase.NumberOfExpectedDataRowErrorsOnCsvRead, len(dataRowErrors))
	}

	if err := compareColumnTables(testCase.ExpectedColumnSet, dataSource.AllAvailableColumns()); err != nil {
		return err
	}

	gotDataRows := dataSource.Rows()

	if testCase.ExpectedNumberOfDataRows != len(gotDataRows) {
		return fmt.Errorf("expected (%d) data rows, got (%d)", testCase.ExpectedNumberOfDataRows, len(gotDataRows))
	}

	for indexOfExpectedDataRowValues, indexOfGotDataRow := range testCase.IndicesOfDataRowsToValidate {
		if err := compareTwoDataRows(testCase.ExpectedDataRowValues[indexOfExpectedDataRowValues], gotDataRows[indexOfGotDataRow]); err != nil {
			return fmt.Errorf("for data row (%d): %s", indexOfGotDataRow+1, err.Error())
		}
	}

	return nil
}

type TableOfColumnsTestCase struct {
	Table           *jtl.TableOfColumns
	ExpectedColumns []jtl.ColumnType
}

func (testCase *TableOfColumnsTestCase) RunTest() error {
	mapOfColumnTypes := make(map[jtl.ColumnType]bool)

	for column := range jtl.ColumnTypeAsAstring {
		mapOfColumnTypes[column] = true
	}

	for _, column := range testCase.ExpectedColumns {
		delete(mapOfColumnTypes, column)
		if !testCase.Table.HasTheColumn(column) {
			return fmt.Errorf("expected table HasTheColumn(%s) to be true, but it was false", jtl.ColumnTypeAsAstring[column])
		}
		if !testCase.Table.IncludesTheColumn(column) {
			return fmt.Errorf("expected table IncludesTheColumn(%s) to be true, but it was false", jtl.ColumnTypeAsAstring[column])
		}
	}

	for column := range mapOfColumnTypes {
		if testCase.Table.HasTheColumn(column) {
			return fmt.Errorf("expected table HasTheColumn(%s) to be false, but it was true", jtl.ColumnTypeAsAstring[column])
		}
		if testCase.Table.IncludesTheColumn(column) {
			return fmt.Errorf("expected table IncludesTheColumn(%s) to be false, but it was true", jtl.ColumnTypeAsAstring[column])
		}
	}

	return nil
}
