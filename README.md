# jtl
Processor for Jmeter JTL file

## Thoughts

```golang
j, err := jtl.ReadFromFile(filepath string)
if err != nil {
    panic(err)
}

for _, row := range j.DataRows() {
    if row.Timestamp > someTimeInThePast {
        ...
    }
}

stats, err := j.GlobalSummaryStats()
stats, err := j.SummaryStatsByRequestURL()
stats, err := j.SummaryStatsByRequestBodySize()
stats, err := j.SummaryStatsByResponseSize()
stats, err := j.SummaryStatsByResponseSuccess()

fmt.Printf("%f, %f, %f, %f\n", globalStats.TTFB.Mean, globalStats.TTFB.Max, globalStats.TTFB.Min, ...)

```

In order to summarize:

- foreach data row:
    - foreach dimension that can be generated (based on available columns in data source)
        - get dimension key
        - add count, success, ttfb, ttlb to dimension/key
    - 

```golang
type DataSource interface {
    Rows() []*DataRow
    HasColumn(ColumnType) bool
}

type Summarizer struct {}

func NewSummarizer(usingDataSource DataSource) *Summarizer {}

func (s *Summarizer) ComputeSummariesForColumns(columns ...ColumnType) error {}

func (s *Summarizer) AggregateSummary() (*Summary, error)
func (s *Summarizer) SummariesForColumn(column ColumnType) ([]*Summary, error)
func (s *Summarizer) SortedSummariesForColumn(column ColumnType) ([]*Summary, error)

```