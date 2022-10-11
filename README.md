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
