package benchmark

import (
	"math"
	"sort"
)

// Distribution summarizes samples with the nearest-rank percentile method.
// Null percentile values mean there were no samples for that measurement.
type Distribution struct {
	Count int      `json:"count"`
	Min   *float64 `json:"min"`
	Mean  *float64 `json:"mean"`
	P50   *float64 `json:"p50"`
	P95   *float64 `json:"p95"`
	P99   *float64 `json:"p99"`
	Max   *float64 `json:"max"`
}

func summarize(values []float64) Distribution {
	result := Distribution{Count: len(values)}
	if len(values) == 0 {
		return result
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	var total float64
	for _, value := range ordered {
		total += value
	}
	mean := total / float64(len(ordered))
	min, max := ordered[0], ordered[len(ordered)-1]
	result.Min = &min
	result.Mean = &mean
	result.P50 = percentile(ordered, .50)
	result.P95 = percentile(ordered, .95)
	result.P99 = percentile(ordered, .99)
	result.Max = &max
	return result
}

func percentile(ordered []float64, p float64) *float64 {
	if len(ordered) == 0 || p <= 0 || p > 1 {
		return nil
	}
	rank := int(math.Ceil(p * float64(len(ordered))))
	if rank < 1 {
		rank = 1
	}
	value := ordered[rank-1]
	return &value
}
