package elastic

import (
	"github.com/basebytes/elastic-go/service/constructor"
	"github.com/basebytes/elastic-go/service/constructor/aggregations"
	"github.com/basebytes/elastic/fields"
	"github.com/basebytes/elastic/index"
)

var (
	builder         = constructor.New()
	AtLeastMatchOne = BoolQueryMinShouldMatchParam("1")
)

func Builder() *constructor.Constructor {
	return builder
}

func AggsBuilder() *constructor.Aggregations {
	return builder.Aggs
}

func AggSelector(path map[string]string, script any) map[string]any {
	return AggsBuilder().Pipeline.Selector(path, script)
}

func DateHistogramAgg(index index.Index, field fields.Field, fixed bool, agg map[string]any) map[string]any {
	date := AggsBuilder().Bucket.DateHistogram
	params := make([]func(*aggregations.DateHistogramParam), 0, 3)
	if fixed {
		params = append(params, date.WithFixedInterval(field.DateInterval()))
	} else {
		params = append(params, date.WithCalendarInterval(field.DateInterval()))
	}
	params = append(params, date.WithOffset("-8h"), date.WithChildAgg(agg))
	if field.Missing() != nil {
		params = append(params, date.WithMissingValue(field.Missing()))
	}
	return date(index.QueryField(field.Name()), params[0], params[1:]...)
}

func DistinctAgg(index index.Index, field fields.Field) map[string]any {
	cardinality := AggsBuilder().Metrics.Cardinality
	return cardinality(index.QueryField(field.Name()), cardinality.WithMissingValue(field.Missing()))
}

func FilterAgg(filterQuery, agg map[string]any) map[string]any {
	filterAgg := AggsBuilder().Bucket.Filter
	return filterAgg(filterQuery, filterAgg.WithChildAgg(agg))
}

func FiltersAgg(filters []func(*aggregations.FiltersParam), otherBucketKey string, agg map[string]any) map[string]any {
	if len(filters) == 0 {
		return nil
	}
	filtersAgg := AggsBuilder().Bucket.Filters
	if otherBucketKey != "" {
		filters = append(filters, filtersAgg.WithOtherBucketKey(otherBucketKey))
	}
	if len(agg) > 0 {
		filters = append(filters, filtersAgg.WithChildAgg(agg))
	}
	if len(filters) == 1 {
		return filtersAgg(filters[0])
	} else {
		return filtersAgg(filters[0], filters[1:]...)
	}
}

func HistogramAgg(index index.Index, field fields.Field, agg map[string]any) map[string]any {
	histogram := AggsBuilder().Bucket.Histogram
	return histogram(index.QueryField(field.Name()), field.DataInterval(), histogram.WithChildAgg(agg))
}

func NamedAgg(name string, agg map[string]any) map[string]any {
	return map[string]any{
		name: agg,
	}
}

func NestedAgg(name string, agg map[string]any) map[string]any {
	nested := AggsBuilder().Bucket.Nested
	return nested(name, nested.WithChildAgg(agg))
}

func RangeAgg(index index.Index, field fields.Field, rangeParams []func(param *aggregations.RangeParam), agg map[string]any) map[string]any {
	rangeAgg := AggsBuilder().Bucket.Range
	rangeParams = append(rangeParams, rangeAgg.WithChildAgg(agg))
	return rangeAgg(index.QueryField(field.Name()), rangeParams[0], rangeParams[1:]...)
}

func RangeAggParam(key string, start, end any) func(param *aggregations.RangeParam) {
	return AggsBuilder().Bucket.Range.WithRange(key, start, end)
}

func ReverseNestedAgg(path string, agg map[string]any) map[string]any {
	reverse := AggsBuilder().Bucket.ReverseNested
	return reverse(reverse.WithPath(path), reverse.WithChildAgg(agg))
}

func SumAgg(index index.Index, field fields.Field) map[string]any {
	sum := AggsBuilder().Metrics.Sum
	return sum(index.QueryField(field.Name()), sum.WithMissingValue(field.Missing()))
}

func SumBucketAgg(path string) map[string]any {
	return AggsBuilder().Pipeline.SumBucket(path)
}

func TermsAgg(index index.Index, field fields.Field, agg map[string]any) map[string]any {
	terms := AggsBuilder().Bucket.Terms
	params := make([]func(param *aggregations.TermsParam), 0, 4)
	params = append(params, terms.WithChildAgg(agg), terms.WithSize(index.FieldTermSize(field.Name())), terms.WithMissingValue(field.Missing()))
	if field.MinDocCount() > 0 {
		params = append(params, terms.WithMinDocCount(field.MinDocCount()))
	}
	return terms(index.QueryField(field.Name()), params...)
}

func ValueCountAgg(index index.Index, field fields.Field) map[string]any {
	return AggsBuilder().Metrics.ValueCount(index.QueryField(field.Name()))
}

func GenerateFilter(name string, query map[string]any) func(*aggregations.FiltersParam) {
	return AggsBuilder().Bucket.Filters.WithFilter(name, query)
}
