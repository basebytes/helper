package elastic

import (
	"github.com/basebytes/elastic-go/service/constructor/query"
	"github.com/basebytes/elastic/index"
)

func BoolQueryClauseParam(clause query.ClauseType, filters ...map[string]any) func(*query.BoolParam) {
	return Builder().Compound.Bool.WithClause(clause, filters...)
}

func BoolQueryMinShouldMatchParam(minShouldMatch string) func(*query.BoolParam) {
	return Builder().Compound.Bool.WithMinShouldMatch(minShouldMatch)
}

func BoolQuery(params ...func(*query.BoolParam)) map[string]any {
	return Builder().Compound.Bool(params...)
}

func ExistsQuery(index index.Index, field string) map[string]any {
	return Builder().TermLevel.Exists(index.QueryField(field))
}

func NamedQuery(name string, query map[string]any) map[string]any {
	return map[string]any{
		name: query,
	}
}

func NestedQuery(name string, query map[string]any) map[string]any {
	return Builder().Join.Nested(name, query)
}

func RangeQuery(index index.Index, field string, start, end int64) map[string]any {
	rangeQuery := Builder().TermLevel.Range
	condition := make([]func(*query.RangeParam), 0, 2)
	if start > 0 {
		condition = append(condition, rangeQuery.WithCompareOperate(query.CompareOperatorGTE, start))
	}
	if end > 0 {
		condition = append(condition, rangeQuery.WithCompareOperate(query.CompareOperatorLTE, end))
	}
	return rangeQuery(index.QueryField(field), condition...)
}

func StoredScriptQuery(id string, params map[string]any) map[string]any {
	scriptQuery := Builder().Specialized.Script
	return scriptQuery(scriptQuery.WithScriptId(id, params))
}

func TermsQuery(index index.Index, field string, values ...any) map[string]any {
	terms := Builder().TermLevel.Terms
	return terms(index.QueryField(field), terms.WithValue(values...))
}

func TermQuery(index index.Index, field string, value string) map[string]any {
	return Builder().TermLevel.Term(index.QueryField(field), value)
}
