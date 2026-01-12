package elastic

import (
	"github.com/basebytes/elastic/fields"
	"github.com/basebytes/elastic/filter"
	"github.com/basebytes/elastic/index"
	"github.com/basebytes/elastic/query"
)

func NewDefaultIndexQuery(index index.Index, filters, not filter.Filter, extend fields.Extend, fieldsNotReturn ...string) *query.IndexQuery {
	return query.NewIndexQuery(index, filters, not, extend, fields.NewFields(), fields.NewFields(), DefaultQueryBuilder, DefaultAggBuilder, fieldsNotReturn...)
}
