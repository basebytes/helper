package elastic

import (
	"sort"

	"github.com/basebytes/elastic-go/service/constructor/query"
	"github.com/basebytes/elastic/fields"
	"github.com/basebytes/elastic/filter"
	"github.com/basebytes/elastic/index"
)

func CheckNumbers(fields ...[]*filter.NumberRanges) (invalid bool) {
	for _, numbers := range fields {
		for _, number := range numbers {
			if (number.Start == 0 && number.End == 0) || (number.End != 0 && number.Start > number.End) {
				invalid = true
				break
			}
		}
	}
	return
}

func Exists(index index.Index, filters *[]map[string]any, fields []string) {
	for _, field := range fields {
		existsQuery := ExistsQuery(index, field)
		if p, nested := index.IsNestedField(field); nested {
			existsQuery = NestedQuery(p, existsQuery)
		}
		*filters = append(*filters, existsQuery)
	}
}

func NumberRange(index index.Index, filters *[]map[string]any, field string, numbers []*filter.NumberRanges) {
	switch len(numbers) {
	case 1:
		start, end := numbers[0].Start, numbers[0].End
		if start > 0 || end > 0 {
			*filters = append(*filters, RangeQuery(index, field, start, end))
		}
	case 0:
	default:
		subQueries := make([]map[string]any, 0, len(numbers))
		for _, number := range numbers {
			start, end := number.Start, number.End
			if start > 0 || end > 0 {
				subQueries = append(subQueries, RangeQuery(index, field, start, end))
			}
		}
		*filters = append(*filters, BoolQuery(BoolQueryClauseParam(query.ClauseTypeShould, subQueries...), AtLeastMatchOne))
	}
}

func Terms(index index.Index, parent, filters *[]map[string]any, field string, values []any, nullFields map[string]struct{}) {
	var nullValueQuery, termsQuery map[string]any
	p, nested := index.IsNestedField(field)
	if _, OK := nullFields[field]; OK {
		if nullValueQuery = ExistsQuery(index, field); nested {
			nullValueQuery = NestedQuery(p, nullValueQuery)
		}
		nullValueQuery = BoolQuery(BoolQueryClauseParam(query.ClauseTypeMustNot, nullValueQuery))
	}
	if len(values) > 0 {
		termsQuery = TermsQuery(index, field, values...)
		if nullValueQuery != nil && nested {
			termsQuery = NestedQuery(p, termsQuery)
		}
	}

	if nullValueQuery != nil && termsQuery != nil {
		*parent = append(*parent, BoolQuery(BoolQueryClauseParam(query.ClauseTypeShould, nullValueQuery, termsQuery), AtLeastMatchOne))
	} else if nullValueQuery != nil {
		*parent = append(*parent, nullValueQuery)
	} else if termsQuery != nil {
		*filters = append(*filters, termsQuery)
	}
}

var DefaultQueryBuilder = func(filter, not filter.Filter) map[string]any {
	clause := make([]func(*query.BoolParam), 0, 2)
	if filter != nil {
		if filters := filter.Filters(); len(filters) > 0 {
			clause = append(clause, BoolQueryClauseParam(query.ClauseTypeFilter, filters...))
		}
	}
	if not != nil {
		if nots := not.Filters(); len(nots) > 0 {
			clause = append(clause, BoolQueryClauseParam(query.ClauseTypeMustNot, nots...))
		}
	}
	if len(clause) > 0 {
		return BoolQuery(clause...)
	}
	return nil
}

var DefaultAggBuilder = func(group, stats fields.Fields, extend fields.Extend) (map[string]any, string) {
	if stats == nil || stats.Len() == 0 {
		return nil, ""
	}
	resultAgg := make(map[string]any, stats.Len())
	nestedAgg := make(map[string]map[string]any, 2)
	var lastField string
	_orders := newOrders()
	//构造统计语句
	for _, field := range stats.Fields() {
		if agg := extend.Statistics(field); agg != nil {
			if parent, isNested := extend.IsNestedField(field); isNested {
				subs, OK := nestedAgg[parent]
				if !OK {
					subs = make(map[string]any)
					nestedAgg[parent] = subs
				}
				subs[field.Name()] = agg
			} else {
				resultAgg[field.Name()] = agg
			}
		}
	}
	for k, v := range nestedAgg {
		resultAgg[k] = NestedAgg(k, v)
	}
	groupFields := make(map[string][]fields.Field, 3)
	groups := group.Fields()
	for i := group.Len() - 1; i >= 0; i-- {
		field := groups[i]
		parent, _ := extend.IsNestedField(field)
		groupFields[parent] = append(groupFields[parent], field)
		_orders.append(parent, i)
	}

	for i, _order := range *_orders {
		if i == 0 && _order.Name != "" {
			resultAgg = NamedAgg(index.Item, ReverseNestedAgg("", resultAgg))
		}
		for _, field := range groupFields[_order.Name] {
			resultAgg = NamedAgg(field.Name(), extend.Group(field, resultAgg))
		}

		switch _order.Name {
		case "":
			if i != _orders.Len()-1 {
				resultAgg = NamedAgg(index.Item, ReverseNestedAgg("", resultAgg))
			}
		default:
			if i == _orders.Len()-1 || _orders.get(i+1).Name == "" {
				resultAgg = NamedAgg(_order.Name, NestedAgg(_order.Name, resultAgg))
			} else {
				resultAgg = NamedAgg(_order.Name, ReverseNestedAgg(_order.Name, resultAgg))
			}
		}
	}

	if len(groupFields) > 0 {
		lastField = groupFields[_orders.get(0).Name][0].Name()
	}
	return resultAgg, lastField
}

type fieldOrder struct {
	Name string
	Pos  int
}

type fieldOrders []*fieldOrder

func newOrders() *fieldOrders {
	return &fieldOrders{}
}

func (o *fieldOrders) Less(i, j int) bool {
	return (*o)[i].Pos > (*o)[j].Pos
}

func (o *fieldOrders) Swap(i, j int) {
	(*o)[i], (*o)[j] = (*o)[j], (*o)[i]
}

func (o *fieldOrders) Len() int {
	return len(*o)
}

func (o *fieldOrders) append(name string, pos int) {
	for i := 0; i < o.Len(); i++ {
		if (*o)[i].Name == name {
			if (*o)[i].Pos > pos {
				(*o)[i].Pos = pos
				sort.Sort(o)
			}
			return
		}
	}
	*o = append(*o, &fieldOrder{Name: name, Pos: pos})
	if o.Len() > 1 {
		sort.Sort(o)
	}
}

func (o *fieldOrders) get(pos int) *fieldOrder {
	if pos >= o.Len() {
		return nil
	}
	return (*o)[pos]
}
