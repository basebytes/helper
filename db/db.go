package db

import (
	"strings"

	"github.com/basebytes/component/database/rdb"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	defaultPageSize = 10
	maxPageSize     = 100
)

func Create(name string, table any) error {
	return conn(name).Create(table).Error
}

func FirstOrCreate(name string, data rdb.Data, conditions ...rdb.Condition) error {
	return conn(name).FirstOrCreate(data, conditions...).Error
}

func CreateIgnoreConflicts(name string, conflicts []string, values any) error {
	return conn(name).CreateIgnoreConflicts(conflicts, values).Error
}

func UpdatesNotEmpty(name string, table rdb.Data) error {
	return conn(name).UpdatesNotEmpty(table).Error
}

func AssociationUpdatesNotEmpty(name string, table rdb.Data, tableName string, values ...rdb.Data) error {
	return conn(name).AssociationUpdatesNotEmpty(table, name, values)
}

func UpdatesByCondition(name string, table rdb.Data, conditions ...rdb.Condition) error {
	return conn(name).UpdatesByCondition(table, conditions...).Error
}

func UpdatesWithConditionWithRowsAffected(name string, table rdb.Data, values any, conditions ...rdb.Condition) (int64, error) {
	result := conn(name).UpdatesWithCondition(table, values, conditions...)
	return result.RowsAffected, result.Error
}

func UpdatesByConditionWithRowsAffected(name string, table rdb.Data, conditions ...rdb.Condition) (int64, error) {
	result := conn(name).UpdatesByCondition(table, conditions...)
	return result.RowsAffected, result.Error
}

func UpdateColumnsById(name string, table rdb.Data, columns ...string) error {
	return conn(name).UpdateColumnsById(table, columns...).Error
}

func BatchUpdatesNotEmpty(name string, tables []rdb.Data) (err error) {
	return conn(name).BatchUpdatesNotEmpty(tables)
}

func PageQuery(name string, table rdb.Data, result any, page rdb.Condition, conditions ...rdb.Condition) (int64, error) {
	return conn(name).PageQuery(table, result, page, conditions...)
}

func Upsert(name string, conflicts, updates []string, values any) error {
	return conn(name).Upsert(conflicts, updates, values).Error
}

func FindById(name string, table rdb.Data, id any) error {
	return conn(name).FindById(table, id).Error
}

func FindFirstByCondition(name string, table rdb.Data) error {
	return conn(name).FindFirstByCondition(table).Error
}

func FindByConditions(name string, table rdb.Data, result any, conditions ...rdb.Condition) error {
	return conn(name).GetData(table, result, conditions...).Error
}

func Raw(name string, result any, sql string, args ...any) error {
	return conn(name).Raw(sql, args...).Scan(result).Error
}

func Count(name string, table rdb.Data, conditions ...rdb.Condition) (int64, error) {
	return conn(name).Count(table, conditions...)
}

func DeleteByCondition(name string, table rdb.Data, conditions ...rdb.Condition) error {
	return conn(name).DeleteByCondition(table, conditions...).Error
}

func DeleteByConditionWithRowsAffected(name string, table rdb.Data, conditions ...rdb.Condition) (int64, error) {
	result := conn(name).DeleteByCondition(table, conditions...)
	return result.RowsAffected, result.Error
}

func SubQuery(name string, table rdb.Data, conditions ...rdb.Condition) *gorm.DB {
	return conn(name).SubQuery(table, conditions...)
}

func Transaction(name string, fc func(tx *gorm.DB) error) error {
	return conn(name).Transaction(fc)
}

func PageCondition(ctx *gin.Context) (f rdb.Condition, err error) {
	var page = Page{}
	if err = ctx.BindQuery(&page); err == nil {
		if page.Limit <= 0 {
			page.Limit = defaultPageSize
		} else if page.Limit > maxPageSize {
			page.Limit = maxPageSize
		}
		f = rdb.Page(page.Offset, page.Limit)
	}
	return
}

func IsMissValueError(err error) bool {
	return err != nil && strings.Index(err.Error(), "doesn't have a default value") >= 0
}

func IsDuplicateKeyError(err error) bool {
	return err != nil && strings.Index(err.Error(), "Duplicate entry") >= 0
}

func IsNotFoundError(err error) bool {
	return err != nil && strings.Index(err.Error(), "record not found") >= 0
}

func IsConnectionRefusedError(err error) bool {
	return err != nil && strings.Index(err.Error(), "connection refused") >= 0
}

func conn(name string) (c *rdb.Instance) {
	c, _ = rdb.GetConnection(name)
	return
}

type Page struct {
	Offset int `form:"offset"`
	Limit  int `form:"limit"`
}
