package bond

import (
	"context"
	"fmt"
	"reflect"

	"github.com/fatih/structs"
)

type Inspect interface {
	Tables() []string
	Indexes(table string) []string
	Query(table string, index string, indexSelector map[string]interface{}, filter map[string]interface{}, limit int, after *string) ([]map[string]interface{}, error)
}

type inspect struct {
	tableInfos []TableInfo
}

func NewInspect(ti []TableInfo) (Inspect, error) {
	return &inspect{tableInfos: ti}, nil
}

func (in *inspect) Tables() []string {
	var tables []string

	for _, ti := range in.tableInfos {
		tables = append(tables, ti.Name())
	}
	return tables
}

func (in *inspect) Indexes(table string) []string {
	var indexes []string

	for _, ti := range in.tableInfos {
		if table == ti.Name() {
			for _, ii := range ti.Indexes() {
				indexes = append(indexes, ii.Name())
			}
		}
	}
	return indexes
}

func (in *inspect) Query(table string, index string, indexSelector map[string]interface{}, filter map[string]interface{}, limit int, after *string) ([]map[string]interface{}, error) {
	if table == "" {
		return nil, fmt.Errorf("table can not be empty")
	}

	tableInfo, _, err := in.findTableAndIndex(table, index)
	if err != nil {
		return nil, err
	}

	result := reflect.New(reflect.SliceOf(tableInfo.Type()))

	tableValue := reflect.ValueOf(tableInfo)
	queryValue := tableValue.MethodByName("Query").Call([]reflect.Value{})[0]

	queryValue.MethodByName("Execute").Call(
		[]reflect.Value{
			reflect.ValueOf(context.Background()),
			result,
		},
	)

	resultArray := result.Elem()
	resultMapArray := make([]map[string]interface{}, 0)
	for i := 0; i < resultArray.Len(); i++ {
		row := resultArray.Index(i)
		resultMapArray = append(resultMapArray, structs.Map(row.Interface()))
	}

	return resultMapArray, nil
}

func (in *inspect) findTableAndIndex(table string, index string) (TableInfo, IndexInfo, error) {
	if index == "" {
		index = PrimaryIndexName
	}

	var tableInfo TableInfo
	var indexInfo IndexInfo

	for _, t := range in.tableInfos {
		if t.Name() == table {
			tableInfo = t
			for _, i := range t.Indexes() {
				if i.Name() == index {
					indexInfo = i
					break
				}
			}
			break
		}
	}

	if tableInfo == nil {
		return nil, nil, fmt.Errorf("table not found")
	}

	if indexInfo == nil {
		return nil, nil, fmt.Errorf("index not found")
	}

	return tableInfo, indexInfo, nil
}
