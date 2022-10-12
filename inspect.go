package bond

import (
	"context"
	"fmt"
	"reflect"

	"github.com/fatih/structs"
)

type Inspect interface {
	Tables() ([]string, error)
	Indexes(table string) ([]string, error)
	EntryFields(table string) (map[string]string, error)

	Query(ctx context.Context, table string, index string, indexSelector map[string]interface{}, filter map[string]interface{}, limit uint64, after map[string]interface{}) ([]map[string]interface{}, error)
}

type inspect struct {
	tableInfos []TableInfo
}

func NewInspect(ti []TableInfo) (Inspect, error) {
	return &inspect{tableInfos: ti}, nil
}

func (in *inspect) Tables() ([]string, error) {
	var tables []string

	for _, ti := range in.tableInfos {
		tables = append(tables, ti.Name())
	}

	return tables, nil
}

func (in *inspect) Indexes(table string) ([]string, error) {
	var indexes []string

	for _, ti := range in.tableInfos {
		if table == ti.Name() {
			for _, ii := range ti.Indexes() {
				indexes = append(indexes, ii.Name())
			}
			return indexes, nil
		}
	}

	return nil, fmt.Errorf("table not found")
}

func (in *inspect) EntryFields(table string) (map[string]string, error) {
	for _, ti := range in.tableInfos {
		if table == ti.Name() {
			emptyEntry := makeValue(ti.EntryType())
			if emptyEntry.Kind() == reflect.Ptr {
				emptyEntry = emptyEntry.Elem()
			}

			fieldsAndTypes := make(map[string]string)
			for fieldName, value := range structs.Map(emptyEntry.Interface()) {
				fieldsAndTypes[fieldName] = reflect.ValueOf(value).Kind().String()
			}
			return fieldsAndTypes, nil
		}
	}
	return nil, fmt.Errorf("table not found")
}

func (in *inspect) Query(ctx context.Context, table string, index string, indexSelector map[string]interface{}, filter map[string]interface{}, limit uint64, after map[string]interface{}) ([]map[string]interface{}, error) {
	if table == "" {
		return nil, fmt.Errorf("table can not be empty")
	}

	tableInfo, indexInfo, err := in.findTableAndIndexInfo(table, index)
	if err != nil {
		return nil, err
	}

	result := reflect.New(reflect.SliceOf(tableInfo.EntryType()))

	tableValue := reflect.ValueOf(tableInfo)
	queryValue := tableValue.MethodByName("Query").Call([]reflect.Value{})[0]

	if indexInfo.Name() != PrimaryIndexName {
		indexValue := reflect.ValueOf(indexInfo)
		indexSelectorValue := makeValue(tableInfo.EntryType())
		err = setFields(indexSelectorValue, indexSelector)
		if err != nil {
			return nil, err
		}

		queryValue = queryValue.MethodByName("With").Call([]reflect.Value{indexValue, indexSelectorValue})[0]
	}

	if filter != nil {
		filterFuncType := reflect.FuncOf([]reflect.Type{tableInfo.EntryType()}, []reflect.Type{reflect.TypeOf(false)}, false)
		filterFunc := reflect.MakeFunc(filterFuncType, func(args []reflect.Value) (results []reflect.Value) {
			ret := true
			row := structs.Map(args[0].Interface())

			for filterField, filterInterface := range filter {
				fv := reflect.ValueOf(filterInterface)
				if rowInterface, ok := row[filterField]; ok {
					rv := reflect.ValueOf(rowInterface)
					if fv.Kind() != rv.Kind() {
						if fv.CanConvert(rv.Type()) {
							filterInterface = fv.Convert(rv.Type()).Interface()
						} else {
							ret = false
							break
						}
					}

					if !reflect.DeepEqual(filterInterface, rowInterface) {
						ret = false
						break
					}
				} else {
					ret = false
					break
				}
			}

			return []reflect.Value{reflect.ValueOf(ret)}
		})

		queryValue = queryValue.MethodByName("Filter").Call([]reflect.Value{filterFunc})[0]
	}

	if limit > 0 {
		queryValue = queryValue.MethodByName("Limit").Call([]reflect.Value{reflect.ValueOf(limit)})[0]
	}

	if after != nil {
		afterValue := makeValue(tableInfo.EntryType())
		err = setFields(afterValue, after)
		if err != nil {
			return nil, err
		}

		queryValue = queryValue.MethodByName("After").Call([]reflect.Value{afterValue})[0]
	}

	execValues := queryValue.MethodByName("Execute").Call(
		[]reflect.Value{
			reflect.ValueOf(ctx),
			result,
		},
	)
	if execValues[0].Interface() != nil {
		return nil, execValues[0].Interface().(error)
	}

	resultArray := result.Elem()
	resultMapArray := make([]map[string]interface{}, 0)
	for i := 0; i < resultArray.Len(); i++ {
		row := resultArray.Index(i)
		resultMapArray = append(resultMapArray, structs.Map(row.Interface()))
	}

	return resultMapArray, nil
}

func (in *inspect) findTableAndIndexInfo(table string, index string) (TableInfo, IndexInfo, error) {
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

func setFields(val reflect.Value, toSet map[string]interface{}) error {
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	for fieldNameToSet, valueToSetInterface := range toSet {
		fields := structs.Map(val.Interface())
		if _, ok := fields[fieldNameToSet]; ok {
			vt := reflect.ValueOf(valueToSetInterface)
			vv := val.FieldByName(fieldNameToSet)

			if vt.Kind() != vv.Kind() {
				if vt.CanConvert(vv.Type()) {
					vt = vt.Convert(vv.Type())
				} else {
					return fmt.Errorf("field type mismatch %s != %s", vt.Kind().String(), vv.Kind().String())
				}
			}

			vv.Set(vt)
		} else {
			return fmt.Errorf("field '%s' not found", fieldNameToSet)
		}
	}

	return nil
}
