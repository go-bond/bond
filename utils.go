package bond

import "reflect"

func makeNew[T any]() T {
	var v T
	if typ := reflect.TypeOf(v); typ.Kind() == reflect.Ptr {
		elem := typ.Elem()
		return reflect.New(elem).Interface().(T)
	} else {
		return *new(T)
	}
}

func makeNewAny(v any) any {
	return makeValue(reflect.TypeOf(v)).Interface()
}

func makeValue(elem reflect.Type) reflect.Value {
	if elem.Kind() == reflect.Ptr {
		nextElem := elem.Elem()
		if nextElem.Kind() == reflect.Ptr {
			ptrElem := nextElem.Elem()
			newValue := reflect.New(nextElem)
			newValue.Elem().Set(makeValue(ptrElem))
			return newValue
		}
		return reflect.New(nextElem)
	} else {
		return reflect.New(elem)
	}
}

func findRootInterface(v reflect.Value) any {
	if v.Kind() == reflect.Ptr {
		if v.Elem().Kind() != reflect.Ptr {
			return v.Interface()
		}
		return findRootInterface(v.Elem())
	} else {
		return v.Interface()
	}
}
