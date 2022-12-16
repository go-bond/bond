package utils

import (
	"reflect"
)

func MakeNew[T any]() T {
	var v T
	if typ := reflect.TypeOf(v); typ.Kind() == reflect.Ptr {
		elem := typ.Elem()
		return reflect.New(elem).Interface().(T)
	} else {
		return *new(T)
	}
}

func MakeNewAny(v any) any {
	return MakeValue(reflect.TypeOf(v)).Interface()
}

func MakeValue(elem reflect.Type) reflect.Value {
	if elem.Kind() == reflect.Ptr {
		nextElem := elem.Elem()
		if nextElem.Kind() == reflect.Ptr {
			ptrElem := nextElem.Elem()
			newValue := reflect.New(nextElem)
			newValue.Elem().Set(MakeValue(ptrElem))
			return newValue
		}
		return reflect.New(nextElem)
	} else {
		return reflect.New(elem)
	}
}

func FindRootInterface(v reflect.Value) any {
	if v.Kind() == reflect.Ptr {
		if v.Elem().Kind() != reflect.Ptr {
			return v.Interface()
		}
		return FindRootInterface(v.Elem())
	} else {
		return v.Interface()
	}
}

func ToSliceAny[T any](in []T) []any {
	out := make([]any, len(in))
	for i, data := range in {
		out[i] = data
	}
	return out
}
