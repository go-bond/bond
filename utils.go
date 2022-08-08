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
