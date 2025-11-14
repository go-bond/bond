package mocks_generator_tests

//go:generate go run go.uber.org/mock/mockgen -destination ./testing/mocks/bond/bond.go -package bondmock github.com/go-bond/bond Filter,FilterStorer,TableScanner,Batch
