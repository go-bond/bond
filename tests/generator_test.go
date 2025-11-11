package bond_tests

//go:generate go run go.uber.org/mock/mockgen -destination ./mock/bond/bond.go -package bondmock github.com/go-bond/bond Filter,FilterStorer,TableScanner
