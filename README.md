bond
====

Bond is database built on [Pebble](https://github.com/cockroachdb/pebble) the key-value store that's based
on LevelDB/RocksDB and is used in CockroachDB as a storage engine. It leverages Pebbles SSTables in order
to provide efficient query execution times in comparison to other solutions on the market.

Bond features:
- Tables
- Indexes
- Ordered Indexes
- Partial Indexes
- Custom Serialization / Deserialization

### Basics:

The example data structure:
```go
type ExampleStruct struct {
    Id          uint64 `json:"id"`
    Type        string `json:"type"`
    IsActive    bool   `json:"isActive"`
    Description string `json:"description"`
    Amount      uint64 `json:"amount"`
}
```

The Bond database open / close:
```go
db, err := bond.Open("example", &bond.Options{})
if err != nil {
    panic(err)
}

defer func() { _ = db.Close() }()
```

Table create:
```go
const (
    ExampleStructTableId bond.TableID = 1
)

ExampleStructTable := bond.NewTable[*ExampleStruct](
    // The database instance
    db,
    // The unique table identifier
    ExampleStructTableId,
    // The primary key building function
    func(b bond.KeyBuilder, es *ExampleStruct) []byte {
        return b.AddUint64Field(es.Id).Bytes()
    }
)
```

The index creation:
```go
ExampleStructTypeIndex := bond.NewIndex[*ExampleStruct](
    // The unique index identifier
    bond.PrimaryIndexID+1,
	// The function that determines index key
    func(b bond.KeyBuilder, es *ExampleStruct) []byte {
        return b.AddBytesField([]byte(es.Type)).Bytes()
    },
	// The function that determines index ordering
    bond.IndexOrderDefault[*ExampleStruct],
)
```

Insert:
```go
exapleStructs := []*ExampleStruct{
    {
        Id:          1,
        Type:        "test",
        IsActive:    true,
        Description: "test description",
        Amount:      1,
    },
}

err := ExampleStructTable.Insert(exapleStructs)
if err != nil {
    panic(err)
}
```

Update:
```go
exapleStructs := []*ExampleStruct{
    {
        Id:          1,
        Type:        "test",
        IsActive:    true,
        Description: "test description",
        Amount:      1,
    },
}

err := ExampleStructTable.Update(exapleStructs)
if err != nil {
    panic(err)
}
```

Delete:
```go
err := ExampleStructTable.Delete(&ExampleStruct{Id: 1})
if err != nil {
    panic(err)
}
```

Query:
```go
var exampleStructsFromQuery []*ExampleStruct
err := ExampleStructTable.Query().Execute(&exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Query using index:
```go
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, &ExampleStruct{Type: "test"}).
    Execute(&exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Query using index with filter:
```go
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, &ExampleStruct{Type: "test"}).
    Filter(func(es *ExampleStruct) bool {
        return es.Amount > 5
    }).
    Execute(&exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Query using index with filter and order:
```go
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, &ExampleStruct{Type: "test"}).
    Filter(func(es *ExampleStruct) bool {
        return es.Amount > 5
    }).
    Order(func(es *ExampleStruct, es2 *ExampleStruct) bool {
        return es.Amount < es2.Amount
    }).
    Execute(&exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Query using index with offset and limit:
```go
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, &ExampleStruct{Type: "test"}).
    Offset(1).
    Limit(2).
    Execute(&exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Please see working example: [here](https://github.com/go-bond/bond/blob/master/_examples/simple/main.go) 

### Advanced:

TODO

### Benchmarks:
```bash
/usr/local/Cellar/go/1.18.2/libexec/bin/go test -v ./... -bench . -run ^$
goos: darwin
goarch: amd64
pkg: github.com/go-bond/bond
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
BenchmarkBondTableQuery_1000000_Account_200_Order_Balance
BenchmarkBondTableQuery_1000000_Account_200_Order_Balance-8                                         	    1998	    595911 ns/op	  157070 B/op	    2218 allocs/op
BenchmarkBondTableQuery_1000000_Account_500_Order_Balance
BenchmarkBondTableQuery_1000000_Account_500_Order_Balance-8                                         	     688	   1595757 ns/op	  325645 B/op	    5521 allocs/op
BenchmarkBondTableQuery_1000000_Account_1000_Order_Balance
BenchmarkBondTableQuery_1000000_Account_1000_Order_Balance-8                                        	     321	   3627263 ns/op	  618876 B/op	   11026 allocs/op
BenchmarkBondTableQuery_MsgPack_1000000_Account_200_Order_Balance
BenchmarkBondTableQuery_MsgPack_1000000_Account_200_Order_Balance-8                                 	    2995	    392486 ns/op	  120911 B/op	    1419 allocs/op
BenchmarkBondTableQuery_MsgPack_1000000_Account_500_Order_Balance
BenchmarkBondTableQuery_MsgPack_1000000_Account_500_Order_Balance-8                                 	     956	   1099926 ns/op	  234998 B/op	    3521 allocs/op
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Order_Balance
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Order_Balance-8                                	     444	   2629749 ns/op	  437948 B/op	    7025 allocs/op
BenchmarkBondTableQuery_1000000_Account_1000_Limit_200
BenchmarkBondTableQuery_1000000_Account_1000_Limit_200-8                                            	    2890	    389682 ns/op	  120759 B/op	    1417 allocs/op
BenchmarkBondTableQuery_1000000_Account_1000_Limit_500
BenchmarkBondTableQuery_1000000_Account_1000_Limit_500-8                                            	     718	   1581316 ns/op	  325303 B/op	    5519 allocs/op
BenchmarkBondTableQuery_1000000_Account_1000_Offset_200_Limit_500
BenchmarkBondTableQuery_1000000_Account_1000_Offset_200_Limit_500-8                                 	     678	   1599294 ns/op	  325138 B/op	    5520 allocs/op
BenchmarkBondTableQuery_1000000_Account_1000_Offset_500_Limit_500
BenchmarkBondTableQuery_1000000_Account_1000_Offset_500_Limit_500-8                                 	     664	   1654690 ns/op	  325901 B/op	    5520 allocs/op
BenchmarkBondTableQuery_1000000_Account_1000_Offset_500_Limit_200
BenchmarkBondTableQuery_1000000_Account_1000_Offset_500_Limit_200-8                                 	    1873	    631868 ns/op	  157273 B/op	    2217 allocs/op
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Limit_200
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Limit_200-8                                    	    3103	    376566 ns/op	  120751 B/op	    1417 allocs/op
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Limit_500
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Limit_500-8                                    	    1119	   1048182 ns/op	  234353 B/op	    3519 allocs/op
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_200_Limit_500
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_200_Limit_500-8                         	     963	   1061069 ns/op	  234487 B/op	    3519 allocs/op
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_500_Limit_500
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_500_Limit_500-8                         	     998	   1111740 ns/op	  234696 B/op	    3519 allocs/op
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_500_Limit_200
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_500_Limit_200-8                         	    2848	    422792 ns/op	  120970 B/op	    1417 allocs/op
BenchmarkBondTableQuery_MsgPack_1000000_OrderedIndex_Account_1000_Offset_500_Limit_200
BenchmarkBondTableQuery_MsgPack_1000000_OrderedIndex_Account_1000_Offset_500_Limit_200-8            	    2799	    423247 ns/op	  121021 B/op	    1417 allocs/op
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_500_Limit_200_OrderByBalance
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_500_Limit_200_OrderByBalance-8          	     465	   2571154 ns/op	  437285 B/op	    7025 allocs/op
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_500_Limit_200_OrderedIndexByBalance
BenchmarkBondTableQuery_MsgPack_1000000_Account_1000_Offset_500_Limit_200_OrderedIndexByBalance-8   	    2674	    422972 ns/op	  121016 B/op	    1417 allocs/op
BenchmarkBondTableInsert_1
BenchmarkBondTableInsert_1-8                                                                        	      50	  22683690 ns/op	   25535 B/op	      10 allocs/op
BenchmarkBondTableInsert_1000
BenchmarkBondTableInsert_1000-8                                                                     	      33	  37475356 ns/op	 1078253 B/op	    4045 allocs/op
BenchmarkBondTableInsert_1000000
BenchmarkBondTableInsert_1000000-8                                                                  	       1	1775909412 ns/op	1039541720 B/op	 4000204 allocs/op
BenchmarkBondTableInsert_MsgPack_1
BenchmarkBondTableInsert_MsgPack_1-8                                                                	      50	  23442021 ns/op	   25668 B/op	      12 allocs/op
BenchmarkBondTableInsert_MsgPack_1000
BenchmarkBondTableInsert_MsgPack_1000-8                                                             	      28	  36500537 ns/op	 1197585 B/op	    6047 allocs/op
BenchmarkBondTableInsert_MsgPack_1000000
BenchmarkBondTableInsert_MsgPack_1000000-8                                                          	       1	1806980279 ns/op	1151837920 B/op	 6000201 allocs/op
BenchmarkBondTableScan_1000
BenchmarkBondTableScan_1000-8                                                                       	     525	   2133587 ns/op	  460422 B/op	   12001 allocs/op
BenchmarkBondTableScan_1000000
BenchmarkBondTableScan_1000000-8                                                                    	       1	2097555681 ns/op	445856712 B/op	11991598 allocs/op
BenchmarkBondTableScanIndex_1000
BenchmarkBondTableScanIndex_1000-8                                                                  	     392	   2749043 ns/op	  445410 B/op	   12001 allocs/op
BenchmarkBondTableScanIndex_1000000
BenchmarkBondTableScanIndex_1000000-8                                                               	       1	8371929225 ns/op	500787000 B/op	12427267 allocs/op
BenchmarkBondTableScan_MsgPack_1000
BenchmarkBondTableScan_MsgPack_1000-8                                                               	    1076	    962907 ns/op	  221294 B/op	    5020 allocs/op
BenchmarkBondTableScan_MsgPack_1000000
BenchmarkBondTableScan_MsgPack_1000000-8                                                            	       2	 934018484 ns/op	196985956 B/op	 5000883 allocs/op
BenchmarkBondTableScanIndex_MsgPack_1000
BenchmarkBondTableScanIndex_MsgPack_1000-8                                                          	     699	   1584679 ns/op	  245061 B/op	    6020 allocs/op
BenchmarkBondTableScanIndex_MsgPack_1000000
BenchmarkBondTableScanIndex_MsgPack_1000000-8                                                       	       1	2571536775 ns/op	386294272 B/op	 6918213 allocs/op
BenchmarkBondTableScanIndex_1000000_Skip_Through
BenchmarkBondTableScanIndex_1000000_Skip_Through-8                                                  	      19	  59767337 ns/op	  380512 B/op	      86 allocs/op
Benchmark_KeyBuilder
Benchmark_KeyBuilder-8                                                                              	78375480	        14.30 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/go-bond/bond	504.088s

```
