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
- Queries
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

ExampleStructTable := bond.NewTable[*ExampleStruct](bond.TableOptions[*ExampleStruct]{
    // The database instance
    DB:        db,
    // The unique table identifier
    TableID:   ExampleStructTableID,
    // The table name for inspect purposes
    TableName: "example_stuct_table",
    TablePrimaryKeyFunc: func(b bond.KeyBuilder, es *ExampleStruct) []byte {
        return b.AddUint64Field(es.Id).Bytes()
    },
})
```

The index creation:
```go
ExampleStructTypeIndex := bond.NewIndex[*ExampleStruct](bond.IndexOptions[*ExampleStruct]{
    // The unique index identifier
    IndexID:   bond.PrimaryIndexID + 1,
    // The index name for inspect purposes
    IndexName: "type_idx",
    // The function that determines index key
    IndexKeyFunc: func(b bond.KeyBuilder, es *ExampleStruct) []byte {
        return b.AddBytesField([]byte(es.Type)).Bytes()
    },
    IndexOrderFunc: bond.IndexOrderDefault[*ExampleStruct],
})
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

err := ExampleStructTable.Insert(context.Background(), exapleStructs)
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

err := ExampleStructTable.Update(context.Background(), exapleStructs)
if err != nil {
    panic(err)
}
```

Delete:
```go
err := ExampleStructTable.Delete(context.Background(), &ExampleStruct{Id: 1})
if err != nil {
    panic(err)
}
```

Query:
```go
var exampleStructsFromQuery []*ExampleStruct
err := ExampleStructTable.Query().Execute(context.Background(), bond.NewSelectorPoint(&exampleStructsFromQuery))
if err != nil {
    panic(err)
}
```

Query using index:
```go
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, bond.NewSelectorPoint(&ExampleStruct{Type: "test"})).
    Execute(context.Background(), &exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Query using index with filter:
```go
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, bond.NewSelectorPoint(&ExampleStruct{Type: "test"})).
    Filter(func(es *ExampleStruct) bool {
        return es.Amount > 5
    }).
    Execute(context.Background(), &exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Query using index with filter and order:
```go
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, bond.NewSelectorPoint(&ExampleStruct{Type: "test"})).
    Filter(func(es *ExampleStruct) bool {
        return es.Amount > 5
    }).
    Order(func(es *ExampleStruct, es2 *ExampleStruct) bool {
        return es.Amount < es2.Amount
    }).
    Execute(context.Background(), &exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Query using index with offset and limit:
```go
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, bond.NewSelectorPoint(&ExampleStruct{Type: "test"})).
    Offset(1).
    Limit(2).
    Execute(context.Background(), &exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Query using index with cursor:
```go
var exampleStructsFromQuery []*ExampleStruct

// page 1, page size 10
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, bond.NewSelectorPoint(&ExampleStruct{Type: "test"})).
    Limit(10).
    Execute(context.Background(), &exampleStructsFromQuery)
if err != nil {
    panic(err)
}

// page 2, page size 10
err := ExampleStructTable.Query().
    With(ExampleStructTypeIndex, bond.NewSelectorPoint(exampleStructsFromQuery[9])).
    Limit(10).
    Execute(context.Background(), &exampleStructsFromQuery)
if err != nil {
    panic(err)
}
```

Please see working example: [here](https://github.com/go-bond/bond/blob/master/_examples/simple/main.go) 

### Advanced:

TODO

### Benchmarks:
```bash
goos: darwin
goarch: amd64
pkg: github.com/go-bond/bond/_benchmarks
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
BenchmarkTableDeleteSuite/MsgpackSerializer/Delete_1                                                                                      55      21435621 ns/op               46.65 op/s          46761 B/op          8 allocs/op   
BenchmarkTableDeleteSuite/MsgpackSerializer/Delete_10                                                                                     57      22982539 ns/op               43.51 op/s          48793 B/op          9 allocs/op   
BenchmarkTableDeleteSuite/MsgpackSerializer/Delete_100                                                                                    51      22071951 ns/op               45.31 op/s          76952 B/op         15 allocs/op   
BenchmarkTableDeleteSuite/MsgpackSerializer/Delete_1000                                                                                   56      25474395 ns/op               39.26 op/s         437938 B/op         38 allocs/op   
BenchmarkTableDeleteSuite/MsgpackSerializer/Delete_10000                                                                                   9     126136403 ns/op                7.928 op/s       5600129 B/op        348 allocs/op   
BenchmarkTableDeleteSuite/MsgpackSerializer/Delete_100000                                                                                  4     257728666 ns/op                3.880 op/s      38339220 B/op        473 allocs/op   
BenchmarkTableDeleteSuite/MsgpackSerializer/Delete_1000000                                                                                 1    1779859111 ns/op                0.5618 op/s     443109856 B/op      2174 allocs/op  
BenchmarkTableExistSuite/MsgpackSerializer/Exist_True                                                                                 356911          3507 ns/op           285225 op/s             21776 B/op          2 allocs/op   
BenchmarkTableExistSuite/MsgpackSerializer/Exist_False                                                                                327555          3341 ns/op           299312 op/s             21776 B/op          2 allocs/op   
BenchmarkTableGetSuite/MsgpackSerializer/Get_Row_Exist                                                                                274851          4608 ns/op           217061 op/s             21929 B/op          7 allocs/op   
BenchmarkTableGetSuite/MsgpackSerializer/Get_Row_Does_Not_Exist                                                                       214294          5513 ns/op           181389 op/s             22794 B/op         19 allocs/op   
BenchmarkTableInsertSuite/MsgpackSerializer/Insert_1                                                                                      52      22040371 ns/op               45.37 op/s          46106 B/op         15 allocs/op   
BenchmarkTableInsertSuite/MsgpackSerializer/Insert_10                                                                                     52      21085066 ns/op               47.43 op/s          55167 B/op         71 allocs/op   
BenchmarkTableInsertSuite/MsgpackSerializer/Insert_100                                                                                    55      22587303 ns/op               44.27 op/s         172031 B/op        618 allocs/op   
BenchmarkTableInsertSuite/MsgpackSerializer/Insert_1000                                                                                   39      33895317 ns/op               29.50 op/s        1328954 B/op       6052 allocs/op   
BenchmarkTableInsertSuite/MsgpackSerializer/Insert_10000                                                                                  10     167267479 ns/op                5.978 op/s      12690833 B/op      64187 allocs/op   
BenchmarkTableInsertSuite/MsgpackSerializer/Insert_100000                                                                                  3     478770643 ns/op                2.089 op/s      135244917 B/op    604687 allocs/op  
BenchmarkTableInsertSuite/MsgpackSerializer/Insert_1000000                                                                                 1    4105578035 ns/op                0.2436 op/s     1316474888 B/op  6000308 allocs/op 
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_0                                                            1    30996623071 ns/op               0.03226 op/s   4271256224 B/op  100038541 allocs/op
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_500                                                       1918        700731 ns/op             1427 op/s            132644 B/op       2519 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_1000                                                       914       1329632 ns/op              752.1 op/s          229947 B/op       5021 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_5000                                                       175       6398196 ns/op              156.3 op/s          971058 B/op      25023 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_10000                                                       81      13246979 ns/op               75.49 op/s        2000515 B/op      50026 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_100000                                                       7     181796024 ns/op                5.501 op/s      20148132 B/op     500042 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_0                                                            1    27678424450 ns/op               0.03613 op/s   4162342944 B/op  100005887 allocs/op
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_500_Limit_1000                                                     927       1288063 ns/op              776.4 op/s          228820 B/op       5019 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_1000_Limit_1000                                                    968       1233684 ns/op              810.6 op/s          228819 B/op       5019 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_5000_Limit_1000                                                    772       1658814 ns/op              602.8 op/s          228824 B/op       5019 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_10000_Limit_1000                                                   500       2269328 ns/op              440.7 op/s          228838 B/op       5019 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_100000_Limit_1000                                                   56      23033768 ns/op               43.41 op/s         229017 B/op       5019 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_1000000_Limit_1000                                                   5     244945729 ns/op                4.083 op/s        233812 B/op       5027 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_Default_Offset_10000000_Limit_1000                                                  1    2597524128 ns/op                0.3850 op/s       233384 B/op       5029 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_0                                                     1    68874325866 ns/op               0.01452 op/s   4482103144 B/op  120003295 allocs/op
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_500                                                 780       1629769 ns/op              613.6 op/s          139004 B/op       3014 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_1000                                                367       3318433 ns/op              301.3 op/s          244011 B/op       6016 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_5000                                                 69      15977567 ns/op               62.59 op/s        1051362 B/op      30020 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_10000                                                34      35823646 ns/op               27.91 op/s        2161064 B/op      60023 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_100000                                                3     355105585 ns/op                2.816 op/s      21754522 B/op     600052 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_0                                                     1    67871699169 ns/op               0.01473 op/s   4481792008 B/op  120000143 allocs/op
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_500_Limit_1000                                              345       3480769 ns/op              287.3 op/s          244864 B/op       6016 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_1000_Limit_1000                                             351       3515941 ns/op              284.4 op/s          244885 B/op       6016 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_5000_Limit_1000                                             292       4063913 ns/op              246.1 op/s          244875 B/op       6016 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_10000_Limit_1000                                            259       4656679 ns/op              214.7 op/s          244910 B/op       6016 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_100000_Limit_1000                                            75      13553063 ns/op               73.78 op/s         244986 B/op       6016 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_1000000_Limit_1000                                            8     134739117 ns/op                7.422 op/s        247848 B/op       6024 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_10000000_Limit_1000                                           1    1259131522 ns/op                0.7942 op/s       268176 B/op       6053 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_0                                     1    155749201000 ns/op              0.006421 op/s 4482067744 B/op   120002569 allocs/op
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_500                                 741       1702388 ns/op              587.4 op/s          139803 B/op       3014 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_1000                                338       3654652 ns/op              273.6 op/s          244804 B/op       6016 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_5000                                 27      45435742 ns/op               22.01 op/s        1052697 B/op      30021 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_10000                                15      79228908 ns/op               12.62 op/s        2162754 B/op      60025 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_100000                                2     839746535 ns/op                1.191 op/s      21759216 B/op     600056 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_0                                     1    151634938487 ns/op              0.006595 op/s 4481832976 B/op   120000234 allocs/op
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_500_Limit_1000                              357       3392862 ns/op              294.7 op/s          244861 B/op       6016 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_1000_Limit_1000                             307       4006004 ns/op              249.6 op/s          244871 B/op       6016 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_5000_Limit_1000                             279       4404264 ns/op              227.1 op/s          244896 B/op       6016 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_10000_Limit_1000                            249       4896684 ns/op              204.2 op/s          244908 B/op       6016 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_100000_Limit_1000                            87      13176678 ns/op               75.89 op/s         245055 B/op       6016 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_1000000_Limit_1000                            9     117464379 ns/op                8.513 op/s        341324 B/op       6111 allocs/op   
BenchmarkTableQuerySuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_10000000_Limit_1000                           1    1149570525 ns/op                0.8699 op/s       277512 B/op       6132 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_0                                         1    13473462540 ns/op               0.07422 op/s   3258296152 B/op  80018131 allocs/op 
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_500                                    4354        302066 ns/op             3311 op/s            108045 B/op       2020 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_1000                                   2012        573114 ns/op             1745 op/s            181893 B/op       4022 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_5000                                    469       2644978 ns/op              378.1 op/s          740143 B/op      20028 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_10000                                   205       5600330 ns/op              178.6 op/s         1541788 B/op      40033 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_100000                                   21      54555322 ns/op               18.33 op/s       15534291 B/op     400083 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_0_Limit_0                                         1    8372340234 ns/op                0.1194 op/s     3224929720 B/op 80020549 allocs/op 
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_500_Limit_1000                                 3174        406053 ns/op             2463 op/s            181538 B/op       4020 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_1000_Limit_1000                                2667        472707 ns/op             2115 op/s            180805 B/op       4019 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_5000_Limit_1000                                1539        864022 ns/op             1157 op/s            180808 B/op       4019 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_10000_Limit_1000                                783       1398672 ns/op              715.0 op/s          180817 B/op       4019 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_100000_Limit_1000                                60      20634266 ns/op               48.46 op/s         181116 B/op       4019 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_1000000_Limit_1000                                5     227228841 ns/op                4.401 op/s        184054 B/op       4027 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_Default_Offset_10000000_Limit_1000                               1    2277793494 ns/op                0.4390 op/s       197448 B/op       4133 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_0                                  1    48164156295 ns/op               0.02076 op/s   3522612528 B/op  100008652 allocs/op
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_500                             1213       1177553 ns/op              849.2 op/s          114989 B/op       2514 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_1000                             480       2845872 ns/op              351.4 op/s          196006 B/op       5016 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_5000                              99      11917934 ns/op               83.91 op/s         811226 B/op      25020 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_10000                             45      23932712 ns/op               41.78 op/s        1680789 B/op      50023 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_100000                             4     258984731 ns/op                3.861 op/s      16945386 B/op     500038 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_0_Limit_0                                  1    47339128888 ns/op               0.02112 op/s   3521755360 B/op  100000087 allocs/op
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_500_Limit_1000                           568       2154471 ns/op              464.2 op/s          196829 B/op       5016 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_1000_Limit_1000                          600       2182465 ns/op              458.2 op/s          196827 B/op       5016 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_5000_Limit_1000                          398       3152631 ns/op              317.2 op/s          196842 B/op       5016 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_10000_Limit_1000                         361       3346484 ns/op              298.8 op/s          196852 B/op       5016 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_100000_Limit_1000                         86      12360362 ns/op               80.90 op/s         196877 B/op       5016 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_1000000_Limit_1000                         9     130624754 ns/op                7.656 op/s        198771 B/op       5019 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddress_Offset_10000000_Limit_1000                        1    1320082639 ns/op                0.7575 op/s       214544 B/op       5043 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_0                  1    132789792932 ns/op              0.007531 op/s 3521987768 B/op   100002214 allocs/op
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_500             1106       1133327 ns/op              882.4 op/s          115783 B/op       2514 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_1000             429       2616724 ns/op              382.2 op/s          196834 B/op       5016 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_5000              38      31323974 ns/op               31.92 op/s         812298 B/op      25020 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_10000             16      68928247 ns/op               14.51 op/s        1682340 B/op      50025 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_100000             2     718200814 ns/op                1.392 op/s      16947612 B/op     500045 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_0                  1    131208252426 ns/op              0.007621 op/s 3521783120 B/op   100000178 allocs/op
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_500_Limit_1000           484       2566719 ns/op              389.6 op/s          196834 B/op       5016 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_1000_Limit_1000          498       3012091 ns/op              332.0 op/s          196809 B/op       5016 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_5000_Limit_1000          344       3427806 ns/op              291.7 op/s          196844 B/op       5016 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_10000_Limit_1000         296       4036202 ns/op              247.8 op/s          198722 B/op       5016 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_100000_Limit_1000         82      14511872 ns/op               68.91 op/s         196840 B/op       5016 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_1000000_Limit_1000         8     143265253 ns/op                6.980 op/s        199461 B/op       5024 allocs/op   
BenchmarkTableQueryWithTableSerializerSuite/MsgpackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_10000000_Limit_1000        1    1289410439 ns/op                0.7755 op/s       213304 B/op       5041 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_1                                                                                      112639         14949 ns/op            66894 op/s             43744 B/op         13 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_10                                                                                      70560         18569 ns/op            53856 op/s             45352 B/op         62 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_1000                                                                                     1080       1369065 ns/op              730.4 op/s          228055 B/op       5022 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_10000                                                                                     100      10793037 ns/op               92.65 op/s        2004723 B/op      50028 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_100000                                                                                     10     125962566 ns/op                7.939 op/s      20234629 B/op     500086 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_1000000                                                                                     1    1088019188 ns/op                0.9191 op/s     208238184 B/op   5001535 allocs/op  
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_1                                                                                 187689         18730 ns/op            53393 op/s             43760 B/op         11 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_10                                                                                 44625         26748 ns/op            37386 op/s             45512 B/op         69 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_1000                                                                                 625       2119371 ns/op              471.8 op/s          244672 B/op       6016 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_10000                                                                                 63      31597260 ns/op               31.65 op/s        2246877 B/op      60475 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_100000                                                                                 3     349450438 ns/op                2.862 op/s      27028565 B/op     628569 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_1000000                                                                                1    5135691707 ns/op                0.1947 op/s     356617224 B/op   6750521 allocs/op  
BenchmarkTableScanSuite/MsgpackSerializer/Scan_Skip_0_Read_0                                                                          100496         13555 ns/op            73779 op/s             43849 B/op          9 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_Skip_1000_Read_0                                                                        10000        109424 ns/op             9139 op/s             44177 B/op          9 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_Skip_10000_Read_0                                                                        1396        896962 ns/op             1115 op/s             48114 B/op          9 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_Skip_100000_Read_0                                                                        126       9501140 ns/op              105.3 op/s           88855 B/op         19 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_Skip_1000000_Read_0                                                                        12      91755776 ns/op               10.90 op/s         344361 B/op         79 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_Skip_0_Read_1000                                                                          931       1309299 ns/op              763.8 op/s          231218 B/op       5016 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_Skip_1000_Read_1000                                                                       789       2159174 ns/op              463.1 op/s          228639 B/op       5013 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_Skip_10000_Read_1000                                                                      246      10723794 ns/op               93.25 op/s         228690 B/op       5013 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_Skip_100000_Read_1000                                                                      33      31780862 ns/op               31.47 op/s         229070 B/op       5013 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/Scan_Skip_1000000_Read_1000                                                                      4     261022972 ns/op                3.831 op/s        235722 B/op       5024 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_Skip_0_Read_0                                                                      32185         39069 ns/op            25596 op/s             43903 B/op          6 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_Skip_1000_Read_0                                                                   10000        127520 ns/op             7842 op/s             43954 B/op          3 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_Skip_10000_Read_0                                                                   1000       1128521 ns/op              886.1 op/s           45716 B/op          4 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_Skip_100000_Read_0                                                                   117      10272669 ns/op               97.35 op/s          43682 B/op          3 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_Skip_1000000_Read_0                                                                   10     108041116 ns/op                9.256 op/s         45021 B/op          5 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_Skip_0_Read_1000                                                                     210       5467755 ns/op              182.9 op/s          244368 B/op       6003 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_Skip_1000_Read_1000                                                                  225       5480393 ns/op              182.5 op/s          244697 B/op       6009 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_Skip_10000_Read_1000                                                                 187       6353224 ns/op              157.4 op/s          244741 B/op       6009 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_Skip_100000_Read_1000                                                                 57      18664344 ns/op               53.58 op/s         244983 B/op       6009 allocs/op   
BenchmarkTableScanSuite/MsgpackSerializer/ScanIndex_Skip_1000000_Read_1000                                                                 9     114901596 ns/op                8.703 op/s        247905 B/op       6013 allocs/op   
BenchmarkTableUpdateSuite/MsgpackSerializer/Update_1                                                                                      55      24977718 ns/op               40.04 op/s          89446 B/op         23 allocs/op   
BenchmarkTableUpdateSuite/MsgpackSerializer/Update_10                                                                                     55      20944039 ns/op               47.75 op/s          82883 B/op        139 allocs/op   
BenchmarkTableUpdateSuite/MsgpackSerializer/Update_100                                                                                    49      22186017 ns/op               45.07 op/s         187637 B/op       1317 allocs/op   
BenchmarkTableUpdateSuite/MsgpackSerializer/Update_1000                                                                                   42      32783521 ns/op               30.50 op/s        1446429 B/op      13070 allocs/op   
BenchmarkTableUpdateSuite/MsgpackSerializer/Update_10000                                                                                   8     158237332 ns/op                6.320 op/s      14846201 B/op     139094 allocs/op   
BenchmarkTableUpdateSuite/MsgpackSerializer/Update_100000                                                                                  2     536754324 ns/op                1.863 op/s      115795248 B/op   1320747 allocs/op  
BenchmarkTableUpdateSuite/MsgpackSerializer/Update_1000000                                                                                 1    5807789010 ns/op                0.1722 op/s     1374315704 B/op 13000591 allocs/op 
BenchmarkTableUpsertSuite/MsgpackSerializer/Upsert_1                                                                                      57      21369676 ns/op               46.80 op/s          97086 B/op         26 allocs/op   
BenchmarkTableUpsertSuite/MsgpackSerializer/Upsert_10                                                                                     57      21444930 ns/op               46.63 op/s         303898 B/op        184 allocs/op   
BenchmarkTableUpsertSuite/MsgpackSerializer/Upsert_100                                                                                    54      24032799 ns/op               41.61 op/s        2387705 B/op       1725 allocs/op   
BenchmarkTableUpsertSuite/MsgpackSerializer/Upsert_1000                                                                                   42      34393250 ns/op               29.08 op/s       23415072 B/op      17060 allocs/op   
BenchmarkTableUpsertSuite/MsgpackSerializer/Upsert_10000                                                                                   7     164457707 ns/op                6.081 op/s      235914947 B/op    184559 allocs/op  
BenchmarkTableUpsertSuite/MsgpackSerializer/Upsert_100000                                                                                  1    1043953019 ns/op                0.9579 op/s     2337483320 B/op  1070225 allocs/op 
BenchmarkTableUpsertSuite/MsgpackSerializer/Upsert_1000000                                                                                 1    19010319722 ns/op               0.05260 op/s   23349540512 B/op 10700424 allocs/op
```
