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
goos: darwin
goarch: amd64
pkg: github.com/go-bond/bond/_benchmarks
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
BenchmarkTableDeleteSuite/MsgPackSerializer/Delete_1                                                                   55         22268008 ns/op                44.91 op/s          46745 B/op         8 allocs/op   
BenchmarkTableDeleteSuite/MsgPackSerializer/Delete_10                                                                  56         21778041 ns/op                45.92 op/s          48795 B/op         9 allocs/op   
BenchmarkTableDeleteSuite/MsgPackSerializer/Delete_100                                                                 54         24056791 ns/op                41.57 op/s          76860 B/op        15 allocs/op   
BenchmarkTableDeleteSuite/MsgPackSerializer/Delete_1000                                                                50         26799090 ns/op                37.31 op/s         437441 B/op        34 allocs/op   
BenchmarkTableDeleteSuite/MsgPackSerializer/Delete_10000                                                                8        140110681 ns/op                 7.137 op/s       5562649 B/op       541 allocs/op   
BenchmarkTableDeleteSuite/MsgPackSerializer/Delete_100000                                                               4        268512400 ns/op                 3.724 op/s      38359066 B/op       726 allocs/op   
BenchmarkTableDeleteSuite/MsgPackSerializer/Delete_1000000                                                              1       1694246965 ns/op                 0.5902 op/s     443697976 B/op     3321 allocs/op  
BenchmarkTableExistSuite/MsgPackSerializer/Exist_True                                                              340587             2974 ns/op            336361 op/s             21776 B/op         2 allocs/op   
BenchmarkTableExistSuite/MsgPackSerializer/Exist_False                                                             362940             2957 ns/op            338295 op/s             21776 B/op         2 allocs/op   
BenchmarkTableGetSuite/MsgPackSerializer/Get_Row_Exist                                                             284804             4022 ns/op            248694 op/s             21929 B/op         7 allocs/op   
BenchmarkTableGetSuite/MsgPackSerializer/Get_Row_Does_Not_Exist                                                    228415             5002 ns/op            199920 op/s             22792 B/op        19 allocs/op   
BenchmarkTableInsertSuite/MsgPackSerializer/Insert_1                                                                   56         23892712 ns/op                41.85 op/s          46125 B/op        14 allocs/op   
BenchmarkTableInsertSuite/MsgPackSerializer/Insert_10                                                                  54         21772279 ns/op                45.93 op/s          54446 B/op        70 allocs/op   
BenchmarkTableInsertSuite/MsgPackSerializer/Insert_100                                                                 50         26012666 ns/op                38.44 op/s         172117 B/op       617 allocs/op   
BenchmarkTableInsertSuite/MsgPackSerializer/Insert_1000                                                                49         36475071 ns/op                27.42 op/s        1315875 B/op      6045 allocs/op   
BenchmarkTableInsertSuite/MsgPackSerializer/Insert_10000                                                               12        186288974 ns/op                 5.368 op/s      12677258 B/op     62298 allocs/op   
BenchmarkTableInsertSuite/MsgPackSerializer/Insert_100000                                                               2        750068838 ns/op                 1.333 op/s      134961612 B/op   602010 allocs/op  
BenchmarkTableInsertSuite/MsgPackSerializer/Insert_1000000                                                              1       6308249228 ns/op                 0.1585 op/s     1316571680 B/op         6000418 allocs/op 
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_0_Limit_0                                         1       25846066965 ns/op                0.03869 op/s   4248255216 B/op 100025596 allocs/op
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_0_Limit_500                                    1764           604308 ns/op              1655 op/s            132880 B/op      2520 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_0_Limit_1000                                   1058          1616130 ns/op               618.8 op/s          231594 B/op      5022 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_0_Limit_5000                                    183          5995510 ns/op               166.8 op/s          989915 B/op     25030 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_0_Limit_10000                                   100         10983469 ns/op                91.05 op/s        2031947 B/op     50057 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_0_Limit_100000                                    9        118656305 ns/op                 8.428 op/s      20332925 B/op    500326 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_0_Limit_0                                         1       22921444643 ns/op                0.04363 op/s   4174467016 B/op 100019730 allocs/op
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_500_Limit_1000                                 1012          1040839 ns/op               960.8 op/s          228819 B/op      5019 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_1000_Limit_1000                                1090          1079958 ns/op               926.0 op/s          228817 B/op      5019 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_5000_Limit_1000                                 847          1435626 ns/op               696.6 op/s          228822 B/op      5019 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_10000_Limit_1000                                628          1870270 ns/op               534.7 op/s          228830 B/op      5019 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_100000_Limit_1000                                67         18271230 ns/op                54.73 op/s         229070 B/op      5019 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_1000000_Limit_1000                                6        200232173 ns/op                 4.994 op/s        232148 B/op      5024 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_Default_Offset_10000000_Limit_1000                               1       2043400152 ns/op                 0.4894 op/s       249216 B/op      5053 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_0_Limit_0                                  1       59258333670 ns/op                0.01688 op/s   4482110928 B/op 120003262 allocs/op
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_0_Limit_500                              866          1347491 ns/op               742.1 op/s          139006 B/op      3014 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_0_Limit_1000                             456          2642338 ns/op               378.5 op/s          244009 B/op      6016 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_0_Limit_5000                              85         13923133 ns/op                71.82 op/s        1051355 B/op     30020 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_0_Limit_10000                             42         26729292 ns/op                37.41 op/s        2160968 B/op     60023 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_0_Limit_100000                             4        287015350 ns/op                 3.484 op/s      21750608 B/op    600045 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_0_Limit_0                                  1       58516616843 ns/op                0.01709 op/s   4481813792 B/op 120000185 allocs/op
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_500_Limit_1000                           435          2757363 ns/op               362.7 op/s          244851 B/op      6016 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_1000_Limit_1000                          424          2736964 ns/op               365.4 op/s          244852 B/op      6016 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_5000_Limit_1000                          366          3202628 ns/op               312.2 op/s          244860 B/op      6016 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_10000_Limit_1000                         346          3466516 ns/op               288.5 op/s          244863 B/op      6016 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_100000_Limit_1000                         92         10936493 ns/op                91.44 op/s         245075 B/op      6016 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_1000000_Limit_1000                        10        105035816 ns/op                 9.521 op/s        247933 B/op      6024 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddress_Offset_10000000_Limit_1000                        1       1115182077 ns/op                 0.8967 op/s       257120 B/op      6044 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_0                  1       137042692203 ns/op               0.007297 op/s 4482060152 B/op  120002615 allocs/op
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_500              828          1533874 ns/op               651.9 op/s          139794 B/op      3014 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_1000             378          2963738 ns/op               337.4 op/s          244871 B/op      6016 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_5000              39         30610137 ns/op                32.67 op/s        1052094 B/op     30020 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_10000             19         61423647 ns/op                16.28 op/s        2162681 B/op     60025 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_100000             2        664975470 ns/op                 1.504 op/s      21756792 B/op    600050 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_0_Limit_0                  1       130475425865 ns/op               0.007664 op/s 4481808752 B/op  120000189 allocs/op
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_500_Limit_1000           404          2901840 ns/op               344.6 op/s          244854 B/op      6016 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_1000_Limit_1000          424          2943384 ns/op               339.7 op/s          244873 B/op      6016 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_5000_Limit_1000          356          3255524 ns/op               307.2 op/s          244862 B/op      6016 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_10000_Limit_1000         316          3494921 ns/op               286.1 op/s          244870 B/op      6016 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_100000_Limit_1000        106         11096099 ns/op                90.12 op/s         245009 B/op      6016 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_1000000_Limit_1000        12         97559666 ns/op                10.25 op/s         247524 B/op      6026 allocs/op   
BenchmarkTableQuerySuite/MsgPackSerializer/Query_Index_AccountAddressOrderBalanceDESC_Offset_10000000_Limit_1000        2        918366726 ns/op                 1.089 op/s        256704 B/op      6034 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_1                                                                   146841            13508 ns/op             74036 op/s             43744 B/op        13 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_10                                                                   68640            17076 ns/op             58562 op/s             45352 B/op        62 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_1000                                                                   974          1247820 ns/op               801.4 op/s          228057 B/op      5022 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_10000                                                                  121          9340905 ns/op               107.1 op/s         2004095 B/op     50027 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_100000                                                                  12        107459304 ns/op                 9.306 op/s      20218984 B/op    500074 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_1000000                                                                  1       1091707810 ns/op                 0.9160 op/s     207422168 B/op  5001347 allocs/op  
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_1                                                              182397            10822 ns/op             92413 op/s             43760 B/op        11 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_10                                                              48643            23619 ns/op             42341 op/s             45512 B/op        69 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_1000                                                              668          1813232 ns/op               551.5 op/s          244659 B/op      6016 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_10000                                                              37         27446435 ns/op                36.43 op/s        2285178 B/op     60670 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_100000                                                              4        287125924 ns/op                 3.483 op/s      25852136 B/op    622263 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_1000000                                                             1       3940065221 ns/op                 0.2538 op/s     361375856 B/op  6777446 allocs/op  
BenchmarkTableScanSuite/MsgPackSerializer/Scan_Skip_0_Read_0                                                       202444             9958 ns/op            100422 op/s             43829 B/op         9 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_Skip_1000_Read_0                                                     19042            63807 ns/op             15672 op/s             44163 B/op         9 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_Skip_10000_Read_0                                                     1996           566898 ns/op              1764 op/s             47261 B/op         9 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_Skip_100000_Read_0                                                     196          6034144 ns/op               165.7 op/s           70385 B/op        15 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_Skip_1000000_Read_0                                                     19         59260682 ns/op                16.87 op/s         222285 B/op        60 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_Skip_0_Read_1000                                                      1273           934584 ns/op              1070 op/s            229870 B/op      5015 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_Skip_1000_Read_1000                                                   1191           996086 ns/op              1004 op/s            228635 B/op      5013 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_Skip_10000_Read_1000                                                   742          1597727 ns/op               625.9 op/s          228646 B/op      5013 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_Skip_100000_Read_1000                                                   61         19141671 ns/op                52.24 op/s         228934 B/op      5013 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/Scan_Skip_1000000_Read_1000                                                   6        204092684 ns/op                 4.900 op/s        233341 B/op      5019 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_Skip_0_Read_0                                                   44494            30300 ns/op             33003 op/s             43891 B/op         5 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_Skip_1000_Read_0                                                12710            90726 ns/op             11022 op/s             43781 B/op         3 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_Skip_10000_Read_0                                                1664           716593 ns/op              1395 op/s             43569 B/op         3 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_Skip_100000_Read_0                                                165          7070912 ns/op               141.4 op/s           43651 B/op         3 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_Skip_1000000_Read_0                                                13         91009277 ns/op                10.99 op/s          44691 B/op         4 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_Skip_0_Read_1000                                                  266          4499030 ns/op               222.3 op/s          244346 B/op      6003 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_Skip_1000_Read_1000                                               242          4580708 ns/op               218.3 op/s          244714 B/op      6009 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_Skip_10000_Read_1000                                              228          5236058 ns/op               191.0 op/s          244644 B/op      6009 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_Skip_100000_Read_1000                                              93         11725095 ns/op                85.29 op/s         244835 B/op      6009 allocs/op   
BenchmarkTableScanSuite/MsgPackSerializer/ScanIndex_Skip_1000000_Read_1000                                             12         92575598 ns/op                10.80 op/s         247084 B/op      6012 allocs/op   
BenchmarkTableUpdateSuite/MsgPackSerializer/Update_1                                                                   45         24301902 ns/op                41.15 op/s          83706 B/op        23 allocs/op   
BenchmarkTableUpdateSuite/MsgPackSerializer/Update_10                                                                  54         22410978 ns/op                44.62 op/s          82878 B/op       138 allocs/op   
BenchmarkTableUpdateSuite/MsgPackSerializer/Update_100                                                                 52         24967832 ns/op                40.05 op/s         187348 B/op      1316 allocs/op   
BenchmarkTableUpdateSuite/MsgPackSerializer/Update_1000                                                                40         38873710 ns/op                25.72 op/s        1448984 B/op     13071 allocs/op   
BenchmarkTableUpdateSuite/MsgPackSerializer/Update_10000                                                               10        231111696 ns/op                 4.327 op/s      15041491 B/op    139462 allocs/op   
BenchmarkTableUpdateSuite/MsgPackSerializer/Update_100000                                                               3        482247276 ns/op                 2.074 op/s      117091890 B/op  1327911 allocs/op  
BenchmarkTableUpdateSuite/MsgPackSerializer/Update_1000000                                                              1       4757047629 ns/op                 0.2102 op/s     1374405976 B/op        13000758 allocs/op 
BenchmarkTableUpsertSuite/MsgPackSerializer/Upsert_1                                                                   51         22789424 ns/op                43.88 op/s          97379 B/op        25 allocs/op   
BenchmarkTableUpsertSuite/MsgPackSerializer/Upsert_10                                                                  56         22518178 ns/op                44.41 op/s         304031 B/op       183 allocs/op   
BenchmarkTableUpsertSuite/MsgPackSerializer/Upsert_100                                                                 51         27283493 ns/op                36.65 op/s        2387559 B/op      1724 allocs/op   
BenchmarkTableUpsertSuite/MsgPackSerializer/Upsert_1000                                                                42         42998110 ns/op                23.26 op/s       23415823 B/op     17059 allocs/op   
BenchmarkTableUpsertSuite/MsgPackSerializer/Upsert_10000                                                                8        177840921 ns/op                 5.623 op/s      235430847 B/op   181650 allocs/op  
BenchmarkTableUpsertSuite/MsgPackSerializer/Upsert_100000                                                               2       1012133648 ns/op                 0.9880 op/s     2314949224 B/op         1712353 allocs/op 
BenchmarkTableUpsertSuite/MsgPackSerializer/Upsert_1000000                                                              1       9262514170 ns/op                 0.1080 op/s     23349524712 B/op       10700318 allocs/op
```
