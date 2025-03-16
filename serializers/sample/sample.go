//go:generate protoc -I=./ --go_out=./ --go-vtproto_out=./ --go-vtproto_opt=features=marshal+unmarshal+size ./sample.proto
package sample
