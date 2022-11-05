package bond

type Filter interface {
	Add(key []byte)
	MayContain(key []byte) bool
}
