package serializers_test

import (
	"encoding"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"
)

// Hash is a type alias for common.Hash used for data normalization
// with JSON/Database marshalling. Hash is expected to be a hex string.
//
// NOTE: when used with a db like postgres, the column type must be a `bytea`
type Hash string

func HashFromString(s string) Hash {
	return Hash(strings.ToLower(s))
}

func HashFromBytes(src []byte) Hash {
	return Hash("0x" + hex.EncodeToString(src))
}

type Hexer interface {
	Hex() string
}

func ToHash(h Hexer) Hash {
	return HashFromString(h.Hex())
}

func (h Hash) ToShortHash() Hash {
	// in case hash is shorter then we expect, return just its value
	if len(h) < 10 {
		return h
	}
	// return short-hash if already is len(0x12345678) == 10
	if len(h) == 10 {
		return h
	}
	// return short-hash as 0x plus first 8 bytes of the blockHash,
	// which assumes the blockHash will always have 0x prefix.
	return h[0:10]
}

var (
	_h                            = Hash("")
	_  encoding.BinaryMarshaler   = _h
	_  encoding.BinaryUnmarshaler = &_h
	_  encoding.TextMarshaler     = _h
	_  encoding.TextUnmarshaler   = &_h
)

// UnmarshalText implements encoding.TextMarshaler.
func (h Hash) MarshalText() ([]byte, error) {
	return []byte(h.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (h *Hash) UnmarshalText(src []byte) error {
	*h = HashFromString(string(src))
	return nil
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (h Hash) MarshalBinary() ([]byte, error) {
	return HexStringToBytes(string(h)), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (h *Hash) UnmarshalBinary(b []byte) error {
	*h = Hash(HexBytesToString(b))
	return nil
}

func (h Hash) String() string {
	return string(h)
}

func (h Hash) Hex() string {
	return string(h)
}

func (h Hash) Len() int {
	return len(h.String())
}

func (h Hash) ByteSize() int {
	if len(h) == 0 {
		return 0
	}
	if has0xPrefix(string(h)) {
		return (len(h) - 2) / 2
	} else {
		return len(h) / 2
	}
}

func (h Hash) IsZeroValue() bool {
	if h.String() == "" {
		return true
	}
	if h.String() == "0x" {
		return true
	}
	if h.String() == "0x00000000" {
		return true
	}
	if h.String() == "0x0000000000000000000000000000000000000000" {
		return true
	}
	if h.String() == "0x0000000000000000000000000000000000000000000000000000000000000000" {
		return true
	}
	return false
}

func (h Hash) IsValidHex() bool {
	if len(h) == 0 {
		return false
	}

	s := string(h)
	if has0xPrefix(s) {
		s = s[2:]
	}
	if len(s) == 0 {
		return true // its valid, but its empty
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}

	return true
}

func (h Hash) IsValidAddress() bool {
	if len(h) <= 2 || h[0:2] != "0x" {
		return false
	}
	if len(h) != 42 {
		return false
	}
	return true
}

func (h Hash) IsValidTxnHash() bool {
	if h[0:2] != "0x" {
		return false
	}
	if len(h) != 66 {
		return false
	}
	return true
}

func (h Hash) Bytes() []byte {
	return HexStringToBytes(string(h))
}

func ToHashList[T Hexer](list []T) []Hash {
	result := make([]Hash, 0, len(list))
	for _, a := range list {
		result = append(result, ToHash(a))
	}
	return result
}

func HexBytesToString(b []byte) string {
	return "0x" + hex.EncodeToString(b)
}

func HexStringToBytes(s string) []byte {
	if has0xPrefix(s) {
		s = s[2:]
	}
	if len(s)%2 == 1 {
		s = "0" + s
	}
	h, _ := hex.DecodeString(s)
	return h
}

func has0xPrefix(str string) bool {
	return len(str) >= 2 && str[0] == '0' && (str[1] == 'x' || str[1] == 'X')
}

// BigInt is a type alias for big.Int used for JSON/Database marshalling.
//
// For JSON values we encode BigInt's as strings.
//
// For Database values we encode BigInt's as NUMERIC(78).
type BigInt big.Int

func NewBigInt(n int64) BigInt {
	b := big.NewInt(n)
	return BigInt(*b)
}

// Decimal number string
func NewBigIntFromNumberString(hs string) BigInt {
	return NewBigIntFromDecimalString(hs)
}

func NewBigIntFromBinaryString(hs string) BigInt {
	return NewBigIntFromString(hs, 2)
}

func NewBigIntFromOctalString(hs string) BigInt {
	return NewBigIntFromString(hs, 8)
}

func NewBigIntFromDecimalString(hs string) BigInt {
	return NewBigIntFromString(hs, 10)
}

func NewBigIntFromHexString(hs string) BigInt {
	return NewBigIntFromString(hs, 16)
}

func NewBigIntFromString(s string, base int) BigInt {
	b := big.NewInt(0)
	if base != 0 && (2 > base || base > big.MaxBase) {
		return BigInt{}
	}

	var bi BigInt
	b, ok := b.SetString(s, base)
	if ok {
		bi = BigInt(*b)
	} else {
		bi, _ = ParseBigIntString(s, base)
	}
	return bi
}

func ToBigInt(b *big.Int) BigInt {
	if b == nil {
		return BigInt{}
	}
	c := big.NewInt(0).Set(b)
	return BigInt(*c)
}

func ToBigIntArray(bs []*big.Int) []BigInt {
	var pbs []BigInt
	for _, b := range bs {
		pbs = append(pbs, ToBigInt(b))
	}
	return pbs
}

func ToBigIntArrayFromStringArray(s []string, base int) ([]BigInt, error) {
	var pbs []BigInt
	for _, v := range s {
		b, ok := (&big.Int{}).SetString(v, base)
		if !ok {
			return nil, fmt.Errorf("invalid number %s", s)
		}
		pbs = append(pbs, ToBigInt(b))
	}
	return pbs, nil
}

func ToBigIntFromInt64(n int64) BigInt {
	return ToBigInt(big.NewInt(n))
}

func (b *BigInt) SetString(s string, base int) bool {
	v := big.Int(*b)
	n, ok := v.SetString(s, base)
	if !ok {
		return false
	}
	*b = BigInt(*n)
	return true
}

func (b BigInt) String() string {
	return b.Int().String()
}

func (b BigInt) Bytes() []byte {
	return b.Int().Bytes()
}

func (b BigInt) Int() *big.Int {
	v := big.Int(b)
	return &v
}

func (b BigInt) Uint64() uint64 {
	return b.Int().Uint64()
}

func (b BigInt) Int64() int64 {
	return b.Int().Int64()
}

func (b *BigInt) Add(n *big.Int) {
	z := b.Int().Add(b.Int(), n)
	*b = BigInt(*z)
}

func (b *BigInt) Sub(n *big.Int) {
	z := b.Int().Sub(b.Int(), n)
	*b = BigInt(*z)
}

func (b BigInt) Equals(n *big.Int) bool {
	return b.Int().Cmp(n) == 0
}

func (b BigInt) Gt(n *big.Int) bool {
	return b.Int().Cmp(n) == 1
}

func (b BigInt) Gte(n *big.Int) bool {
	return b.Int().Cmp(n) == 0 || b.Int().Cmp(n) == 1
}

func (b BigInt) Lt(n *big.Int) bool {
	return b.Int().Cmp(n) == -1
}

func (b BigInt) Lte(n *big.Int) bool {
	return b.Int().Cmp(n) == 0 || b.Int().Cmp(n) == -1
}

var (
	_bi                            = BigInt{}
	_   encoding.BinaryMarshaler   = _bi
	_   encoding.BinaryUnmarshaler = &_bi
	_   encoding.TextMarshaler     = _bi
	_   encoding.TextUnmarshaler   = &_bi
	_   json.Marshaler             = _bi
	_   json.Unmarshaler           = &_bi
)

// MarshalText implements encoding.TextMarshaler.
func (b BigInt) MarshalText() ([]byte, error) {
	v := fmt.Sprintf("\"%s\"", b.String())
	return []byte(v), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (b *BigInt) UnmarshalText(text []byte) error {
	t := string(text)
	if len(text) <= 2 || t == "null" || t == "" {
		return nil
	}
	i, ok := big.NewInt(0).SetString(string(text[1:len(text)-1]), 10)
	if !ok {
		return fmt.Errorf("BigInt.UnmarshalText: failed to unmarshal %q", text)
	}
	*b = BigInt(*i)
	return nil
}

// MarshalJSON implements json.Marshaler
func (b BigInt) MarshalJSON() ([]byte, error) {
	return b.MarshalText()
}

// UnmarshalJSON implements json.Unmarshaler
func (b *BigInt) UnmarshalJSON(text []byte) error {
	if string(text) == "null" {
		return nil
	}
	return b.UnmarshalText(text)
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (b BigInt) MarshalBinary() (data []byte, err error) {
	return b.Int().Bytes(), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (b *BigInt) UnmarshalBinary(buff []byte) error {
	i := big.NewInt(0).SetBytes(buff)
	*b = BigInt(*i)
	return nil
}

func ParseBigIntString(s string, base int) (BigInt, bool) {
	neg := strings.HasPrefix(s, "-")
	var ns strings.Builder

	switch base {
	case 2:
		for _, char := range s {
			// 0-9 || B || b
			if (char >= 48 && char <= 57) || char == 66 || char == 98 {
				ns.WriteRune(char)
			}
		}
		s = ns.String()
		s = strings.TrimPrefix(s, "0B")
		s = strings.TrimPrefix(s, "0b")

	case 8:
		for _, char := range s {
			// 0-9 || O || o
			if (char >= 48 && char <= 57) || char == 79 || char == 111 {
				ns.WriteRune(char)
			}
		}
		s = ns.String()
		s = strings.TrimPrefix(s, "0O")
		s = strings.TrimPrefix(s, "0o")

	case 10:
		for _, char := range s {
			// 0-9
			if char >= 48 && char <= 57 {
				ns.WriteRune(char)
			}
		}
		s = ns.String()

	case 16:
		for _, char := range s {
			// 0-9 || A-Z || a-z || X || x
			if (char >= 48 && char <= 57) || (char >= 65 && char <= 70) || (char >= 97 && char <= 102) || char == 88 || char == 120 {
				ns.WriteRune(char)
			}
		}

		s = ns.String()
		s = strings.TrimPrefix(s, "0X")
		s = strings.TrimPrefix(s, "0x")

	default:
		return BigInt{}, false
	}

	if neg {
		s = "-" + s
	}

	b := big.NewInt(0)
	b, ok := b.SetString(s, base)
	if !ok {
		return BigInt{}, false
	}

	return BigInt(*b), true
}

func ParseNumberString(s string, base int) (int64, bool) {
	bi, ok := ParseBigIntString(s, base)
	if !ok {
		return 0, false
	}

	// Check if it fits within int64 range, if it doesn't fit in int64, return false
	if bi.Lt(big.NewInt(math.MinInt64)) || bi.Gt(big.NewInt(math.MaxInt64)) {
		return 0, false
	}

	return bi.Int64(), true
}

func IsValidNumberString(s string) bool {
	for _, char := range s {
		if char < 48 || char > 57 {
			return false
		}
	}
	return true
}
