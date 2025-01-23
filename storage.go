package ts

import (
	"errors"
	"io"
	"unsafe"
)

var (
	ErrTermNotFound = errors.New("term not found")
)

type Store interface {
	Get(key string, dest interface{}) error
	Put(key string, value interface{}) error
	Delete(key string) error
}

func NewMemStore() *MemStore {
	return &MemStore{
		terms: make(map[string]*term),
	}
}

type termStore interface {
	get(string) (*term, error)
}

type MemStore struct {
	terms map[string]*term
}

func (ms *MemStore) get(key string) (*term, error) {
	t, ok := ms.terms[key]
	if !ok {
		return nil, ErrTermNotFound
	}
	return t, nil
}

type DiskStore struct{}

func writePosting(w io.Writer, p *posting) (int, error) {
	// Posting Frame:
	// | ID | Positions Array    |
	// |    | Length | Values... |
	// | 8  | 8      | variable  |
	var (
		acc, n int
		err    error
	)
	n, err = writeUint64(w, p.ID)
	if err != nil {
		return n, err
	}
	acc += n
	n, err = writeUint64(w, uint64(len(p.Pos)))
	if err != nil {
		return acc, err
	}
	acc += n
	n, err = writeUintArray(w, p.Pos)
	if err != nil {
		return acc, err
	}
	acc += n
	return acc, nil
}

func readPosting(r io.Reader, p *posting) error {
	// Posting Frame:
	// | ID | Positions Array    |
	// |    | Length | Values... |
	// | 8  | 8      | variable  |
	var err error
	p.ID, err = readUint64(r)
	if err != nil {
		return err
	}
	length, err := readUint64(r)
	if err != nil {
		return err
	}
	p.Pos = make([]uint, length)
	return readUintArray(r, p.Pos, length)
}

func writeUintArray(w io.Writer, src []uint) (int, error) {
	size := uintptr(len(src))
	const l = unsafe.Sizeof(src[0])
	var (
		buf = make([]byte, l*size)
		n   int
	)
	for i := uintptr(0); i < size; i++ {
		n = serializeUint(src[i], buf, n)
	}
	return w.Write(buf[:])
}

func readUintArray(r io.Reader, dest []uint, length uint64) error {
	_ = dest[0]
	var (
		n   = uintptr(length) * unsafe.Sizeof(dest[0])
		buf = make([]byte, n)
	)
	_, err := r.Read(buf)
	if err != nil {
		return err
	}
	deserializeUintArr(dest, 0, buf)
	return nil
}

// TODO(generics) make this work for all numerical types
func writeUint64(w io.Writer, n uint64) (int, error) {
	const l = uint64(unsafe.Sizeof(n))
	var buf [l]byte
	for i := uint64(0); i < l; i++ {
		buf[i] = byte(n >> (i * l))
	}
	return w.Write(buf[:])
}

// TODO(generics) make this work for all numerical types
func readUint64(r io.Reader) (uint64, error) {
	const l = uint64(unsafe.Sizeof(uint(0)))
	var (
		x   uint64
		buf [l]byte
	)
	_, err := r.Read(buf[:])
	if err != nil && err != io.EOF {
		return 0, err
	}
	for i := uint64(0); i < l; i++ {
		x |= uint64(buf[i]) << (i * l)
	}
	return x, err
}

func serializePosting(p *posting) []byte {
	var (
		// Posting Frame:
		// | ID | Positions Array    |
		// |    | Length | Values... |
		// | 8  | 8      | variable  |
		bytes = uint(unsafe.Sizeof(p.ID) + unsafe.Sizeof(uint(0)) + uintArraySize(p.Pos))
		b     = make([]byte, bytes)
	)
	k := serializeUint64(p.ID, b, 0)
	k = serializeUint(uint(len(p.Pos)), b, k)
	serializeUintArray(p.Pos, b, k)
	return b
}

func deserializePosting(b []byte, dest *posting, start int) int {
	var (
		length uint
	)
	dest.ID, start = deserializeUint64(b, start)
	length, start = deserializeUint(b, start)
	if uint(len(dest.Pos)) < length {
		dest.Pos = make([]uint, length)
	}
	deserializeUintArr(dest.Pos, start, b)
	return start
}

func uintArraySize(a []uint) uintptr {
	return uintptr(len(a)) * unsafe.Sizeof(uint(0))
}

// TODO(generics) make this work for all numerical types
func serializeUint64(n uint64, dest []byte, start int) int {
	const l = uint64(unsafe.Sizeof(n))
	for i := uint64(0); i < l; i++ {
		dest[start] = byte(n >> (i * l))
		start++
	}
	return start
}

// TODO(generics) make this work for all numerical types
func serializeUint(n uint, dest []byte, start int) int {
	const l = uint(unsafe.Sizeof(n))
	for i := uint(0); i < l; i++ {
		dest[start] = byte(n >> (i * l))
		start++
	}
	return start
}

// TODO(generics) make this work for all numerical types
func deserializeUint64(src []byte, start int) (uint64, int) {
	var x uint64
	const l = uint64(unsafe.Sizeof(x))
	for j := uint64(0); start < len(src) && j < l; j++ {
		x |= uint64(src[start]) << (j * l)
		start++
	}
	return x, start
}

// TODO(generics) make this work for all numerical types
func deserializeUint(src []byte, start int) (uint, int) {
	var x uint
	const l = uint(unsafe.Sizeof(x))
	for j := uint(0); start < len(src) && j < l; j++ {
		x |= uint(src[start]) << (j * l)
		start++
	}
	return x, start
}

// TODO(generics) make this work for all numerical types
func serializeUintArray(data []uint, dest []byte, start int) int {
	l := len(data)
	if l == 0 {
		return start
	}
	for i := 0; i < l; i++ {
		start = serializeUint(data[i], dest, start)
	}
	return start
}

// TODO(generics) make this work for all numerical types
func deserializeUintArray(b []byte) []uint {
	var (
		i, k int
		dec  = make([]uint, len(b)/int(unsafe.Sizeof(uint(0))))
	)
	if len(b) == 0 {
		return nil
	}
	for i = 0; i < len(dec); i++ {
		dec[i], k = deserializeUint(b, k)
	}
	return dec
}

// De-Serialize an array of bytes into an array of unsigned integers
//
// TODO(generics) make this work for all numerical types
func deserializeUintArr(dest []uint, start int, src []byte) int {
	var i int
	for i = 0; i < len(dest); i++ {
		dest[i], start = deserializeUint(src, start)
	}
	return i
}
