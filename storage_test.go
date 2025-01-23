package ts

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"unsafe"
)

func assertEq(t *testing.T, exp, have byte) bool {
	if exp != have {
		t.Errorf("wrong result: expected %v, have %v", exp, have)
		return false
	}
	return true
}

func uintArrayEq(a, b []uint) bool {
	if len(a) != len(b) {
		return false
	}
	for i, av := range a {
		if av != b[i] {
			return false
		}
	}
	return true
}

func TestSerializePosting(t *testing.T) {
	var (
		p = posting{
			ID:  3,
			Pos: []uint{1, 2, 3},
		}
		p1 posting
	)
	b := serializePosting(&p)
	deserializePosting(b, &p1, 0)
	if p.ID != p1.ID {
		t.Errorf("deserialized ID was not correct: got %d, want %d", p1.ID, p.ID)
	}
	if !uintArrayEq(p.Pos, p1.Pos) {
		t.Errorf("deserialized position array incorrectly: got %v, want %v", p1.Pos, p.Pos)
	}
}

func TestPostingDiskSerialization(t *testing.T) {
	t.Parallel()
	var (
		tmp  = t.TempDir()
		file = filepath.Join(tmp, "posting.test")
		post = posting{ID: 98, Pos: []uint{5, 4, 3, 2, 1, 97}}
	)
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatal(err)
	}
	_, err = writePosting(f, &post)
	if err != nil {
		t.Error(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	f, err = os.OpenFile(file, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var p posting
	err = readPosting(f, &p)
	if err != nil {
		t.Error(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if post.ID != p.ID {
		t.Errorf("expected %d, got %d", post.ID, p.ID)
	}
	if len(post.Pos) != len(p.Pos) {
		t.Fatal("length position arrays don't match")
	}
	for i := 0; i < len(post.Pos); i++ {
		if post.Pos[i] != p.Pos[i] {
			t.Errorf("element %d not equal: %d %d", i, post.Pos[i], p.Pos[i])
		}
	}
}

func TestSerialization(t *testing.T) {
	var dest [8]byte
	pos := serializeUint(0xffffffffffffffff, dest[:], 0)
	assertEq(t, dest[0], 255)
	assertEq(t, dest[1], 255)
	assertEq(t, dest[2], 255)
	assertEq(t, dest[3], 255)
	assertEq(t, dest[4], 255)
	assertEq(t, dest[5], 255)
	assertEq(t, dest[6], 255)
	assertEq(t, dest[7], 255)
	if pos != 8 {
		t.Errorf("expected 8, got %d", pos)
	}
	const n = 512
	var data [n]uint
	for i := uint(0); i < n; i++ {
		data[i] = i
	}
	res := make([]byte, len(data)*int(unsafe.Sizeof(uint(0))))
	serializeUintArray(data[:], res, 0)
	dec := deserializeUintArray(res)
	for i := 0; i < len(data); i++ {
		if data[i] != dec[i] {
			t.Error("wrong decoded value")
		}
	}
	var buf bytes.Buffer
	_, err := writeUintArray(&buf, data[:])
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(res, buf.Bytes()) {
		t.Error("results and buffer are not equal")
	}
	result := make([]uint, len(data))
	err = readUintArray(&buf, result, uint64(len(result)))
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < len(data); i++ {
		if data[i] != result[i] {
			t.Error("wrong read value")
		}
	}
}
