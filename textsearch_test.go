package ts

import (
	"bufio"
	"bytes"
	"embed"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bleve "github.com/blevesearch/bleve/v2"
	"github.com/dgraph-io/badger/v3"
	"github.com/jdkato/prose/v2"
	"github.com/matryer/is"
)

//go:embed testdata
var testdata embed.FS

func Test(t *testing.T) {
	t.Skip()
	index := NewIndex()
	files, names := getTestData(t)
	tm := time.Now()
	defer func() { fmt.Println(time.Since(tm)) }()
	for _, name := range names {
		f, err := files.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(f)
		if err != nil {
			f.Close()
			t.Fatal(err)
		}
		if err = f.Close(); err != nil {
			t.Fatal(err)
		}
		toks, err := newTokenizer(string(body))
		if err != nil {
			t.Fatal(err)
		}
		if err = index.add(name, toks); err != nil {
			t.Fatal(err)
		}
	}
	for i, name := range index.docNames {
		fmt.Printf("%d %s\n", i, name)
	}
	println()

	// term0 := index.terms["index"]
	// term1 := index.terms["web"]
	// fmt.Printf("%+v\n", term0)
	// fmt.Printf("%+v\n", term1)
	// for _, p := range term0.postings {
	// 	fmt.Printf("%+v, ", p.ID)
	// }
	// println()
	// for _, p := range term1.postings {
	// 	fmt.Printf("%+v, ", p.ID)
	// }
	// println()

	// res := index.Search(And(
	// 	StringQuery("index"),
	// 	StringQuery("web"),
	// ))

	res := index.Search(StringQuery("index"))
	for _, r := range res {
		fmt.Printf("%+v\n", r)
	}

	// println()
	// res = index.Search(StringQuery("index"))
	// for _, r := range res {
	// 	fmt.Printf("%+v\n", r)
	// }
}

func TestKIntersect(t *testing.T) {
	t.Parallel()
	is := is.New(t)

	docs := [...]string{
		0: "hello this is a test",
		1: "this is a test for this token hello",
	}
	var (
		r  []*QueryResult
		ix = NewIndex()
	)
	t1 := mustNewTokenizer(docs[0])
	t2 := mustNewTokenizer(docs[1])
	ix.add("doc1", t1)
	ix.add("doc2", t2)
	r = ix.Search(StringQuery("token"))
	if len(r) < 1 {
		t.Fatal("expected at least one result")
	}

	postings := [][]*posting{
		{
			{ID: 0, Pos: []uint{1}},
		},
		{
			{ID: 0, Pos: []uint{3}},
		},
	}
	res := kIntersect(postings)
	is.Equal(len((res)), 1) // only has one document
	is.Equal(res[0].ID, uint64(0))
	is.Equal(len(res[0].Pos), 2)
	is.Equal(res[0].Pos[0], uint(1))
	is.Equal(res[0].Pos[1], uint(3))

	postings = [][]*posting{
		{
			{ID: 0, Pos: []uint{0, 8}},
			{ID: 1, Pos: []uint{1, 2}},
		},
		{
			{ID: 0, Pos: []uint{1, 7}},
		},
	}
	res = kIntersect(postings)
	// fmt.Printf("%+v\n", res)
	// fmt.Printf("%+v\n", intersect(postings[0], postings[1]))
	is.Equal(len(res), 2)
	is.Equal(res[0].ID, uint64(1))
	is.Equal(res[0].Pos, []uint{1, 2})
	is.Equal(res[1].ID, uint64(0))
	is.Equal(res[1].Pos, []uint{0, 8, 1, 7})

	// res = intersect(
	// 	[]*posting{
	// 		{ID: 0, Pos: []uint{0, 8}},
	// 		{ID: 1, Pos: []uint{1, 2}},
	// 	},
	// 	[]*posting{
	// 		{ID: 0, Pos: []uint{1, 7}},
	// 	},
	// )
	// is.Equal(len(res), 2)
	// is.Equal(res[0].ID, uint64(1))
	// is.Equal(res[0].Pos, []uint{1, 2})
	// is.Equal(res[1].ID, uint64(0))
	// is.Equal(res[1].Pos, []uint{0, 8, 1, 7})
}

func TestIntersect(t *testing.T) {}

func TestLevenshtein(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		A, B string
		Exp  int
	}{
		{"kitten", "sitten", 1},
		{"one", "ane", 1},
		{"", "hello", 5},
		{"hello", "", 5},
		{"hello", "hello", 0},
		{"ab", "aa", 1},
		{"ab", "ba", 2},
		{"ab", "aaa", 2},
		{"bbb", "a", 3},
		{"kitten", "sitting", 3},
		{"distance", "difference", 5},
		{"levenshtein", "frankenstein", 6},
		{"resume and cafe", "resumes and cafes", 2},
		{"a very long string that is meant to exceed", "another very long string that is meant to exceed", 6},
	} {
		l := levenshtein(tc.A, tc.B)
		if l != tc.Exp {
			t.Errorf("%q, %q: got %d, want %d", tc.A, tc.B, l, tc.Exp)
		}
	}
}

func printTerms(terms map[string]*term) {
	for k, term := range terms {
		fmt.Printf("%q: ", k)
		printTerm(term)
	}
}

func printTerm(term *term) {
	fmt.Printf("{\n\ttoken: %q\n\tfreq: %d\n\tpostings: ", term.token, term.freq)
	i := 0
	fmt.Printf("[")
	for i = 0; i < len(term.postings)-1; i++ {
		fmt.Printf("%+v, ", *term.postings[i])
	}
	fmt.Printf("%+v]\n", *term.postings[i])
	fmt.Printf("},\n")
}

func BenchmarkSearch(b *testing.B) {
	b.StartTimer()
	ix := getTestIndex(b)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		ix.Search(StringQuery("file"))
	}
}

func BenchmarkCustomTokenizer(b *testing.B) {}

func BenchmarkDefaultTokenizer(b *testing.B) {}

type testobj interface {
	Helper()
	Fatal(...interface{})
}

func getTestData(t testobj) (fs.FS, []string) {
	t.Helper()
	files, err := testdata.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	filenames := make([]string, len(files))
	for i, f := range files {
		filenames[i] = f.Name()
	}
	data, err := fs.Sub(testdata, "testdata")
	if err != nil {
		t.Fatal(err)
	}
	return data, filenames
}

func getTestFiles(t testobj) []string {
	files := make([]string, 0)
	dir, err := testdata.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range dir {
		files = append(files, filepath.Join("testdata", d.Name()))
	}
	return files
}

func getTestIndex(t testobj) *index {
	data, filenames := getTestData(t)
	ix := NewIndex()
	for _, filename := range filenames {
		f, err := data.Open(filename)
		if err != nil {
			t.Fatal(err)
		}
		err = ix.addDoc(filename, f)
		if err != nil {
			t.Fatal(err)
		}
	}
	return ix
}

func getTerm(name string, index int) string {
	f, err := testdata.Open("testdata/" + name)
	if err != nil {
		return ""
	}
	defer f.Close()
	buf := bufio.NewReader(f)
	tokens := make([]string, 0)
	for err != io.EOF {
		segment, err := buf.ReadString(' ')
		if err != nil && err != io.EOF {
			break
		}
		if len(segment) == 0 {
			buf.Discard(1)
			continue
		}
		fmt.Println(len(tokens), len(segment), buf.Size(), buf.Buffered())
		parts := strings.Split(segment, "\n")
		for _, b := range parts {
			b = strings.Trim(b, " \n\t\r")
			raw := cleanWord(b)
			if len(raw) == 0 {
				continue
			}
			tokens = append(tokens, string(raw))
		}
	}
	return tokens[index]
}

func TestDeps(t *testing.T) {
	t.Skip()
	t.Parallel()
	var (
		_    = bleve.NewIndexMapping()   // full text search library
		_    = badger.DefaultOptions("") // storage
		_, _ = prose.NewDocument("")     // nlp and tokenization
	)
	mapping := bleve.NewIndexMapping()
	ix, err := bleve.NewMemOnly(mapping)
	if err != nil {
		t.Fatal(err)
	}
	err = ix.Index("hello", struct {
		ID    string
		Index int
	}{ID: "1", Index: 1})
	if err != nil {
		t.Fatal(err)
	}
	opts := badger.DefaultOptions("")
	opts.InMemory = true
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := testdata.Open("testdata/google.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var buf bytes.Buffer
	buf.ReadFrom(f)

	doc, err := prose.NewDocument(
		buf.String(),
		prose.WithTokenization(true),
		prose.WithSegmentation(true),
		prose.WithTagging(true),
		prose.WithExtraction(false), // don't need named entities
	)
	if err != nil {
		t.Fatal(err)
	}
	// for _, ent := range doc.Entities() {
	// 	fmt.Printf("%s %q\n", ent.Label, ent.Text)
	// }
	for _, tok := range doc.Tokens() {
		switch tok.Tag {
		case "SYM", ".", ",", "(", ")", ":":
			continue
		}
		fmt.Printf("%s %s %q\n", tok.Label, tok.Tag, tok.Text)
	}
}
