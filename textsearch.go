package ts

import (
	"io"
	"math"
	"sort"
)

type DocID uint64

func NewIndex() *index {
	return &index{
		terms:           make(map[string]*term),
		documents:       0,
		documentMaxFreq: make([]float64, 0),
	}
}

type index struct {
	documents       uint64
	docNames        []string
	documentMaxFreq []float64
	// Set of terms.
	terms map[string]*term
}

type term struct {
	// Freq is the frequency of the term across all of the documents
	freq int
	// Token is the actual token for which this object indexes
	token string
	// Postings list
	postings []*posting
}

type posting struct {
	ID  uint64 // document ID
	Pos []uint // term positions in document
}

type postingsList []*posting

func (pl postingsList) Len() int              { return len(pl) }
func (pl postingsList) Less(i, j int) bool    { return pl[i].ID < pl[j].ID }
func (pl postingsList) Swap(i, j int)         { pl[i], pl[j] = pl[j], pl[i] }
func (pl postingsList) Eq(i, j int) bool      { return pl[i].ID == pl[j].ID }
func (pl postingsList) Greater(i, j int) bool { return pl[i].ID > pl[j].ID }

const (
	maxUint = ^uint64(0)
	minUint = 0
	maxInt  = int(maxUint >> 1)
	minInt  = -maxInt - 1
)

func (ix *index) addDoc(docname string, r io.Reader) error {
	return ix.add(docname, newCustomTokenizer(r))
}

func (ix *index) add(name string, tokens tokenizer) error {
	var (
		max   = minInt
		docID = ix.documents
	)
	for {
		tok, err := tokens.Next()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
		freq := ix.addToken(tok.text, tok.pos, docID, name)
		if freq > max {
			max = freq
		}
	}
	ix.docNames = append(ix.docNames, name)
	ix.documents++
	if max == minInt {
		max = 1
	}
	ix.documentMaxFreq = append(ix.documentMaxFreq, float64(max))
	for _, term := range ix.terms {
		sort.Sort(postingsList(term.postings))
	}
	return nil
}

// addToken will take a token from a document at some position in the document
// and add it to the index while collecting all relevant information. Returns
// the new frequency of that token in the index.
func (ix *index) addToken(
	token string,
	position uint,
	docID uint64,
	docname string,
) int {
	t, ok := ix.terms[token]
	if !ok {
		t = &term{
			freq:  1,
			token: token,
			// Postings list should have no more elements
			// than the number of documents indexed.
			postings: make([]*posting, 0, ix.documents),
		}
		ix.terms[token] = t
	} else {
		t.freq += 1
	}
	// Add or append to postings list
	i, ok := t.findPostingByDocID(docID)
	if !ok {
		t.postings = append(t.postings, &posting{ID: docID, Pos: []uint{position}})
	} else {
		t.postings[i].Pos = append(t.postings[i].Pos, position)
	}
	return t.freq
}

// Find the index of a posting given the document ID.
// If a result is not found, it will return `false` in the second return value.
func (t *term) findPostingByDocID(docID uint64) (int, bool) {
	n := len(t.postings)
	i := sort.Search(n, func(i int) bool {
		return t.postings[i].ID == docID
	})
	if i == n {
		return -1, false
	}
	return i, true
}

func (ix *index) Search(query Query) []*QueryResult {
	var (
		tokens   = query.Keys()
		postings = make([][]*posting, 0, len(tokens))
	)
	for _, key := range tokens {
		term, ok := ix.terms[key]
		if !ok {
			continue
		}
		postings = append(postings, term.postings)
	}
	if len(postings) == 0 {
		return nil
	}
	result := ix.tfIdf(query.Join(postings))
	sort.Sort(QueryResults(result))
	return result
}

// Term frequency - inverse document frequency
func (ix *index) tfIdf(postings []*posting) []*QueryResult {
	var (
		result = make([]*QueryResult, 0)
		idf    = math.Log2(float64(ix.documents) / float64(len(postings)))
	)
	for _, p := range postings {
		l := len(p.Pos)
		tf := float64(l) / ix.documentMaxFreq[p.ID]
		rank := tf * idf
		result = append(result, &QueryResult{
			DocumentName: ix.docNames[p.ID],
			DocumentID:   p.ID,
			TokenCount:   l,
			Rank:         rank,
		})
	}
	return result
}

func levenshtein(s, t string) int {
	var (
		i, j int
		m, n = len(s), len(t)
		d    = make([][]int, m+1) // d[0..m, 0..n]
	)
	for i = range d {
		d[i] = make([]int, n+1)
		d[i][0] = i
	}
	for j = range d[0] {
		d[0][j] = j
	}
	for j := 1; j <= n; j++ {
		for i := 1; i <= m; i++ {
			if s[i-1] == t[j-1] {
				d[i][j] = d[i-1][j-1]
			} else {
				min := d[i-1][j]
				if d[i][j-1] < min {
					min = d[i][j-1]
				}
				if d[i-1][j-1] < min {
					min = d[i-1][j-1]
				}
				d[i][j] = min + 1
			}
		}
	}
	return d[m][n]
}
