package ts

type QueryResult struct {
	Rank         float64
	TokenCount   int
	DocumentName string
	DocumentID   uint64
}

type QueryResults []*QueryResult

func (r QueryResults) Len() int           { return len(r) }
func (r QueryResults) Less(i, j int) bool { return r[i].Rank >= r[j].Rank }
func (r QueryResults) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

type Query interface {
	Join([][]*posting) []*posting
	Keys() []string
}

func And(queries ...Query) Query {
	return &intersectQuery{queries: queries}
}

func Or(query ...string) Query {
	tokens := make([]string, len(query))
	for i, t := range query {
		tokens[i] = string(cleanWord(t))
	}
	return &unionQuery{queries: tokens}
}

type StringQuery string

func (sq StringQuery) Join(p [][]*posting) []*posting {
	postings := make([]*posting, 0, len(p))
	for _, posts := range p {
		postings = append(postings, posts...)
	}
	return postings
}

func (sq StringQuery) Keys() []string {
	k := cleanWord(string(sq))
	return []string{string(k)}
}

type intersectQuery struct{ queries []Query }

func (iq *intersectQuery) Join(posts [][]*posting) []*posting {
	postings := make([][]*posting, 0, len(posts))
	for _, q := range iq.queries {
		postings = append(postings, q.Join(posts))
	}
	return kIntersect(postings)
}

func (iq *intersectQuery) Keys() []string {
	keys := make([]string, 0)
	for _, q := range iq.queries {
		keys = append(keys, q.Keys()...)
	}
	return keys
}

type unionQuery struct{ queries []string }

func (uq *unionQuery) Keys() []string { return uq.queries }

func (uq *unionQuery) Join(postings [][]*posting) []*posting {
	p := make([]*posting, len(postings)*2)
	for _, posting := range postings {
		p = append(p, posting...)
	}
	return p
}

// QueryTree results in the combination of two queries
func QueryTree(l, r Query) Query {
	return &queryTree{left: l, right: r}
}

type queryTree struct{ left, right Query }

func (qt *queryTree) Keys() []string {
	left, right := qt.left.Keys(), qt.right.Keys()
	keys := make([]string, 0, len(left)+len(right))
	keys = append(keys, left...)
	return append(keys, right...)
}

func (qt *queryTree) Join(postings [][]*posting) []*posting {
	p := make([]*posting, 0)
	p = append(p, qt.left.Join(postings)...)
	p = append(p, qt.right.Join(postings)...)
	return p
}

var (
	// interface checks
	_ Query = (*StringQuery)(nil)
	_ Query = (*intersectQuery)(nil)
	_ Query = (*unionQuery)(nil)
	_ Query = (*queryTree)(nil)
)

func kIntersect(list [][]*posting) []*posting {
	var (
		i            uint64
		ix, smallest uint64
		p            *posting
		n            = uint64(len(list))
		result       = make([]*posting, 0)
		iters        = make([]uint64, len(list))
	)

	for {
		ix = 1
		smallest = maxUint
		for i = 1; i < n; i++ {
			// Check bounds on all the iterator's current indices.
			if iters[i] >= uint64(len(list[i])) {
				return result
			}
			// If even one of the current values is not
			// the same as the rest, then we need to
			// find the list with the smallest current value
			// and advance it by one.
			if list[0][iters[0]].ID != list[i][iters[i]].ID {
				if list[i][iters[i]].ID < smallest {
					smallest = uint64(list[i][iters[i]].ID)
					ix = i
				}
				goto NotEq
			}
		}
		// All current IDs are equal, store the
		// positions in a new posting and advance
		// all iterators.
		p = &posting{
			ID:  list[0][iters[0]].ID,
			Pos: make([]uint, 0, n),
		}
		for k := uint(0); k < uint(n); k++ {
			p.Pos = append(p.Pos, list[k][iters[k]].Pos...)
			iters[k]++ // advance all
		}
		result = append(result, p)
		continue

	NotEq: // they are not equal, advance the smallest one
		for k := ix; k < n; k++ {
			if iters[k] >= uint64(len(list[k])) {
				continue
			}
			v := uint64(list[k][iters[k]].ID)
			if v < smallest {
				smallest = v
				ix = k
			}
		}
		iters[ix]++ // advance the smallest one
	}
}

func intersect(left, right []*posting) []*posting {
	l := 0
	r := 0
	res := make([]*posting, 0, len(left)+len(right))
	for l < len(left) && r < len(right) {
		if left[l].ID < right[r].ID {
			l++
		} else if right[r].ID < left[l].ID {
			r++
		} else {
			res = append(res, &posting{
				ID:  left[l].ID,
				Pos: append(left[l].Pos, right[r].Pos...),
			})
			l++
			r++
		}
	}
	return res
}
