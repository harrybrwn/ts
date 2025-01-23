package ts

import (
	"bufio"
	"io"
	"strings"
	"unicode"

	"github.com/jdkato/prose/v2"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

var transformer = transform.Chain(
	norm.NFD,
	runes.Remove(runes.In(unicode.Mn)),
	norm.NFKD,
)

type token struct {
	text string
	pos  uint
}

type tokenizer interface{ Next() (token, error) }

type customTokenizer struct {
	buf   *bufio.Reader
	cache []token
	pos   uint
}

func (ct *customTokenizer) Next() (token, error) {
	var (
		err     error
		segment string
		parts   []string
		raw     []rune
	)
	if len(ct.cache) > 0 {
		return ct.pop()
	}
Next:
	segment, err = ct.buf.ReadString(' ')
	if err != nil {
		return token{}, err
	}
	parts = strings.Split(segment, "\n")
	for _, b := range parts {
		b = strings.Trim(b, " \n\t\r")
		raw = cleanWord(b)
		if len(raw) == 0 {
			continue
		}
		tok := string(raw)
		if IsStopWord(tok) {
			continue
		}
		ct.pos++
		ct.cache = append(ct.cache, token{
			pos:  ct.pos,
			text: tok,
		})
	}
	if len(ct.cache) == 0 {
		goto Next
	}
	return ct.pop()
}

func (ct *customTokenizer) pop() (token, error) {
	if len(ct.cache) == 0 {
		return token{}, io.EOF
	}
	res := ct.cache[len(ct.cache)-1]
	ct.cache = ct.cache[:len(ct.cache)-1]
	return res, nil
}

func newCustomTokenizer(r io.Reader) *customTokenizer {
	return &customTokenizer{
		buf: bufio.NewReader(r),
	}
}

func newTokenizer(body string) (tokenizer, error) {
	s, _, err := transform.String(transformer, body)
	if err != nil {
		return nil, err
	}
	doc, err := prose.NewDocument(
		s,
		prose.WithExtraction(false),
		prose.WithTagging(false),
		prose.WithSegmentation(false),
		prose.WithTokenization(true),
	)
	if err != nil {
		return nil, err
	}
	toks := doc.Tokens()
	tokens := make([]string, 0, len(toks))
	for _, t := range toks {
		tokens = append(tokens, t.Text)
	}
	return newTokenList(tokens), nil
}

func mustNewTokenizer(body string) tokenizer {
	t, err := newTokenizer(body)
	if err != nil {
		panic(err)
	}
	return t
}

func newTokenList(tokens []string) *tokenlist {
	return &tokenlist{tokens: tokens}
}

type tokenlist struct {
	tokens []string
	i      uint
}

func (tl *tokenlist) Next() (token, error) {
loop:
	for tl.i < uint(len(tl.tokens)) {
		switch tl.tokens[tl.i] {
		case ".", ",", "(", ")", ":", ";":
			tl.i++
		default:
			break loop
		}
	}
	if tl.i < uint(len(tl.tokens)) {
		t := token{pos: tl.i, text: tl.tokens[tl.i]}
		tl.i++
		return t, nil
	}
	return token{}, io.EOF
}

func cleanWord(w string) []rune {
	buf, _, err := transform.String(transformer, w)
	if err != nil {
		panic(err)
	}
	l := len(buf)
	if l == 0 {
		return nil
	}
	b := make([]rune, 0, l)
	for _, c := range buf {
		switch c {
		case ' ':
			return nil
		case '”':
			b = append(b, '"')
		case '’':
			b = append(b, '\'')
		default:
			b = append(b, unicode.ToLower(c))
		}
	}
	if len(b) > 0 {
		for len(b) > 0 {
			switch b[len(b)-1] {
			case
				'.', ',',
				'!', '?',
				':', ';',
				')':
				b = b[:len(b)-1]
			default:
				goto stop
			}
		}
	stop:
	}
	if len(b) > 1 {
		switch b[0] {
		case '(':
			b = b[1:]
		}
	}
	return b
}

func IsStopWord(term string) bool {
	_, ok := stopwords[term]
	return ok
}

var stopwords = map[string]struct{}{
	"i":          {},
	"me":         {},
	"my":         {},
	"myself":     {},
	"we":         {},
	"our":        {},
	"ours":       {},
	"ourselves":  {},
	"you":        {},
	"your":       {},
	"yours":      {},
	"yourself":   {},
	"yourselves": {},
	"he":         {},
	"him":        {},
	"his":        {},
	"himself":    {},
	"she":        {},
	"her":        {},
	"hers":       {},
	"herself":    {},
	"it":         {},
	"its":        {},
	"itself":     {},
	"they":       {},
	"them":       {},
	"their":      {},
	"theirs":     {},
	"themselves": {},
	"what":       {},
	"which":      {},
	"who":        {},
	"whom":       {},
	"this":       {},
	"that":       {},
	"these":      {},
	"those":      {},
	"am":         {},
	"is":         {},
	"are":        {},
	"was":        {},
	"were":       {},
	"be":         {},
	"been":       {},
	"being":      {},
	"have":       {},
	"has":        {},
	"had":        {},
	"having":     {},
	"do":         {},
	"does":       {},
	"did":        {},
	"doing":      {},
	"a":          {},
	"an":         {},
	"the":        {},
	"and":        {},
	"but":        {},
	"if":         {},
	"or":         {},
	"because":    {},
	"as":         {},
	"until":      {},
	"while":      {},
	"of":         {},
	"at":         {},
	"by":         {},
	"for":        {},
	"with":       {},
	"about":      {},
	"against":    {},
	"between":    {},
	"into":       {},
	"through":    {},
	"during":     {},
	"before":     {},
	"after":      {},
	"above":      {},
	"below":      {},
	"to":         {},
	"from":       {},
	"up":         {},
	"down":       {},
	"in":         {},
	"out":        {},
	"on":         {},
	"off":        {},
	"over":       {},
	"under":      {},
	"again":      {},
	"further":    {},
	"then":       {},
	"once":       {},
	"here":       {},
	"there":      {},
	"when":       {},
	"where":      {},
	"why":        {},
	"how":        {},
	"all":        {},
	"any":        {},
	"both":       {},
	"each":       {},
	"few":        {},
	"more":       {},
	"most":       {},
	"other":      {},
	"some":       {},
	"such":       {},
	"no":         {},
	"nor":        {},
	"not":        {},
	"only":       {},
	"own":        {},
	"same":       {},
	"so":         {},
	"than":       {},
	"too":        {},
	"very":       {},
	"s":          {},
	"t":          {},
	"can":        {},
	"will":       {},
	"just":       {},
	"don":        {},
	"should":     {},
	"now":        {},
}
