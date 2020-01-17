package gocb

import (
	"time"

	cbsearch "github.com/couchbase/gocb/v2/search"
)

// SearchHighlightStyle indicates the type of highlighting to use for a search query.
type SearchHighlightStyle string

const (
	// DefaultHighlightStyle specifies to use the default to highlight search result hits.
	DefaultHighlightStyle = SearchHighlightStyle("")

	// HTMLHighlightStyle specifies to use HTML tags to highlight search result hits.
	HTMLHighlightStyle = SearchHighlightStyle("html")

	// AnsiHightlightStyle specifies to use ANSI tags to highlight search result hits.
	AnsiHightlightStyle = SearchHighlightStyle("ansi")
)

// SearchScanConsistency indicates the level of data consistency desired for a search query.
type SearchScanConsistency int

const (
	searchScanConsistencyNotSet = SearchScanConsistency(0)

	// SearchScanConsistencyNotBounded indicates no data consistency is required.
	SearchScanConsistencyNotBounded = SearchScanConsistency(1)
)

// SearchHighlightOptions are the options available for search highlighting.
type SearchHighlightOptions struct {
	Style  SearchHighlightStyle
	Fields []string
}

// SearchOptions represents a pending search query.
type SearchOptions struct {
	ScanConsistency SearchScanConsistency
	Limit           int
	Skip            int
	Explain         bool
	Highlight       *SearchHighlightOptions
	Fields          []string
	Sort            []cbsearch.Sort
	Facets          map[string]cbsearch.Facet
	ConsistentWith  *MutationState
	Raw             map[string]interface{}

	Timeout       time.Duration
	RetryStrategy RetryStrategy

	parentSpan requestSpanContext
}

func (opts *SearchOptions) toMap() (map[string]interface{}, error) {
	data := make(map[string]interface{})

	data["size"] = opts.Limit
	data["from"] = opts.Skip
	data["explain"] = opts.Explain
	data["fields"] = opts.Fields
	data["sort"] = opts.Sort

	if opts.Highlight != nil {
		highlight := make(map[string]interface{})
		highlight["style"] = string(opts.Highlight.Style)
		highlight["fields"] = opts.Highlight.Fields
		data["highlight"] = highlight
	}

	if opts.Facets != nil {
		facets := make(map[string]interface{})
		for k, v := range opts.Facets {
			facets[k] = v
		}
		data["facets"] = facets
	}

	if opts.ScanConsistency != 0 && opts.ConsistentWith != nil {
		return nil, makeInvalidArgumentsError("ScanConsistency and ConsistentWith must be used exclusively")
	}

	var ctl map[string]interface{}

	if opts.ScanConsistency != searchScanConsistencyNotSet {
		consistency := make(map[string]interface{})

		if opts.ScanConsistency == SearchScanConsistencyNotBounded {
			consistency["level"] = "not_bounded"
		} else {
			return nil, makeInvalidArgumentsError("unexpected consistency option")
		}

		if ctl == nil {
			ctl = make(map[string]interface{})
		}
		ctl["consistency"] = consistency
	}

	if opts.ConsistentWith != nil {
		consistency := make(map[string]interface{})

		consistency["level"] = "at_plus"
		consistency["vectors"] = opts.ConsistentWith.toSearchMutationState()

		if ctl == nil {
			ctl = make(map[string]interface{})
		}
		ctl["consistency"] = consistency
	}

	if opts.Raw != nil {
		for k, v := range opts.Raw {
			data[k] = v
		}
	}

	return data, nil
}
