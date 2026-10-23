package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// maxKeysetWalkPages hard-bounds walkKeysetPages. A server that mints a new,
// distinct cursor on every page never repeats one, so the repeated-cursor guard
// cannot stop it. The walk fails at this many pages instead of spinning. At
// maxPaginationLimit (1000) rows a page the cap admits 100M rows, orders of
// magnitude past any real city, so a legitimate walk never reaches it.
const maxKeysetWalkPages = 100_000

// listPageError is a keyset list response the client cannot use: a 2xx page
// that decoded no list body, a next_cursor an earlier page already returned,
// or a walk still unfinished at its page cap. Rows built from such pages would
// read as a complete list, so the read fails instead. Read paths treat it the
// way they treat a generic 5xx, as an unhealthy server (ShouldFallbackForRead),
// so a local read can still answer.
type listPageError struct {
	request string // the list request, such as "GET /v0/city/alpha/convoys"
	page    int    // the 1-based page the read failed on
	detail  string
}

func (e *listPageError) Error() string {
	return fmt.Sprintf("%s: page %d: %s", e.request, e.page, e.detail)
}

// isListPageError reports whether err is a keyset list page the client could
// not use.
func isListPageError(err error) bool {
	var lpe *listPageError
	return errors.As(err, &lpe)
}

// keysetPage is one response of a keyset-paginated list, as a list's fetch
// closure hands it to walkKeysetPages. http is the raw response, which carries
// the cache-age header. ok is false when the 2xx response decoded no list body.
// The other fields are the decoded body: its rows, its next_cursor, and the
// partial-read state the server reported for it.
type keysetPage[T any] struct {
	http          *http.Response
	ok            bool
	items         []T
	next          string
	partial       bool
	partialErrors []string
}

// newKeysetPage builds a decoded keysetPage from the fields every generated
// keyset list body carries under the same names.
func newKeysetPage[T any](resp *http.Response, items []T, next *string, partial *bool, partialErrors *[]string) keysetPage[T] {
	page := keysetPage[T]{http: resp, ok: true, items: items, next: derefStr(next), partial: derefBool(partial)}
	if partialErrors != nil {
		page.partialErrors = *partialErrors
	}
	return page
}

// keysetList is a keyset-paginated list read to its end by walkKeysetPages.
type keysetList[T any] struct {
	items         []T      // every page's rows in server order, each key once
	partial       bool     // true when any page reported a partial read
	partialErrors []string // the first error each failed backend label reported
	ageSeconds    float64  // the first page's X-GC-Cache-Age-S
}

// listResponseError returns the error a generated list response stands for:
// a failed request, no response at all, or a non-2xx status. It returns nil
// for a 2xx response.
func listResponseError[S any, R interface {
	*S
	StatusCode() int
}](resp R, err error) error {
	if err != nil {
		return &connError{err: fmt.Errorf("request failed: %w", err)}
	}
	if resp == nil {
		return &connError{err: fmt.Errorf("nil response")}
	}
	return apiErrorFromResponse(resp.StatusCode(), pdOf(resp))
}

// walkKeysetPages reads a keyset-paginated list to its end and returns its
// distinct rows in server order. fetch requests one page, the first with an
// empty cursor and each later one with the cursor the page before it returned,
// asking for at most pageLimit rows. key identifies a row. request names the
// list in errors. limit stops the walk once that many distinct rows are in
// hand, and 0 walks every page. maxPages fails a walk still unfinished after
// that many pages.
//
// The keyset list endpoints cut their pages at the server default of 100 rows
// unless asked for more, so a caller that wants the whole list must walk it:
// one request reads as a complete list while holding only the first page. Each
// page asks for the maxPaginationLimit server cap, or for the rows limit still
// wants when that is fewer, which keeps the walk to as few round trips as the
// server allows.
//
// The server rebuilds and re-sorts the list for every page, so a row whose sort
// key moved between two requests can come back on both. The walk keeps the
// first copy. The server also re-reads its backends for every page, so one page
// can be partial while another is complete. The list is partial when any page
// was, and it keeps the first error each failed backend reported, keyed by the
// label the server's partial errors lead with (the text before the first ": ",
// such as "rig alpha").
//
// A page the walk cannot use fails the whole read with a listPageError. A 2xx
// response that decoded no list body would otherwise end the walk as though
// the list were complete. A next_cursor an earlier page already returned would
// otherwise repeat rows or spin forever: keyset cursors only advance, so a
// repeat means the server is not honoring them.
func walkKeysetPages[T any](request string, limit, maxPages int, fetch func(cursor string, pageLimit int64) (keysetPage[T], error), key func(T) string) (keysetList[T], error) {
	list := keysetList[T]{items: []T{}}
	seenRows := map[string]bool{}
	seenLabels := map[string]bool{}
	requested := map[string]bool{}
	cursor := ""
	for page := 1; ; page++ {
		pageLimit := maxPaginationLimit
		if limit > 0 && limit-len(list.items) < pageLimit {
			pageLimit = limit - len(list.items)
		}
		resp, err := fetch(cursor, int64(pageLimit))
		if err != nil {
			return keysetList[T]{}, err
		}
		if !resp.ok {
			return keysetList[T]{}, &listPageError{request: request, page: page, detail: fmt.Sprintf("the 2xx response decoded no list body (Content-Type %q)", responseContentType(resp.http))}
		}
		if page == 1 {
			list.ageSeconds = cacheAgeFromResponse(resp.http)
		}
		for _, item := range resp.items {
			k := key(item)
			if seenRows[k] {
				continue
			}
			seenRows[k] = true
			list.items = append(list.items, item)
		}
		list.partial = list.partial || resp.partial
		for _, msg := range resp.partialErrors {
			label := partialErrorLabel(msg)
			if seenLabels[label] {
				continue
			}
			seenLabels[label] = true
			list.partialErrors = append(list.partialErrors, msg)
		}
		if limit > 0 && len(list.items) >= limit {
			return list, nil
		}
		if resp.next == "" {
			return list, nil
		}
		if requested[resp.next] {
			return keysetList[T]{}, &listPageError{request: request, page: page, detail: fmt.Sprintf("next_cursor %q repeated: an earlier page already returned it, so the server is not advancing its cursor", resp.next)}
		}
		if page >= maxPages {
			return keysetList[T]{}, &listPageError{request: request, page: page, detail: fmt.Sprintf("the walk exceeded %d pages without reaching the end of the list", maxPages)}
		}
		requested[resp.next] = true
		cursor = resp.next
	}
}

// partialErrorLabel is the backend label a server partial error leads with:
// the text before its first ": ", such as "rig alpha" or "mail provider ops". A
// message with no ": " is its own label.
func partialErrorLabel(msg string) string {
	if label, _, ok := strings.Cut(msg, ": "); ok {
		return label
	}
	return msg
}

// responseContentType is resp's Content-Type header, or "" when there is no
// response.
func responseContentType(resp *http.Response) string {
	if resp == nil {
		return ""
	}
	return resp.Header.Get("Content-Type")
}

// keysetListRequest names a city-scoped keyset list request in errors, for
// example "GET /v0/city/alpha/convoys".
func keysetListRequest(cityName, list string) string {
	return "GET /v0/city/" + cityName + "/" + list
}
