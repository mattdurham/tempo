package logqlparser

import regexp "github.com/coregx/coregex"

type lineFilterMatcher struct {
	filterFn func(body string) bool
	compiled *regexp.Regexp
	pattern  string
}
