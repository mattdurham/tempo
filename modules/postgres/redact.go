package postgres

import (
	"net/url"
	"regexp"
)

// keywordPasswordPattern matches a `password=value` (or `password='value'`)
// pair in a libpq keyword/value DSN (e.g. "host=x password=secret dbname=y").
var keywordPasswordPattern = regexp.MustCompile(`(?i)(password=)('[^']*'|[^\s]*)`)

// RedactDSN returns dsn with any embedded password replaced by "***", safe to
// log or surface in a UI. Never log cfg.DSN directly -- always go through this
// helper first. Handles both postgres:// URL DSNs and libpq keyword/value DSNs.
func RedactDSN(dsn string) string {
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		if _, hasPassword := u.User.Password(); hasPassword {
			u.User = url.UserPassword(u.User.Username(), "***")
			return u.String()
		}
	}
	return keywordPasswordPattern.ReplaceAllString(dsn, "${1}***")
}
