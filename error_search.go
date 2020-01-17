package gocb

// SearchError is the error type of all search query errors.
type SearchError struct {
	InnerError    error         `json:"-"`
	Query         interface{}   `json:"query,omitempty"`
	Endpoint      string        `json:"endpoint,omitempty"`
	RetryReasons  []RetryReason `json:"retry_reasons,omitempty"`
	RetryAttempts uint32        `json:"retry_attempts,omitempty"`
}

// Error returns the string representation of this error.
func (e SearchError) Error() string {
	return e.InnerError.Error() + " | " + serializeWrappedError(e)
}

// Unwrap returns the underlying cause for this error.
func (e SearchError) Unwrap() error {
	return e.InnerError
}
