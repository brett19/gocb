package gocb

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// AnalyticsOptions is the set of options available to an Analytics query.
type AnalyticsOptions struct {
	// Timeout and context are used to control cancellation of the data stream. Any timeout or deadline will also be
	// propagated to the server.
	ServerSideTimeout    time.Duration
	Context              context.Context
	ClientContextID      string
	Raw                  map[string]interface{}
	Priority             bool
	PositionalParameters []interface{}
	NamedParameters      map[string]interface{}
	ReadOnly             bool

	// JSONSerializer is used to deserialize each row in the result. This should be a JSON deserializer as results are JSON.
	// NOTE: if not set then query will always default to DefaultJSONSerializer.
	Serializer JSONSerializer
}

func (opts *AnalyticsOptions) toMap(statement string) (map[string]interface{}, error) {
	execOpts := make(map[string]interface{})
	execOpts["statement"] = statement

	if opts.ServerSideTimeout != 0 {
		execOpts["timeout"] = opts.ServerSideTimeout.String()
	}

	if opts.ClientContextID == "" {
		execOpts["client_context_id"] = uuid.New()
	} else {
		execOpts["client_context_id"] = opts.ClientContextID
	}

	if opts.Priority {
		execOpts["priority"] = -1
	}

	if opts.PositionalParameters != nil && opts.NamedParameters != nil {
		return nil, errors.New("Positional and named parameters must be used exclusively")
	}

	if opts.PositionalParameters != nil {
		execOpts["args"] = opts.PositionalParameters
	}

	if opts.NamedParameters != nil {
		for key, value := range opts.NamedParameters {
			if !strings.HasPrefix(key, "$") {
				key = "$" + key
			}
			execOpts[key] = value
		}
	}

	if opts.Raw != nil {
		for k, v := range opts.Raw {
			execOpts[k] = v
		}
	}

	if opts.ReadOnly {
		execOpts["readonly"] = true
	}

	return execOpts, nil
}
