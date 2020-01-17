package gocb

import (
	"encoding/json"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

func serializeWrappedError(err error) string {
	errBytes, serErr := json.Marshal(err)
	if serErr != nil {
		logErrorf("failed to serialize error to json: %s", serErr.Error())
	}
	return string(errBytes)
}

func maybeEnhanceCoreErr(err error) error {
	if kvErr, ok := err.(gocbcore.KeyValueError); ok {
		return KeyValueError{
			InnerError:       kvErr.InnerError,
			StatusCode:       kvErr.StatusCode,
			BucketName:       kvErr.BucketName,
			ScopeName:        kvErr.ScopeName,
			CollectionName:   kvErr.CollectionName,
			CollectionID:     kvErr.CollectionID,
			ErrorName:        kvErr.ErrorName,
			ErrorDescription: kvErr.ErrorDescription,
			Opaque:           kvErr.Opaque,
			Context:          kvErr.Context,
			Ref:              kvErr.Ref,
			RetryReasons:     translateCoreRetryReasons(kvErr.RetryReasons),
			RetryAttempts:    kvErr.RetryAttempts,
		}
	}
	if viewErr, ok := err.(gocbcore.ViewError); ok {
		return ViewError{
			InnerError:         viewErr.InnerError,
			DesignDocumentName: viewErr.DesignDocumentName,
			ViewName:           viewErr.ViewName,
			Errors:             translateCoreViewErrorDesc(viewErr.Errors),
			Endpoint:           viewErr.Endpoint,
			RetryReasons:       translateCoreRetryReasons(viewErr.RetryReasons),
			RetryAttempts:      viewErr.RetryAttempts,
		}
	}
	if queryErr, ok := err.(gocbcore.N1QLError); ok {
		return QueryError{
			InnerError:      queryErr.InnerError,
			Statement:       queryErr.Statement,
			ClientContextID: queryErr.ClientContextID,
			Errors:          translateCoreQueryErrorDesc(queryErr.Errors),
			Endpoint:        queryErr.Endpoint,
			RetryReasons:    translateCoreRetryReasons(queryErr.RetryReasons),
			RetryAttempts:   queryErr.RetryAttempts,
		}
	}
	if analyticsErr, ok := err.(gocbcore.AnalyticsError); ok {
		return AnalyticsError{
			InnerError:      analyticsErr.InnerError,
			Statement:       analyticsErr.Statement,
			ClientContextID: analyticsErr.ClientContextID,
			Errors:          translateCoreAnalyticsErrorDesc(analyticsErr.Errors),
			Endpoint:        analyticsErr.Endpoint,
			RetryReasons:    translateCoreRetryReasons(analyticsErr.RetryReasons),
			RetryAttempts:   analyticsErr.RetryAttempts,
		}
	}
	if httpErr, ok := err.(gocbcore.HTTPError); ok {
		return HTTPError{
			InnerError:    httpErr.InnerError,
			UniqueID:      httpErr.UniqueID,
			Endpoint:      httpErr.Endpoint,
			RetryReasons:  translateCoreRetryReasons(httpErr.RetryReasons),
			RetryAttempts: httpErr.RetryAttempts,
		}
	}
	return err
}

func maybeEnhanceKVErr(err error, bucketName, scopeName, collName, docKey string) error {
	return maybeEnhanceCoreErr(err)
}

func maybeEnhanceCollKVErr(err error, bucket kvProvider, coll *Collection, docKey string) error {
	return maybeEnhanceKVErr(err, coll.sb.BucketName, coll.Name(), coll.scopeName(), docKey)
}

func maybeEnhanceViewError(err error) error {
	return maybeEnhanceCoreErr(err)
}

func maybeEnhanceQueryError(err error) error {
	return maybeEnhanceCoreErr(err)
}

func maybeEnhanceAnalyticsError(err error) error {
	return maybeEnhanceCoreErr(err)
}

func maybeEnhanceSearchError(err error) error {
	return maybeEnhanceCoreErr(err)
}
