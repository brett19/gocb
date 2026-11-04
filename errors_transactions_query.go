package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"

	"github.com/couchbase/gocbcore/v10"
)

func queryErrorCodeToError(code uint32, c *TransactionAttemptContext) error {
	switch code {
	case 1065:
		return operationFailed(transactionOperationFailedDef{
			ShouldNotRetry: true,
			Reason:         gocbcore.TransactionErrorReasonTransactionFailed,
			ErrorCause:     ErrFeatureNotAvailable,
		}, c)
	case 1080:
		return operationFailed(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			Reason:            gocbcore.TransactionErrorReasonTransactionExpired,
			ErrorCause:        gocbcore.ErrAttemptExpired,
			ErrorClass:        gocbcore.TransactionErrorClassFailExpiry,
			ShouldNotRollback: true,
		}, c)
	case 17004:
		return ErrAttemptNotFoundOnQuery
	case 17010:
		return operationFailed(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			Reason:            gocbcore.TransactionErrorReasonTransactionExpired,
			ErrorCause:        gocbcore.ErrAttemptExpired,
			ErrorClass:        gocbcore.TransactionErrorClassFailExpiry,
			ShouldNotRollback: true,
		}, c)
	case 17012:
		return ErrDocumentExists
	case 17014:
		return ErrDocumentNotFound
	case 17015:
		return ErrCasMismatch
	default:
		return nil
	}
}

func queryCauseToOperationFailedError(queryErr *QueryError, c *TransactionAttemptContext) error {

	var operationFailedErrs []jsonQueryTransactionOperationFailedCause
	if err := json.Unmarshal([]byte(queryErr.ErrorText), &operationFailedErrs); err == nil {
		for _, operationFailedErr := range operationFailedErrs {
			if operationFailedErr.Cause != nil {
				if operationFailedErr.Code >= 17000 && operationFailedErr.Code <= 18000 {
					if err := queryErrorCodeToError(operationFailedErr.Code, c); err != nil {
						return err
					}
				}

				return operationFailed(transactionOperationFailedDef{
					ShouldNotRetry:    !operationFailedErr.Cause.Retry,
					ShouldNotRollback: !operationFailedErr.Cause.Rollback,
					Reason:            errorReasonFromString(operationFailedErr.Cause.Raise),
					ErrorCause:        queryErr,
					ShouldNotCommit:   true,
				}, c)
			}
		}
	}
	return nil
}

func queryMaybeTranslateToTransactionsError(err error, c *TransactionAttemptContext) error {
	if errors.Is(err, ErrTimeout) {
		return operationFailed(transactionOperationFailedDef{
			ShouldNotRetry: true,
			Reason:         gocbcore.TransactionErrorReasonTransactionExpired,
			ErrorCause:     err,
		}, c)
	}

	var queryErr *QueryError
	if !errors.As(err, &queryErr) {
		return err
	}

	if len(queryErr.Errors) == 0 {
		return queryErr
	}

	// If an error contains a cause field, use that error.
	// Otherwise, if an error has code between 17000 and 18000 inclusive, it is a transactions-related error. Use that.
	// Otherwise, fallback to using the first error.
	if err := queryCauseToOperationFailedError(queryErr, c); err != nil {
		return err
	}

	for _, e := range queryErr.Errors {
		if e.Code >= 17000 && e.Code <= 18000 {
			if err := queryErrorCodeToError(e.Code, c); err != nil {
				return err
			}
		}
	}

	if err := queryErrorCodeToError(queryErr.Errors[0].Code, c); err != nil {
		return err
	}

	return queryErr
}

// queryOutcomeUnknown reports whether err means that a statement ended without a definite answer from the query
// service: the statement may or may not have been executed, and the SDK cannot tell which.
//
// A response from the query service that carries errors is definite, and so is an unambiguous timeout, which gocbcore
// only raises when the request was not sent or was rejected before it was processed. An ambiguous timeout, a
// cancellation, and a connection that is lost while the request is in flight are not.
func queryOutcomeUnknown(err error) bool {
	if err == nil {
		return false
	}

	var queryErr *QueryError
	if errors.As(err, &queryErr) && len(queryErr.Errors) > 0 {
		return false
	}

	if errors.Is(err, ErrUnambiguousTimeout) {
		return false
	}

	if errors.Is(err, ErrTimeout) || errors.Is(err, ErrRequestCanceled) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}

	// A failure to resolve or to connect means that the request was never sent.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return false
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op != "dial"
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// queryMaybeTranslateCommitStatementError translates an error from a statement that commits a transaction: COMMIT in
// query mode, or the single statement of a single query transaction (tximplicit), which commits the transaction that
// the query service runs for it.
//
// The query service performs the whole commit within the statement, and then discards the transaction. So if the
// statement ends without a definite answer, the transaction may have committed, and the outcome is ambiguous. It is
// not expired, which tells the application that the transaction did not commit, and it must not be rolled back: the
// query service would reject a ROLLBACK, as the transaction is no longer present, or ignore it while the commit is
// still running. Any other error is translated as for any other statement.
func queryMaybeTranslateCommitStatementError(err error, c *TransactionAttemptContext) error {
	if queryOutcomeUnknown(err) {
		return operationFailed(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			ShouldNotCommit:   true,
			Reason:            gocbcore.TransactionErrorReasonTransactionCommitAmbiguous,
			ErrorCause:        err,
			ErrorClass:        gocbcore.TransactionErrorClassFailAmbiguous,
		}, c)
	}

	return queryMaybeTranslateToTransactionsError(err, c)
}

type transactionOperationFailedDef struct {
	ShouldNotRetry    bool
	ShouldNotRollback bool
	Reason            gocbcore.TransactionErrorReason
	ErrorCause        error
	ErrorClass        gocbcore.TransactionErrorClass
	ShouldNotCommit   bool
}

func operationFailed(def transactionOperationFailedDef, c *TransactionAttemptContext) *TransactionOperationFailedError {
	// An expired transaction can never be committed. Whatever else the error allows (rolling back, for example), the
	// Run loop must not go on to send COMMIT after it.
	if def.Reason == gocbcore.TransactionErrorReasonTransactionExpired {
		def.ShouldNotCommit = true
	}

	err := &TransactionOperationFailedError{
		shouldRetry:       !def.ShouldNotRetry,
		shouldNotRollback: def.ShouldNotRollback,
		errorCause:        def.ErrorCause,
		shouldRaise:       def.Reason,
		errorClass:        def.ErrorClass,
	}

	if c != nil {
		c.logger.logInfof(c.attemptID, "Operation failed: can still commit: %t, should not rollback: %t, should not retry: %t, "+
			"reason: %s", !def.ShouldNotCommit, def.ShouldNotRollback, def.ShouldNotRetry, def.Reason)
		c.updateState(def)
	}
	return err
}

func (c *TransactionAttemptContext) updateState(def transactionOperationFailedDef) {
	opts := gocbcore.TransactionUpdateStateOptions{}
	if def.ShouldNotRollback {
		opts.ShouldNotRollback = true
	}
	if def.ShouldNotRetry {
		opts.ShouldNotRetry = true
	}
	if def.ShouldNotCommit {
		opts.ShouldNotCommit = true
	}
	opts.Reason = def.Reason
	c.txn.UpdateState(opts)
}

func singleQueryErrToTransactionError(err error, txnID string) error {
	err = queryMaybeTranslateCommitStatementError(err, nil)

	var tErr *TransactionOperationFailedError
	if errors.As(err, &tErr) {
		switch tErr.shouldRaise {
		case gocbcore.TransactionErrorReasonTransactionFailed:
			return &TransactionFailedError{
				cause: tErr.errorCause,
				result: &TransactionResult{
					TransactionID:     txnID,
					UnstagingComplete: false,
				},
			}
		case gocbcore.TransactionErrorReasonTransactionCommitAmbiguous:
			return &TransactionCommitAmbiguousError{
				cause: tErr.errorCause,
				result: &TransactionResult{
					TransactionID:     txnID,
					UnstagingComplete: false,
				},
			}
		case gocbcore.TransactionErrorReasonTransactionExpired:
			return &TransactionExpiredError{
				result: &TransactionResult{
					TransactionID:     txnID,
					UnstagingComplete: false,
				},
			}
		}
	}

	return &TransactionFailedError{
		cause: err,
		result: &TransactionResult{
			TransactionID:     txnID,
			UnstagingComplete: false,
		},
	}
}
