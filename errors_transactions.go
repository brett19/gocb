package gocb

import (
	"errors"
	"github.com/couchbase/gocbcore/v10"
)

var (
	// ErrOther indicates an non-specific error has occured.
	ErrOther = gocbcore.ErrOther

	// ErrTransient indicates a transient error occured which may succeed at a later point in time.
	ErrTransient = gocbcore.ErrTransient

	// ErrWriteWriteConflict indicates that another transaction conflicted with this one.
	ErrWriteWriteConflict = gocbcore.ErrWriteWriteConflict

	// ErrHard indicates that an unrecoverable error occured.
	ErrHard = gocbcore.ErrHard

	// ErrAmbiguous indicates that a failure occured but the outcome was not known.
	ErrAmbiguous = gocbcore.ErrAmbiguous

	// ErrAtrFull indicates that the ATR record was too full to accept a new mutation.
	ErrAtrFull = gocbcore.ErrAtrFull

	// ErrAttemptExpired indicates an transactionAttempt expired
	ErrAttemptExpired = gocbcore.ErrAttemptExpired

	// ErrAtrNotFound indicates that an expected ATR document was missing
	ErrAtrNotFound = gocbcore.ErrAtrNotFound

	// ErrAtrEntryNotFound indicates that an expected ATR entry was missing
	ErrAtrEntryNotFound = gocbcore.ErrAtrEntryNotFound

	// ErrDocAlreadyInTransaction indicates that a document is already in a transaction.
	ErrDocAlreadyInTransaction = gocbcore.ErrDocAlreadyInTransaction

	// ErrTransactionAbortedExternally indicates the transaction was aborted externally.
	ErrTransactionAbortedExternally = gocbcore.ErrTransactionAbortedExternally

	// ErrPreviousOperationFailed indicates a previous operation already failed.
	ErrPreviousOperationFailed = gocbcore.ErrPreviousOperationFailed

	// ErrForwardCompatibilityFailure indicates an operation failed due to involving a document in another transaction
	// which contains features this transaction does not support.
	ErrForwardCompatibilityFailure = gocbcore.ErrForwardCompatibilityFailure

	// ErrIllegalState is used for when a transaction enters an illegal State.
	ErrIllegalState = gocbcore.ErrIllegalState

	ErrAttemptNotFoundOnQuery = errors.New("transactionAttempt not found on query")

	// ErrCommitNotPermitted indicates that a commit is not allowed in the current transaction state.
	ErrCommitNotPermitted = gocbcore.ErrCommitNotPermitted

	// ErrRollbackNotPermitted indicates that a rollback is not allowed in the current transaction state.
	ErrRollbackNotPermitted = gocbcore.ErrRollbackNotPermitted

	// ErrConcurrentOperationsDetectedOnSameDocument indicates that concurrent Key-Value operations on the same document have been detected.
	ErrConcurrentOperationsDetectedOnSameDocument = gocbcore.ErrConcurrentOperationsDetectedOnSameDocument
)

// TransactionFailedError is returned when a transaction did not commit, for example because an operation failed in a
// way which could not be retried. Any changes the transaction staged were rolled back, or will be removed by cleanup.
type TransactionFailedError struct {
	cause  error
	result *TransactionResult
}

func (tfe TransactionFailedError) Error() string {
	if tfe.cause == nil {
		return "transaction failed"
	}
	return "transaction failed | " + tfe.cause.Error()
}

func (tfe TransactionFailedError) Unwrap() error {
	return tfe.cause
}

// Internal: This should never be used and is not supported.
func (tfe TransactionFailedError) Result() *TransactionResult {
	return tfe.result
}

// TransactionExpiredError is returned when a transaction reached its timeout before it could commit. The transaction
// did not commit. Once a transaction has expired it is not retried and it is not rolled back: anything it staged is
// left for cleanup to remove. Operations which were started before the timeout are allowed to finish for at most
// a short grace period (gocbcore.TransactionExpiryGrace, 2.5s) after it, so the transaction returns no later than
// that.
//
// errors.Is(err, ErrAttemptExpired) is true for this error. If the transaction expired while deciding whether to
// retry after an error, that error is included in the message.
type TransactionExpiredError struct {
	cause  error
	result *TransactionResult
}

func (tfe TransactionExpiredError) Error() string {
	if tfe.cause == nil || errors.Is(tfe.cause, ErrAttemptExpired) {
		return ErrAttemptExpired.Error()
	}
	return ErrAttemptExpired.Error() + " | " + tfe.cause.Error()
}

func (tfe TransactionExpiredError) Unwrap() error {
	return ErrAttemptExpired
}

// Internal: This should never be used and is not supported.
func (tfe TransactionExpiredError) Result() *TransactionResult {
	return tfe.result
}

// TransactionCommitAmbiguousError is returned when the transaction may or may not have committed, for example because
// the write that commits it timed out or the transaction expired while its outcome was being resolved. Under no
// circumstances is a transaction rolled back after an ambiguous commit.
type TransactionCommitAmbiguousError struct {
	cause  error
	result *TransactionResult
}

func (tfe TransactionCommitAmbiguousError) Error() string {
	if tfe.cause == nil {
		return "transaction commit ambiguous"
	}
	return "transaction failed | " + tfe.cause.Error()
}

func (tfe TransactionCommitAmbiguousError) Unwrap() error {
	return tfe.cause
}

// Internal: This should never be used and is not supported.
func (tfe TransactionCommitAmbiguousError) Result() *TransactionResult {
	return tfe.result
}

type TransactionFailedPostCommit struct {
	cause  error
	result *TransactionResult
}

func (tfe TransactionFailedPostCommit) Error() string {
	if tfe.cause == nil {
		return "transaction failed post commit"
	}
	return "transaction failed | " + tfe.cause.Error()
}

func (tfe TransactionFailedPostCommit) Unwrap() error {
	return tfe.cause
}

// Internal: This should never be used and is not supported.
func (tfe TransactionFailedPostCommit) Result() *TransactionResult {
	return tfe.result
}

// TransactionOperationFailedError is used when a transaction operation fails.
// Internal: This should never be used and is not supported.
type TransactionOperationFailedError struct {
	shouldRetry       bool
	shouldNotRollback bool
	errorCause        error
	shouldRaise       gocbcore.TransactionErrorReason
	errorClass        gocbcore.TransactionErrorClass
}

func (tfe TransactionOperationFailedError) Error() string {
	if tfe.errorCause == nil {
		return "transaction operation failed"
	}
	return "transaction operation failed | " + tfe.errorCause.Error()
}

// InternalUnwrap returns the underlying error for this error.
func (tfe TransactionOperationFailedError) InternalUnwrap() error {
	return tfe.errorCause
}

// Retry signals whether a new transactionAttempt should be made at rollback.
func (tfe TransactionOperationFailedError) Retry() bool {
	return tfe.shouldRetry
}

// Rollback signals whether the transactionAttempt should be auto-rolled back.
func (tfe TransactionOperationFailedError) Rollback() bool {
	return !tfe.shouldNotRollback
}

// ToRaise signals which error type should be raised to the application.
func (tfe TransactionOperationFailedError) ToRaise() TransactionErrorReason {
	return TransactionErrorReason(tfe.shouldRaise)
}

func createTransactionOperationFailedError(err error) error {
	if err == nil {
		return nil
	}

	var txnErr *gocbcore.TransactionOperationFailedError
	if errors.As(err, &txnErr) {
		return &TransactionOperationFailedError{
			shouldRetry:       txnErr.Retry(),
			shouldNotRollback: !txnErr.Rollback(),
			errorCause:        txnErr.InternalUnwrap(),
			shouldRaise:       txnErr.ToRaise(),
			errorClass:        txnErr.ErrorClass(),
		}
	}

	return &TransactionOperationFailedError{
		errorCause: err,
		errorClass: gocbcore.TransactionErrorClassFailOther,
	}
}

// operationFailedFromError converts err, returned by a transaction operation, into a TransactionOperationFailedError.
//
// A TransactionOperationFailedError from gocbcore has already set the attempt's state. Any other error comes from gocb
// itself, for example when a value cannot be encoded, and fails the transaction like any other error that is not a
// TransactionOperationFailed (transactions-design-doc.md, Exceptions Raised by the Application Lambda): it raises
// FAILED and sets the attempt's state, which leaves the attempt unable to commit.
//
// This is for operations within the lambda. Errors from commit and rollback use createTransactionOperationFailedError,
// which does not change the attempt's state.
func operationFailedFromError(err error, c *TransactionAttemptContext) error {
	if err == nil {
		return nil
	}

	var txnErr *gocbcore.TransactionOperationFailedError
	if errors.As(err, &txnErr) {
		return createTransactionOperationFailedError(err)
	}

	return operationFailed(transactionOperationFailedDef{
		ShouldNotRetry: true,
		Reason:         gocbcore.TransactionErrorReasonTransactionFailed,
		ErrorCause:     err,
		ErrorClass:     gocbcore.TransactionErrorClassFailOther,
	}, c)
}

func errorReasonFromString(reason string) gocbcore.TransactionErrorReason {
	switch reason {
	case "failed":
		return gocbcore.TransactionErrorReasonTransactionFailed
	case "expired":
		return gocbcore.TransactionErrorReasonTransactionExpired
	case "commit_ambiguous":
		return gocbcore.TransactionErrorReasonTransactionCommitAmbiguous
	case "failed_post_commit":
		return gocbcore.TransactionErrorReasonTransactionFailedPostCommit
	default:
		return gocbcore.TransactionErrorReasonTransactionFailed
	}
}
