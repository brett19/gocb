package gocb

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocbcore/v10"
)

// Query executes the query statement on the server.
func (c *TransactionAttemptContext) Query(statement string, options *TransactionQueryOptions) (*TransactionQueryResult, error) {
	c.logger.logInfof(c.attemptID, "Performing query: %s", redactUserDataString(statement))
	var opts TransactionQueryOptions
	if options != nil {
		opts = *options
	}
	c.queryStateLock.Lock()
	res, err := c.queryWrapperWrapper(opts.Scope, statement, opts.toSDKOptions(), "query", false, true,
		nil, true)
	c.queryStateLock.Unlock()
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *TransactionAttemptContext) queryModeLocked() bool {
	return c.queryState != nil
}

func (c *TransactionAttemptContext) getQueryMode(collection *Collection, id string) (*TransactionGetResult, error) {
	c.logger.logInfof(c.attemptID, "Performing query mode get: %s", newLoggableDocKey(
		collection.bucketName(),
		collection.ScopeName(),
		collection.Name(),
		id,
	))
	txdata := map[string]interface{}{
		"kv": true,
	}

	b, err := json.Marshal(txdata)
	if err != nil {
		// TODO: should we be wrapping this? It really shouldn't happen...
		return nil, err
	}

	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrDocumentNotFound) {
			return err
		}

		return operationFailed(transactionOperationFailedDef{
			ShouldNotRetry: true,
			ErrorCause:     err,
			Reason:         gocbcore.TransactionErrorReasonTransactionFailed,
		}, c)
	}

	res, err := c.queryWrapperWrapper(c.queryState.scope, "EXECUTE __get", QueryOptions{
		PositionalParameters: []interface{}{c.keyspace(collection), id},
		Adhoc:                true,
	}, "queryKvGet", false, true, b, true)
	if err != nil {
		return nil, handleErr(err)
	}

	type getQueryResult struct {
		Scas    string          `json:"scas"`
		Doc     json.RawMessage `json:"doc"`
		TxnMeta json.RawMessage `json:"txnMeta,omitempty"`
	}

	var row getQueryResult
	err = res.One(&row)
	if err != nil {
		return nil, handleErr(err)
	}

	cas, err := fromScas(row.Scas)
	if err != nil {
		return nil, handleErr(err)
	}

	return &TransactionGetResult{
		collection: collection,
		docID:      id,

		transcoder: NewJSONTranscoder(),
		flags:      2 << 24,

		txnMeta: row.TxnMeta,

		coreRes: &gocbcore.TransactionGetResult{
			Value: row.Doc,
			Cas:   cas,
		},
	}, nil
}

func (c *TransactionAttemptContext) replaceQueryMode(doc *TransactionGetResult, value interface{}, opts *TransactionReplaceOptions) (*TransactionGetResult, error) {
	c.logger.logInfof(c.attemptID, "Performing query mode replace: %s", newLoggableDocKey(
		doc.collection.bucketName(),
		doc.collection.ScopeName(),
		doc.collection.Name(),
		doc.docID,
	))

	if opts.Expiry > 0 {
		return nil, operationFailed(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
			ErrorCause:        wrapError(ErrFeatureNotAvailable, "expiry is not available for queries"),
			ErrorClass:        gocbcore.TransactionErrorClassFailOther,
		}, c)
	}

	transcoder := opts.Transcoder
	if transcoder == nil {
		transcoder = c.transcoder
	}

	valueBytes, flags, err := transcoder.Encode(value)
	if err != nil {
		return nil, operationFailedFromError(err, c)
	}

	userFlags, _ := gocbcore.DecodeCommonFlags(flags)
	if userFlags == gocbcore.BinaryType {
		return nil, operationFailed(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
			ErrorCause:        wrapError(ErrFeatureNotAvailable, "binary transactions is not available for queries"),
			ErrorClass:        gocbcore.TransactionErrorClassFailOther,
		}, c)
	}

	txdata := map[string]interface{}{
		"kv":   true,
		"scas": toScas(doc.coreRes.Cas),
	}

	if len(doc.txnMeta) > 0 {
		txdata["txnMeta"] = doc.txnMeta
	}

	b, err := json.Marshal(txdata)
	if err != nil {
		return nil, err
	}

	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrDocumentNotFound) {
			return operationFailed(transactionOperationFailedDef{
				ErrorCause: err,
				Reason:     gocbcore.TransactionErrorReasonTransactionFailed,
				ErrorClass: gocbcore.TransactionErrorClassFailDocNotFound,
			}, c)
		} else if errors.Is(err, ErrCasMismatch) {
			return operationFailed(transactionOperationFailedDef{
				ErrorCause: err,
				Reason:     gocbcore.TransactionErrorReasonTransactionFailed,
				ErrorClass: gocbcore.TransactionErrorClassFailCasMismatch,
			}, c)
		}

		return operationFailed(transactionOperationFailedDef{
			ShouldNotRetry: true,
			ErrorCause:     err,
			Reason:         gocbcore.TransactionErrorReasonTransactionFailed,
		}, c)
	}

	params := []interface{}{c.keyspace(doc.collection), doc.docID, json.RawMessage(valueBytes), json.RawMessage("{}")}

	res, err := c.queryWrapperWrapper(c.queryState.scope, "EXECUTE __update", QueryOptions{
		PositionalParameters: params,
		Adhoc:                true,
	}, "queryKvReplace", false, true, b, true)
	if err != nil {
		return nil, handleErr(err)
	}

	type replaceQueryResult struct {
		Scas string          `json:"scas"`
		Doc  json.RawMessage `json:"doc"`
	}

	var row replaceQueryResult
	err = res.One(&row)
	if err != nil {
		return nil, handleErr(queryMaybeTranslateToTransactionsError(err, c))
	}

	cas, err := fromScas(row.Scas)
	if err != nil {
		return nil, handleErr(err)
	}

	return &TransactionGetResult{
		collection: doc.collection,
		docID:      doc.docID,

		transcoder: NewJSONTranscoder(),
		flags:      2 << 24,

		coreRes: &gocbcore.TransactionGetResult{
			Value: row.Doc,
			Cas:   cas,
		},
	}, nil
}

func (c *TransactionAttemptContext) insertQueryMode(collection *Collection, id string, value interface{}, opts *TransactionInsertOptions) (*TransactionGetResult, error) {
	c.logger.logInfof(c.attemptID, "Performing query mode insert: %s", newLoggableDocKey(
		collection.bucketName(),
		collection.ScopeName(),
		collection.Name(),
		id,
	))

	if opts.Expiry > 0 {
		return nil, operationFailed(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
			ErrorCause:        wrapError(ErrFeatureNotAvailable, "expiry is not available for queries"),
			ErrorClass:        gocbcore.TransactionErrorClassFailOther,
		}, c)
	}

	transcoder := opts.Transcoder
	if transcoder == nil {
		transcoder = c.transcoder
	}

	valueBytes, flags, err := transcoder.Encode(value)
	if err != nil {
		return nil, operationFailedFromError(err, c)
	}

	userFlags, _ := gocbcore.DecodeCommonFlags(flags)
	if userFlags == gocbcore.BinaryType {
		return nil, operationFailed(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
			ErrorCause: wrapError(
				ErrFeatureNotAvailable,
				"binary transactions is not available for queries",
			),
			ErrorClass: gocbcore.TransactionErrorClassFailOther,
		}, c)
	}

	txdata := map[string]interface{}{
		"kv": true,
	}

	b, err := json.Marshal(txdata)
	if err != nil {
		return nil, operationFailedFromError(err, c)
	}

	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}
		if errors.Is(err, ErrDocumentExists) {
			return err
		}

		return operationFailed(transactionOperationFailedDef{
			ShouldNotRetry: true,
			ErrorCause:     err,
			Reason:         gocbcore.TransactionErrorReasonTransactionFailed,
		}, c)
	}

	params := []interface{}{c.keyspace(collection), id, json.RawMessage(valueBytes), json.RawMessage("{}")}

	res, err := c.queryWrapperWrapper(c.queryState.scope, "EXECUTE __insert", QueryOptions{
		PositionalParameters: params,
		Adhoc:                true,
	}, "queryKvInsert", false, true, b, true)
	if err != nil {
		return nil, handleErr(err)
	}

	type insertQueryResult struct {
		Scas string `json:"scas"`
	}

	var row insertQueryResult
	err = res.One(&row)
	if err != nil {
		return nil, handleErr(queryMaybeTranslateToTransactionsError(err, c))
	}

	cas, err := fromScas(row.Scas)
	if err != nil {
		return nil, handleErr(err)
	}

	return &TransactionGetResult{
		collection: collection,
		docID:      id,

		transcoder: NewJSONTranscoder(),
		flags:      2 << 24,

		coreRes: &gocbcore.TransactionGetResult{
			Value: valueBytes,
			Cas:   cas,
		},
	}, nil
}

func (c *TransactionAttemptContext) removeQueryMode(doc *TransactionGetResult) error {
	c.logger.logInfof(c.attemptID, "Performing query mode remove: %s", newLoggableDocKey(
		doc.collection.bucketName(),
		doc.collection.ScopeName(),
		doc.collection.Name(),
		doc.docID,
	))
	txdata := map[string]interface{}{
		"kv":   true,
		"scas": toScas(doc.coreRes.Cas),
	}

	if len(doc.txnMeta) > 0 {
		txdata["txnMeta"] = doc.txnMeta
	}

	b, err := json.Marshal(txdata)
	if err != nil {
		return err
	}

	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrDocumentNotFound) {
			return operationFailed(transactionOperationFailedDef{
				ErrorCause: err,
				Reason:     gocbcore.TransactionErrorReasonTransactionFailed,
				ErrorClass: gocbcore.TransactionErrorClassFailDocNotFound,
			}, c)
		} else if errors.Is(err, ErrCasMismatch) {
			return operationFailed(transactionOperationFailedDef{
				ErrorCause: err,
				Reason:     gocbcore.TransactionErrorReasonTransactionFailed,
				ErrorClass: gocbcore.TransactionErrorClassFailCasMismatch,
			}, c)
		}

		return operationFailed(transactionOperationFailedDef{
			ShouldNotRetry: true,
			ErrorCause:     err,
			Reason:         gocbcore.TransactionErrorReasonTransactionFailed,
		}, c)
	}

	params := []interface{}{c.keyspace(doc.collection), doc.docID, json.RawMessage("{}")}

	_, err = c.queryWrapperWrapper(c.queryState.scope, "EXECUTE __delete", QueryOptions{
		PositionalParameters: params,
		Adhoc:                true,
	}, "queryKvRemove", false, true, b, true)
	if err != nil {
		return handleErr(err)
	}

	return nil
}

func (c *TransactionAttemptContext) commitQueryMode() error {
	c.logger.logInfof(c.attemptID, "Performing query mode commit")
	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrAttemptExpired) {
			return operationFailed(transactionOperationFailedDef{
				ErrorCause:        err,
				Reason:            gocbcore.TransactionErrorReasonTransactionCommitAmbiguous,
				ShouldNotRollback: true,
				ShouldNotRetry:    true,
				ErrorClass:        gocbcore.TransactionErrorClassFailExpiry,
			}, c)
		}

		return operationFailed(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			ErrorCause:        err,
			Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
		}, c)
	}

	_, err := c.queryWrapperWrapper(c.queryState.scope, "COMMIT", QueryOptions{
		Adhoc: true,
	}, transactionQueryCommitHookPoint, false, true, nil, true)
	c.txn.UpdateState(gocbcore.TransactionUpdateStateOptions{
		ShouldNotCommit: true,
	})
	if err != nil {
		return handleErr(err)
	}

	c.txn.UpdateState(gocbcore.TransactionUpdateStateOptions{
		ShouldNotRollback: true,
		ShouldNotRetry:    true,
		State:             gocbcore.TransactionAttemptStateCompleted,
	})

	return nil
}

// rollbackQueryMode rolls back a query mode attempt with a ROLLBACK statement.
//
// Every rollback that gocb performs is an auto-rollback, done by the Run loop after the lambda; gocb has no app
// rollback (ctx.rollback()). A TransactionOperationFailedError raised during an auto-rollback does not set the
// attempt's state bits, so it cannot change the error that the transaction raises: the application cares about what
// caused the rollback, not about a problem with the rollback itself (transactions-stages.md, rollbackInternal,
// ExtThreadSafety; transactions-query.md, queryWrapper, updateInternalState). The Run loop then raises the error that
// caused the rollback, without retrying. Whatever the outcome, the attempt can no longer be committed or rolled back,
// as for any rollback.
func (c *TransactionAttemptContext) rollbackQueryMode() error {
	c.logger.logInfof(c.attemptID, "Performing query mode rollback")
	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrAttemptNotFoundOnQuery) {
			return nil
		}

		return operationFailed(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			ErrorCause:        err,
			Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
		}, nil)
	}

	_, err := c.queryWrapperWrapper(c.queryState.scope, "ROLLBACK", QueryOptions{
		Adhoc: true,
	}, "queryRollback", false, false, nil, false)
	c.txn.UpdateState(gocbcore.TransactionUpdateStateOptions{
		ShouldNotRollback: true,
		ShouldNotCommit:   true,
	})
	if err != nil {
		err = handleErr(err)
		if err != nil {
			c.logger.logInfof(c.attemptID, "Query mode rollback failed, not updating state: %v", err)
		}
		return err
	}

	c.txn.UpdateState(gocbcore.TransactionUpdateStateOptions{
		State: gocbcore.TransactionAttemptStateRolledBack,
	})

	return nil
}

type jsonTransactionOperationFailed struct {
	Cause    interface{} `json:"cause"`
	Retry    bool        `json:"retry"`
	Rollback bool        `json:"rollback"`
	Raise    string      `json:"raise"`
}

type jsonQueryTransactionOperationFailedCause struct {
	Cause   *jsonTransactionOperationFailed `json:"cause"`
	Code    uint32                          `json:"code"`
	Message string                          `json:"message"`
}

func durabilityLevelToQueryString(level gocbcore.TransactionDurabilityLevel) string {
	switch level {
	case gocbcore.TransactionDurabilityLevelUnknown:
		return "unset"
	case gocbcore.TransactionDurabilityLevelNone:
		return "none"
	case gocbcore.TransactionDurabilityLevelMajority:
		return "majority"
	case gocbcore.TransactionDurabilityLevelMajorityAndPersistToActive:
		return "majorityAndPersistActive"
	case gocbcore.TransactionDurabilityLevelPersistToMajority:
		return "persistToMajority"
	}
	return ""
}

// transactionQueryCommitHookPoint is the hook point of the COMMIT statement.
const transactionQueryCommitHookPoint = "queryCommit"

// translateStatementError translates an error from sending a statement, or from reading its result. A statement that
// commits the transaction gets the commit-specific translation; see queryMaybeTranslateCommitStatementError.
//
// A TransactionOperationFailedError that the translation raises sets the attempt's state from stateCtx, unless it is
// nil; see queryWrapper.
func translateStatementError(err error, hookPoint string, txImplicit bool, stateCtx *TransactionAttemptContext) error {
	if txImplicit || hookPoint == transactionQueryCommitHookPoint {
		return queryMaybeTranslateCommitStatementError(err, stateCtx)
	}

	return queryMaybeTranslateToTransactionsError(err, stateCtx)
}

// queryStateContext returns the context whose state a TransactionOperationFailedError raised by a statement sets: c,
// or nil if the statement must not update the attempt's state.
func (c *TransactionAttemptContext) queryStateContext(updateInternalState bool) *TransactionAttemptContext {
	if !updateInternalState {
		return nil
	}
	return c
}

// queryWrapperWrapper is used by any Query based calls on TransactionAttemptContext that require a non-streaming
// result. It handles converting QueryResult To TransactionQueryResult, handling any errors that occur on the stream,
// or because of a FATAL status in metadata. See queryWrapper for updateInternalState.
func (c *TransactionAttemptContext) queryWrapperWrapper(scope *Scope, statement string, options QueryOptions, hookPoint string,
	isBeginWork bool, existingErrorCheck bool, txData []byte, updateInternalState bool) (*TransactionQueryResult, error) {
	result, err := c.queryWrapper(scope, statement, options, hookPoint, isBeginWork, existingErrorCheck, txData, false,
		updateInternalState)
	if err != nil {
		return nil, err
	}

	stateCtx := c.queryStateContext(updateInternalState)

	var results []json.RawMessage
	for result.Next() {
		var r json.RawMessage
		err = result.Row(&r)
		if err != nil {
			return nil, queryMaybeTranslateToTransactionsError(err, stateCtx)
		}

		results = append(results, r)
	}

	if err := result.Err(); err != nil {
		return nil, translateStatementError(err, hookPoint, false, stateCtx)
	}

	meta, err := result.MetaData()
	if err != nil {
		return nil, translateStatementError(err, hookPoint, false, stateCtx)
	}

	if meta.Status == QueryStatusFatal {
		return nil, operationFailed(transactionOperationFailedDef{
			ShouldNotRetry: true,
			Reason:         gocbcore.TransactionErrorReasonTransactionFailed,
		}, stateCtx)
	}

	return newTransactionQueryResult(results, meta, result.endpoint), nil
}

// queryWrapper is used by every query based call on TransactionAttemptContext. It handles actually sending the
// query as well as begin work and setting up query mode state. It returns a streaming QueryResult, handling only
// errors that occur at query call time.
//
// A TransactionOperationFailedError raised here sets the attempt's state bits only if updateInternalState is set
// (transactions-query.md, queryWrapper, ExtThreadSafety). It is not set for the ROLLBACK statement of an
// auto-rollback, as errors during an auto-rollback must not change the attempt's state, or the error that the
// transaction raises.
func (c *TransactionAttemptContext) queryWrapper(scope *Scope, statement string, options QueryOptions, hookPoint string,
	isBeginWork bool, existingErrorCheck bool, txData []byte, txImplicit bool, updateInternalState bool) (*QueryResult, error) {
	c.logger.logInfof(c.attemptID, "Query wrapped running %s, scope level = %t, begin work = %t, txImplicit = %t, "+
		"update internal state = %t", redactUserDataString(statement), scope != nil, isBeginWork, txImplicit,
		updateInternalState)

	stateCtx := c.queryStateContext(updateInternalState)

	var target string
	if !isBeginWork && !txImplicit {
		if !c.queryModeLocked() {
			// This is quite a big lock but we can't put the context into "query mode" until we know that begin work was
			// successful. We also can't allow any further ops to happen until we know if we're in "query mode" or not.

			// queryBeginWork implicitly performs an existingErrorCheck and the call into Serialize on the gocbcore side
			// will return an error if there have been any previously failed operations.
			if err := c.queryBeginWork(scope); err != nil {
				return nil, err
			}
		}

		// If we've got here then transactionQueryState cannot be nil.
		target = c.queryState.queryTarget

		c.logger.logInfof(c.attemptID, "Using query target %s", redactSystemDataString(target))

		if !c.txn.CanCommit() && !c.txn.ShouldRollback() {
			c.logger.logInfof(c.attemptID, "Transaction marked cannot commit and should not rollback, failing")
			return nil, operationFailed(transactionOperationFailedDef{
				ShouldNotRetry:    true,
				Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
				ErrorCause:        ErrOther,
				ErrorClass:        gocbcore.TransactionErrorClassFailOther,
				ShouldNotRollback: true,
			}, stateCtx)
		}
	}

	if existingErrorCheck {
		if !c.txn.CanCommit() {
			c.logger.logInfof(c.attemptID, "Transaction marked cannot commit during existing error check, failing")
			return nil, operationFailed(transactionOperationFailedDef{
				ShouldNotRetry: true,
				Reason:         gocbcore.TransactionErrorReasonTransactionFailed,
				ErrorCause:     ErrPreviousOperationFailed,
				ErrorClass:     gocbcore.TransactionErrorClassFailOther,
			}, stateCtx)
		}
	}

	expired, err := c.hooks.HasExpiredClientSideHook(*c, hookPoint, statement)
	if err != nil {
		// This isn't meant to happen...
		return nil, operationFailed(transactionOperationFailedDef{
			ShouldNotRetry: true,
			Reason:         gocbcore.TransactionErrorReasonTransactionFailed,
			ErrorCause:     err,
			ErrorClass:     gocbcore.TransactionErrorClassFailOther,
		}, stateCtx)
	}
	cfg := c.txn.Config()
	if cfg.ExpirationTime < 10*time.Millisecond || expired {
		c.logger.logInfof(c.attemptID, "Transaction expired, failing")
		// This refusal sets the attempt's state even when updateInternalState is false. A statement refused here was
		// never sent, so for an auto-rollback's ROLLBACK it means the transaction had already expired before the
		// rollback began: nothing was rolled back, and the transaction raises TransactionExpiredError, as when expiry
		// is detected at the first step of a KV rollback. Errors from a rollback that did begin still leave the state
		// alone, and an expiry refusal can only raise EXPIRED, never COMMIT_AMBIGUOUS.
		return nil, operationFailed(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            gocbcore.TransactionErrorReasonTransactionExpired,
			ErrorCause:        ErrAttemptExpired,
			ErrorClass:        gocbcore.TransactionErrorClassFailExpiry,
		}, c)
	}

	options.Metrics = true
	options.Internal.Endpoint = target
	if options.Raw == nil {
		options.Raw = make(map[string]interface{})
	}
	if !isBeginWork && !txImplicit {
		options.Raw["txid"] = c.txn.Attempt().ID
	}

	if len(txData) > 0 {
		options.Raw["txdata"] = json.RawMessage(txData)
	}
	if txImplicit {
		options.Raw["tximplicit"] = true

		if options.ScanConsistency == 0 {
			options.ScanConsistency = QueryScanConsistencyRequestPlus
		}
		options.Raw["durability_level"] = durabilityLevelToQueryString(cfg.DurabilityLevel)
		options.Raw["txtimeout"] = fmt.Sprintf("%dms", cfg.ExpirationTime.Milliseconds())
		if cfg.CustomATRLocation.Agent != nil {
			// Agent being non nil signifies that this was set.
			options.Raw["atrcollection"] = fmt.Sprintf(
				"%s.%s.%s",
				cfg.CustomATRLocation.Agent.BucketName(),
				cfg.CustomATRLocation.ScopeName,
				cfg.CustomATRLocation.CollectionName,
			)
		}

		// Need to make sure we don't end up straight back here...
		options.AsTransaction = nil
	}
	options.Timeout = cfg.ExpirationTime + cfg.KeyValueTimeout + (1 * time.Second)

	err = c.hooks.BeforeQuery(*c, statement)
	if err != nil {
		return nil, queryMaybeTranslateToTransactionsError(err, stateCtx)
	}

	var result *QueryResult
	var queryErr error
	if scope == nil {
		result, queryErr = c.cluster.Query(statement, &options)
	} else {
		result, queryErr = scope.Query(statement, &options)
	}
	if queryErr != nil {
		return nil, translateStatementError(queryErr, hookPoint, txImplicit, stateCtx)
	}

	err = c.hooks.AfterQuery(*c, statement)
	if err != nil {
		return nil, queryMaybeTranslateToTransactionsError(err, stateCtx)
	}

	return result, nil
}

func (c *TransactionAttemptContext) queryBeginWork(scope *Scope) (errOut error) {
	c.logger.logInfof(c.attemptID, "Performing query begin work")

	mutations := c.txn.GetMutations()
	for _, mutation := range mutations {
		if mutation.Expiry > 0 {
			return operationFailed(transactionOperationFailedDef{
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
				ErrorCause:        wrapError(ErrFeatureNotAvailable, "expiry is not available for queries"),
				ErrorClass:        gocbcore.TransactionErrorClassFailOther,
			}, c)
		}

		datatype, _ := gocbcore.DecodeCommonFlags(mutation.StagedUserFlags)
		if datatype == gocbcore.BinaryType {
			return operationFailed(transactionOperationFailedDef{
				ShouldNotRetry:    true,
				ShouldNotRollback: false,
				Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
				ErrorCause: wrapError(
					ErrFeatureNotAvailable,
					"binary transactions is not available for queries",
				),
				ErrorClass: gocbcore.TransactionErrorClassFailOther,
			}, c)
		}
	}

	waitCh := make(chan struct{}, 1)
	err := c.txn.SerializeAttempt(func(txdata []byte, err error) {
		if err != nil {
			c.logger.logInfof(c.attemptID, "SerializeAttempt failed, not moving into query mode")
			var coreErr *gocbcore.TransactionOperationFailedError
			if errors.As(err, &coreErr) {
				// Note that we purposely do not use operationFailed here, we haven't moved into query mode yet.
				// State will continue to be controlled from the gocbcore side.
				errOut = &TransactionOperationFailedError{
					shouldRetry:       coreErr.Retry(),
					shouldNotRollback: !coreErr.Rollback(),
					errorCause:        coreErr.InternalUnwrap(),
					shouldRaise:       coreErr.ToRaise(),
					errorClass:        coreErr.ErrorClass(),
				}
			} else {
				errOut = operationFailedFromError(err, c)
			}
			waitCh <- struct{}{}
			return
		}

		cfg := c.txn.Config()
		raw := make(map[string]interface{})
		raw["durability_level"] = durabilityLevelToQueryString(cfg.DurabilityLevel)
		raw["txtimeout"] = fmt.Sprintf("%dms", cfg.ExpirationTime.Milliseconds())
		if cfg.CustomATRLocation.Agent != nil {
			// Agent being non nil signifies that this was set.
			raw["atrcollection"] = fmt.Sprintf(
				"%s.%s.%s",
				cfg.CustomATRLocation.Agent.BucketName(),
				cfg.CustomATRLocation.ScopeName,
				cfg.CustomATRLocation.CollectionName,
			)
		}

		res, err := c.queryWrapperWrapper(scope, "BEGIN WORK", QueryOptions{
			ScanConsistency: c.queryConfig.ScanConsistency,
			Raw:             raw,
			Adhoc:           true,
		}, "queryBeginWork", true, false, txdata, true)
		if err != nil {
			errOut = queryBeginWorkFailed(err, c)
			waitCh <- struct{}{}
			return
		}

		// Only now that BEGIN WORK has succeeded do we enter query mode, storing the scope and the query target for
		// later operations.
		c.logger.logInfof(c.attemptID, "Begin work setting query target to %s", res.endpoint)
		c.queryState = &transactionQueryState{
			scope:       scope,
			queryTarget: res.endpoint,
		}

		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = operationFailedFromError(err, c)
		return
	}
	<-waitCh

	return
}

// queryBeginWorkFailed converts err, from a BEGIN WORK statement that failed, into the error that queryBeginWork
// raises.
//
// Any failure of BEGIN WORK is fatal to the transaction (transactions-query.md, queryBeginWork, Handling of failure:
// "Error(cause=err, ec=FAIL_OTHER, retry=false, rollback=true)"). Statement level atomicity does not apply, as from the
// query service's point of view the attempt has not begun until BEGIN WORK succeeds, so unlike a failed statement in
// query mode a plain error is not returned to the lambda: it would leave the attempt committable. A
// TransactionOperationFailedError has already been raised by the query error handling, which follows the spec's rules
// for the error (an expiry, for example, does not allow rollback), and is returned as it is.
//
// The attempt does not enter query mode: the rollback that follows the failure is a KV rollback, of any KV operations
// that the attempt performed before BEGIN WORK. The query service may have opened the transaction even though BEGIN
// WORK failed, for example if the statement timed out on the SDK side, but it is not sent a ROLLBACK: the attempt has
// no query target to send it to, and the query service owns, and cleans up, any transaction it has opened.
func queryBeginWorkFailed(err error, c *TransactionAttemptContext) error {
	var tErr *TransactionOperationFailedError
	if errors.As(err, &tErr) {
		return err
	}

	return operationFailed(transactionOperationFailedDef{
		ShouldNotRetry:    true,
		ShouldNotRollback: false,
		Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
		ErrorCause:        err,
		ErrorClass:        gocbcore.TransactionErrorClassFailOther,
	}, c)
}

func (c *TransactionAttemptContext) keyspace(collection *Collection) string {
	return fmt.Sprintf("default:`%s`.`%s`.`%s`", collection.Bucket().Name(), collection.ScopeName(), collection.Name())
}
