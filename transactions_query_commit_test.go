package gocb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v10"
)

// txnQueryTestProvider is a queryProvider that records the statements it is sent, and answers them with fn.
type txnQueryTestProvider struct {
	suite *UnitTestSuite

	lock       sync.Mutex
	statements []string
	fn         func(statement string) (*QueryResult, error)
}

func (p *txnQueryTestProvider) Query(statement string, _ *Scope, _ *QueryOptions) (*QueryResult, error) {
	p.lock.Lock()
	p.statements = append(p.statements, statement)
	p.lock.Unlock()

	if p.fn != nil {
		if res, err := p.fn(statement); res != nil || err != nil {
			return res, err
		}
	}

	return p.success(), nil
}

func (p *txnQueryTestProvider) sent() []string {
	p.lock.Lock()
	defer p.lock.Unlock()
	return append([]string(nil), p.statements...)
}

// success returns a result with no rows, which completes successfully.
func (p *txnQueryTestProvider) success() *QueryResult {
	return p.streamErr(nil)
}

// streamErr returns a result whose stream fails with err after the request has been accepted.
func (p *txnQueryTestProvider) streamErr(err error) *QueryResult {
	return newQueryResult(&mockQueryRowReader{
		mockQueryRowReaderBase: mockQueryRowReaderBase{
			Meta:    []byte(`{"requestID":"test","status":"success"}`),
			RowsErr: err,
			Suite:   p.suite,
		},
	})
}

func (suite *UnitTestSuite) newQueryTestTransactions(provider *txnQueryTestProvider) (*transactionsProviderCore, *Transactions) {
	cli := new(mockConnectionManager)
	cli.On("close").Return(nil)
	cli.On("getQueryProvider").Return(provider, nil)
	cli.On("getMeter").Return(nil)
	cli.On("MarkOpBeginning").Return(nil)
	cli.On("MarkOpCompleted").Return()

	tConfig := TransactionsConfig{}
	tConfig.CleanupConfig.DisableLostAttemptCleanup = true
	tConfig.CleanupConfig.DisableClientAttemptCleanup = true

	txns := &transactionsProviderCore{
		config: tConfig,
	}
	err := txns.Init(suite.newCluster(cli))
	suite.Require().NoError(err)

	transactions := &Transactions{
		controller: &providerController[transactionsProvider]{
			get: func() (transactionsProvider, error) {
				return txns, nil
			},
			opController: cli,
		},
	}

	return txns, transactions
}

// The errors with which a statement can end without a definite answer from the query service.
func queryTestAmbiguousErrors() map[string]error {
	return map[string]error{
		"ambiguous timeout": &TimeoutError{InnerError: ErrAmbiguousTimeout, OperationID: "N1QLQuery"},
		"canceled":          &QueryError{InnerError: ErrRequestCanceled, Statement: "COMMIT"},
		"unexpected EOF":    &QueryError{InnerError: io.ErrUnexpectedEOF, Statement: "COMMIT"},
		"connection reset": &QueryError{InnerError: &net.OpError{
			Op: "read", Net: "tcp", Err: errors.New("connection reset by peer"),
		}},
		"context canceled": context.Canceled,
	}
}

func queryTestCauseError(code uint32, rollback, retry bool, raise string) *QueryError {
	return &QueryError{
		// A real QueryError always has an inner error, and Error() relies on it.
		InnerError: errors.New("test query error"),
		Statement:  "COMMIT",
		Errors:     []QueryErrorDesc{{Code: code, Message: "test"}},
		ErrorText: fmt.Sprintf(`[{"code":%d,"message":"test","cause":{"rollback":%t,"retry":%t,"raise":%q}}]`,
			code, rollback, retry, raise),
	}
}

func (suite *UnitTestSuite) TestQueryOutcomeUnknown() {
	for name, err := range queryTestAmbiguousErrors() {
		suite.Assert().True(queryOutcomeUnknown(err), name)
	}

	definite := map[string]error{
		"nil":                 nil,
		"unambiguous timeout": &TimeoutError{InnerError: ErrUnambiguousTimeout},
		"server timeout":      &QueryError{InnerError: ErrUnambiguousTimeout, Errors: []QueryErrorDesc{{Code: 1080}}},
		"server error":        queryTestCauseError(17007, true, false, "failed"),
		"server error, wrapping a timeout": &QueryError{
			InnerError: ErrAmbiguousTimeout,
			Errors:     []QueryErrorDesc{{Code: 17010}},
		},
		"dial failure": &QueryError{InnerError: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")}},
		"dns failure":  &QueryError{InnerError: &net.DNSError{Err: "no such host", Name: "x"}},
		"other":        errors.New("something else"),
	}
	for name, err := range definite {
		suite.Assert().False(queryOutcomeUnknown(err), name)
	}
}

func (suite *UnitTestSuite) TestQueryCommitStatementErrorTranslation() {
	for name, err := range queryTestAmbiguousErrors() {
		translated := queryMaybeTranslateCommitStatementError(err, nil)

		var tErr *TransactionOperationFailedError
		suite.Require().ErrorAs(translated, &tErr, name)
		suite.Assert().Equal(gocbcore.TransactionErrorReasonTransactionCommitAmbiguous, tErr.shouldRaise, name)
		suite.Assert().False(tErr.Rollback(), name)
		suite.Assert().False(tErr.Retry(), name)
		suite.Assert().Equal(err, tErr.InternalUnwrap(), name)

		// Any other statement is unchanged: a timeout of it is expiry.
		if errors.Is(err, ErrTimeout) {
			other := queryMaybeTranslateToTransactionsError(err, nil)
			suite.Require().ErrorAs(other, &tErr, name)
			suite.Assert().Equal(gocbcore.TransactionErrorReasonTransactionExpired, tErr.shouldRaise, name)
		}
	}

	// A definite answer from the query service keeps its mapping.
	translated := queryMaybeTranslateCommitStatementError(queryTestCauseError(17007, true, false, "failed"), nil)
	var tErr *TransactionOperationFailedError
	suite.Require().ErrorAs(translated, &tErr)
	suite.Assert().Equal(gocbcore.TransactionErrorReasonTransactionFailed, tErr.shouldRaise)
	suite.Assert().True(tErr.Rollback())

	translated = queryMaybeTranslateCommitStatementError(&QueryError{Errors: []QueryErrorDesc{{Code: 17010}}}, nil)
	suite.Require().ErrorAs(translated, &tErr)
	suite.Assert().Equal(gocbcore.TransactionErrorReasonTransactionExpired, tErr.shouldRaise)
	suite.Assert().False(tErr.Rollback())

	// An unambiguous timeout means that the statement was not processed.
	translated = queryMaybeTranslateCommitStatementError(&TimeoutError{InnerError: ErrUnambiguousTimeout}, nil)
	suite.Require().ErrorAs(translated, &tErr)
	suite.Assert().Equal(gocbcore.TransactionErrorReasonTransactionExpired, tErr.shouldRaise)
}

func (suite *UnitTestSuite) TestSingleQueryStreamErrorTranslation() {
	for name, err := range queryTestAmbiguousErrors() {
		var ambiguousErr *TransactionCommitAmbiguousError
		suite.Assert().ErrorAs(singleQueryErrToTransactionError(err, "txn"), &ambiguousErr, name)
	}

	var expiredErr *TransactionExpiredError
	suite.Assert().ErrorAs(singleQueryErrToTransactionError(
		&QueryError{Errors: []QueryErrorDesc{{Code: 17010}}}, "txn"), &expiredErr)
}

func (suite *UnitTestSuite) runQueryModeCommit(commitErr func(p *txnQueryTestProvider) (*QueryResult, error)) ([]string, error) {
	provider := &txnQueryTestProvider{suite: suite}
	provider.fn = func(statement string) (*QueryResult, error) {
		if statement == "COMMIT" {
			return commitErr(provider)
		}
		return nil, nil
	}
	txns, _ := suite.newQueryTestTransactions(provider)
	defer txns.close()

	_, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Query("UPDATE `default` SET foo = 1", nil)
		return err
	}, txnTestOptions(30*time.Second, nil), false)

	return provider.sent(), err
}

func (suite *UnitTestSuite) TestQueryModeCommitWithoutOutcomeIsAmbiguous() {
	for name, commitErr := range queryTestAmbiguousErrors() {
		for _, inStream := range []bool{false, true} {
			sent, err := suite.runQueryModeCommit(func(p *txnQueryTestProvider) (*QueryResult, error) {
				if inStream {
					return p.streamErr(commitErr), nil
				}
				return nil, commitErr
			})

			var ambiguousErr *TransactionCommitAmbiguousError
			suite.Assert().ErrorAs(err, &ambiguousErr, "%s, in stream: %t", name, inStream)
			suite.Assert().ErrorIs(err, commitErr, "%s, in stream: %t", name, inStream)
			suite.Assert().Equal([]string{"BEGIN WORK", "UPDATE `default` SET foo = 1", "COMMIT"}, sent,
				"%s, in stream: %t: no ROLLBACK may follow the COMMIT", name, inStream)
		}
	}
}

func (suite *UnitTestSuite) TestQueryModeCommitDefiniteFailureKeepsMapping() {
	sent, err := suite.runQueryModeCommit(func(*txnQueryTestProvider) (*QueryResult, error) {
		return nil, queryTestCauseError(17007, true, false, "failed")
	})

	var failedErr *TransactionFailedError
	suite.Assert().ErrorAs(err, &failedErr)
	suite.Assert().Equal([]string{"BEGIN WORK", "UPDATE `default` SET foo = 1", "COMMIT", "ROLLBACK"}, sent)
}

// runQueryModeStatement runs a query mode transaction whose lambda sends one UPDATE statement, answered by stmtErr, and
// returns the statements sent and the error from the transaction.
func (suite *UnitTestSuite) runQueryModeStatement(stmtErr func(p *txnQueryTestProvider) (*QueryResult, error),
	hooks TransactionHooks) ([]string, error) {
	provider := &txnQueryTestProvider{suite: suite}
	provider.fn = func(statement string) (*QueryResult, error) {
		if statement == "UPDATE `default` SET foo = 1" && stmtErr != nil {
			return stmtErr(provider)
		}
		return nil, nil
	}
	txns, _ := suite.newQueryTestTransactions(provider)
	defer txns.close()

	_, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Query("UPDATE `default` SET foo = 1", nil)
		return err
	}, txnTestOptions(30*time.Second, hooks), false)

	return provider.sent(), err
}

// A statement other than COMMIT that times out on the SDK side is expired. An expired transaction must never be
// committed, so no COMMIT may follow; the timeout allows rollback, so a ROLLBACK is sent instead.
func (suite *UnitTestSuite) TestQueryModeStatementTimeoutIsStillExpired() {
	for _, inStream := range []bool{false, true} {
		sent, err := suite.runQueryModeStatement(func(p *txnQueryTestProvider) (*QueryResult, error) {
			timeoutErr := &TimeoutError{InnerError: ErrAmbiguousTimeout}
			if inStream {
				return p.streamErr(timeoutErr), nil
			}
			return nil, timeoutErr
		}, nil)

		var expiredErr *TransactionExpiredError
		suite.Assert().ErrorAs(err, &expiredErr, "in stream: %t", inStream)
		suite.Assert().NotContains(sent, "COMMIT", "in stream: %t: an expired transaction may not be committed", inStream)
		suite.Assert().Equal([]string{"BEGIN WORK", "UPDATE `default` SET foo = 1", "ROLLBACK"}, sent,
			"in stream: %t", inStream)
	}
}

// The query service's own expiry errors (1080 and 17010) on a statement other than COMMIT are expired too, and do not
// allow rollback, so neither COMMIT nor ROLLBACK may follow them.
func (suite *UnitTestSuite) TestQueryModeStatementServerExpiryDoesNotCommit() {
	expiryErrs := map[string]error{
		"1080":                 &QueryError{Statement: "UPDATE", Errors: []QueryErrorDesc{{Code: 1080}}},
		"17010":                &QueryError{Statement: "UPDATE", Errors: []QueryErrorDesc{{Code: 17010}}},
		"17010 with a cause":   queryTestCauseError(17010, true, false, "failed"),
		"cause raises expired": queryTestCauseError(17007, false, false, "expired"),
	}
	for name, expiryErr := range expiryErrs {
		for _, inStream := range []bool{false, true} {
			sent, err := suite.runQueryModeStatement(func(p *txnQueryTestProvider) (*QueryResult, error) {
				if inStream {
					return p.streamErr(expiryErr), nil
				}
				return nil, expiryErr
			}, nil)

			var expiredErr *TransactionExpiredError
			suite.Assert().ErrorAs(err, &expiredErr, "%s, in stream: %t", name, inStream)
			suite.Assert().Equal([]string{"BEGIN WORK", "UPDATE `default` SET foo = 1"}, sent,
				"%s, in stream: %t: no COMMIT or ROLLBACK may follow", name, inStream)
		}
	}
}

// A statement refused locally because the transaction has expired must not be followed by COMMIT either.
func (suite *UnitTestSuite) TestQueryModeLocalExpiryDoesNotCommit() {
	hooks := &txnTestHooks{fn: func(stage string) (bool, error) {
		return stage == "query", nil
	}}
	sent, err := suite.runQueryModeStatement(nil, hooks)

	var expiredErr *TransactionExpiredError
	suite.Assert().ErrorAs(err, &expiredErr)
	suite.Assert().Equal([]string{"BEGIN WORK"}, sent, "no COMMIT or ROLLBACK may follow")
}

// Every translation of a query error that raises EXPIRED leaves the attempt unable to commit.
func (suite *UnitTestSuite) TestQueryExpiredTranslationsPreventCommit() {
	expiryErrs := map[string]error{
		"sdk timeout":        &TimeoutError{InnerError: ErrAmbiguousTimeout},
		"1080":               &QueryError{Errors: []QueryErrorDesc{{Code: 1080}}},
		"17010":              &QueryError{Errors: []QueryErrorDesc{{Code: 17010}}},
		"17010 with a cause": queryTestCauseError(17010, true, false, "failed"),
	}
	for name, expiryErr := range expiryErrs {
		txns := suite.newTxnTestTransactions()
		_, err := txns.Run(func(ctx *TransactionAttemptContext) error {
			translated := queryMaybeTranslateToTransactionsError(expiryErr, ctx)

			var tErr *TransactionOperationFailedError
			suite.Require().ErrorAs(translated, &tErr, name)
			suite.Assert().Equal(gocbcore.TransactionErrorReasonTransactionExpired, tErr.shouldRaise, name)
			suite.Assert().False(ctx.canCommit(), "%s: an expired attempt may not be committed", name)
			return translated
		}, txnTestOptions(30*time.Second, nil), false)

		var expiredErr *TransactionExpiredError
		suite.Assert().ErrorAs(err, &expiredErr, name)
		suite.Require().NoError(txns.close())
	}
}

func (suite *UnitTestSuite) TestSingleQueryWithoutOutcomeIsAmbiguous() {
	for name, queryErr := range queryTestAmbiguousErrors() {
		for _, inStream := range []bool{false, true} {
			provider := &txnQueryTestProvider{suite: suite}
			provider.fn = func(string) (*QueryResult, error) {
				if inStream {
					return provider.streamErr(queryErr), nil
				}
				return nil, queryErr
			}
			txns, transactions := suite.newQueryTestTransactions(provider)

			_, err := transactions.singleQuery("UPDATE `default` SET foo = 1", nil, QueryOptions{
				AsTransaction: &SingleQueryTransactionOptions{},
				Timeout:       30 * time.Second,
			})

			var ambiguousErr *TransactionCommitAmbiguousError
			suite.Assert().ErrorAs(err, &ambiguousErr, "%s, in stream: %t", name, inStream)
			suite.Assert().Equal([]string{"UPDATE `default` SET foo = 1"}, provider.sent(),
				"%s, in stream: %t: the statement may not be retried", name, inStream)
			suite.Require().NoError(txns.close())
		}
	}
}

func (suite *UnitTestSuite) TestSingleQueryDefiniteExpiryIsUnambiguousTimeout() {
	provider := &txnQueryTestProvider{suite: suite}
	provider.fn = func(string) (*QueryResult, error) {
		return nil, &QueryError{Errors: []QueryErrorDesc{{Code: 17010}}}
	}
	txns, transactions := suite.newQueryTestTransactions(provider)
	defer txns.close()

	_, err := transactions.singleQuery("UPDATE `default` SET foo = 1", nil, QueryOptions{
		AsTransaction: &SingleQueryTransactionOptions{},
		Timeout:       30 * time.Second,
	})
	suite.Assert().ErrorIs(err, ErrUnambiguousTimeout)
}

// runQueryModeLambda runs a query mode transaction with the given lambda, answering the UPDATE statement with stmtErr,
// and returns the statements sent and the error from the transaction.
func (suite *UnitTestSuite) runQueryModeLambda(stmtErr error, hooks TransactionHooks,
	fn func(ctx *TransactionAttemptContext) error) ([]string, error) {
	provider := &txnQueryTestProvider{suite: suite}
	provider.fn = func(statement string) (*QueryResult, error) {
		if statement == "UPDATE `default` SET foo = 1" && stmtErr != nil {
			return nil, stmtErr
		}
		return nil, nil
	}
	txns, _ := suite.newQueryTestTransactions(provider)
	defer txns.close()

	_, err := txns.Run(fn, txnTestOptions(30*time.Second, hooks), false)

	return provider.sent(), err
}

// The query errors which gocb raises as a TransactionOperationFailedError that fails the transaction, other than those
// raising EXPIRED (see TestQueryExpiredTranslationsPreventCommit), with what the Run loop should send after them.
func queryTestFailingErrors() map[string]struct {
	err         error
	reason      gocbcore.TransactionErrorReason
	rollback    bool
	errorAsType error
} {
	return map[string]struct {
		err         error
		reason      gocbcore.TransactionErrorReason
		rollback    bool
		errorAsType error
	}{
		"1065 feature not available": {
			err:         &QueryError{Statement: "UPDATE", Errors: []QueryErrorDesc{{Code: 1065}}},
			reason:      gocbcore.TransactionErrorReasonTransactionFailed,
			rollback:    true,
			errorAsType: &TransactionFailedError{},
		},
		"cause raises failed": {
			err:         queryTestCauseError(17007, true, false, "failed"),
			reason:      gocbcore.TransactionErrorReasonTransactionFailed,
			rollback:    true,
			errorAsType: &TransactionFailedError{},
		},
		"cause raises commit_ambiguous": {
			err:         queryTestCauseError(17007, false, false, "commit_ambiguous"),
			reason:      gocbcore.TransactionErrorReasonTransactionCommitAmbiguous,
			rollback:    false,
			errorAsType: &TransactionCommitAmbiguousError{},
		},
		"cause raises failed_post_commit": {
			err:      queryTestCauseError(17007, false, false, "failed_post_commit"),
			reason:   gocbcore.TransactionErrorReasonTransactionFailedPostCommit,
			rollback: false,
		},
	}
}

// Every query error that gocb raises as a TransactionOperationFailedError leaves the attempt unable to commit, whatever
// it raises: per the spec (transactions-design-doc.md, ExtThreadSafety) every TransactionOperationFailed sets
// COMMIT_NOT_ALLOWED.
func (suite *UnitTestSuite) TestQueryFailingTranslationsPreventCommit() {
	for name, tc := range queryTestFailingErrors() {
		txns := suite.newTxnTestTransactions()
		_, _ = txns.Run(func(ctx *TransactionAttemptContext) error {
			translated := queryMaybeTranslateToTransactionsError(tc.err, ctx)

			var tErr *TransactionOperationFailedError
			suite.Require().ErrorAs(translated, &tErr, name)
			suite.Assert().Equal(tc.reason, tErr.shouldRaise, name)
			suite.Assert().False(ctx.canCommit(), "%s: a failed attempt may not be committed", name)
			return nil
		}, txnTestOptions(30*time.Second, nil), false)
		suite.Require().NoError(txns.close())
	}
}

// A statement that fails with an error which fails the transaction is never followed by COMMIT, whether the lambda
// returns the error or swallows it. If the error allows rollback, a ROLLBACK follows instead.
func (suite *UnitTestSuite) TestQueryModeFailingStatementDoesNotCommit() {
	for name, tc := range queryTestFailingErrors() {
		for _, swallow := range []bool{false, true} {
			sent, err := suite.runQueryModeLambda(tc.err, nil, func(ctx *TransactionAttemptContext) error {
				_, err := ctx.Query("UPDATE `default` SET foo = 1", nil)
				suite.Assert().Error(err, "%s, swallowed: %t", name, swallow)
				if swallow {
					return nil
				}
				return err
			})

			expected := []string{"BEGIN WORK", "UPDATE `default` SET foo = 1"}
			if tc.rollback {
				expected = append(expected, "ROLLBACK")
			}
			suite.Assert().Equal(expected, sent, "%s, swallowed: %t: no COMMIT may follow", name, swallow)
			if tc.errorAsType != nil {
				suite.Assert().IsType(tc.errorAsType, err, "%s, swallowed: %t", name, swallow)
			}
		}
	}
}

// 1065 is translated to FeatureNotAvailable, and is the cause of the TransactionFailedError.
func (suite *UnitTestSuite) TestQueryModeFeatureNotAvailableFailsTransaction() {
	for _, inStream := range []bool{false, true} {
		sent, err := suite.runQueryModeStatement(func(p *txnQueryTestProvider) (*QueryResult, error) {
			featureErr := &QueryError{Statement: "UPDATE", Errors: []QueryErrorDesc{{Code: 1065}}}
			if inStream {
				return p.streamErr(featureErr), nil
			}
			return nil, featureErr
		}, nil)

		var failedErr *TransactionFailedError
		suite.Assert().ErrorAs(err, &failedErr, "in stream: %t", inStream)
		suite.Assert().ErrorIs(err, ErrFeatureNotAvailable, "in stream: %t", inStream)
		suite.Assert().Equal([]string{"BEGIN WORK", "UPDATE `default` SET foo = 1", "ROLLBACK"}, sent,
			"in stream: %t: no COMMIT may follow", inStream)
	}
}

// A failing HasExpiredClientSideHook fails the statement, and the transaction with it.
func (suite *UnitTestSuite) TestQueryModeHookErrorDoesNotCommit() {
	for _, swallow := range []bool{false, true} {
		hooks := &txnTestHooks{fn: func(stage string) (bool, error) {
			if stage == "query" {
				return false, errTxnTest
			}
			return false, nil
		}}
		sent, err := suite.runQueryModeLambda(nil, hooks, func(ctx *TransactionAttemptContext) error {
			_, err := ctx.Query("UPDATE `default` SET foo = 1", nil)
			suite.Assert().Error(err, "swallowed: %t", swallow)
			if swallow {
				return nil
			}
			return err
		})

		var failedErr *TransactionFailedError
		suite.Assert().ErrorAs(err, &failedErr, "swallowed: %t", swallow)
		suite.Assert().Equal([]string{"BEGIN WORK", "ROLLBACK"}, sent, "swallowed: %t: no COMMIT may follow", swallow)
	}
}

// Query errors that the spec returns to the lambda as plain errors (statement level atomicity) do not fail the
// transaction: if the lambda handles them, the transaction still commits. If it returns them, the Run loop fails the
// transaction, as for any other error from the lambda.
func (suite *UnitTestSuite) TestQueryModeBenignErrorsStillCommit() {
	benignErrs := map[string]error{
		"syntax error":     &QueryError{Statement: "UPDATE", Errors: []QueryErrorDesc{{Code: 3000}}},
		"unknown keyspace": &QueryError{Statement: "UPDATE", Errors: []QueryErrorDesc{{Code: 12003}}},
		"no error codes":   &QueryError{Statement: "UPDATE", InnerError: ErrParsingFailure},
		"17012":            &QueryError{Statement: "UPDATE", Errors: []QueryErrorDesc{{Code: 17012}}},
		"17014":            &QueryError{Statement: "UPDATE", Errors: []QueryErrorDesc{{Code: 17014}}},
		"17015":            &QueryError{Statement: "UPDATE", Errors: []QueryErrorDesc{{Code: 17015}}},
	}
	for name, benignErr := range benignErrs {
		for _, swallow := range []bool{false, true} {
			sent, err := suite.runQueryModeLambda(benignErr, nil, func(ctx *TransactionAttemptContext) error {
				_, err := ctx.Query("UPDATE `default` SET foo = 1", nil)
				suite.Require().Error(err, name)

				var tErr *TransactionOperationFailedError
				suite.Assert().False(errors.As(err, &tErr), "%s: must be a plain error", name)
				suite.Assert().True(ctx.canCommit(), "%s: must leave the attempt committable", name)
				if swallow {
					return nil
				}
				return err
			})

			if swallow {
				suite.Assert().NoError(err, name)
				suite.Assert().Equal([]string{"BEGIN WORK", "UPDATE `default` SET foo = 1", "COMMIT"}, sent, name)
			} else {
				var failedErr *TransactionFailedError
				suite.Assert().ErrorAs(err, &failedErr, name)
				suite.Assert().Equal([]string{"BEGIN WORK", "UPDATE `default` SET foo = 1", "ROLLBACK"}, sent, name)
			}
		}
	}
}

type txnFailingTranscoder struct {
	JSONTranscoder
}

var errTxnTestEncode = errors.New("test encode failure")

func (t *txnFailingTranscoder) Encode(interface{}) ([]byte, uint32, error) {
	return nil, 0, errTxnTestEncode
}

// assertOperationFailedWith asserts that err is a TransactionOperationFailedError caused by cause.
func (suite *UnitTestSuite) assertOperationFailedWith(err, cause error, msgAndArgs ...interface{}) {
	var tErr *TransactionOperationFailedError
	if suite.Assert().ErrorAs(err, &tErr, msgAndArgs...) {
		suite.Assert().ErrorIs(tErr.InternalUnwrap(), cause, msgAndArgs...)
		suite.Assert().Equal(gocbcore.TransactionErrorReasonTransactionFailed, tErr.shouldRaise, msgAndArgs...)
	}
}

// A query mode insert whose value cannot be encoded fails the transaction, so no COMMIT follows.
func (suite *UnitTestSuite) TestQueryModeEncodeFailureDoesNotCommit() {
	collection := newBucket("default", nil, nil).DefaultCollection()
	for _, swallow := range []bool{false, true} {
		sent, err := suite.runQueryModeLambda(nil, nil, func(ctx *TransactionAttemptContext) error {
			_, err := ctx.Query("UPDATE `default` SET foo = 1", nil)
			suite.Require().NoError(err)

			_, err = ctx.InsertWithOptions(collection, "id", "value", &TransactionInsertOptions{
				Transcoder: &txnFailingTranscoder{},
			})
			suite.assertOperationFailedWith(err, errTxnTestEncode, "swallowed: %t", swallow)
			if swallow {
				return nil
			}
			return err
		})

		var failedErr *TransactionFailedError
		suite.Assert().ErrorAs(err, &failedErr, "swallowed: %t", swallow)
		if !swallow {
			suite.Assert().ErrorIs(err, errTxnTestEncode)
		}
		suite.Assert().Equal([]string{"BEGIN WORK", "UPDATE `default` SET foo = 1", "ROLLBACK"}, sent,
			"swallowed: %t: no COMMIT may follow", swallow)
	}
}

// The same applies in KV mode: gocb, not gocbcore, raises the error when a value cannot be encoded, and it must fail
// the transaction rather than let it commit.
func (suite *UnitTestSuite) TestKVModeEncodeFailureDoesNotCommit() {
	collection := newBucket("default", nil, nil).DefaultCollection()
	ops := map[string]func(ctx *TransactionAttemptContext) error{
		"insert": func(ctx *TransactionAttemptContext) error {
			_, err := ctx.InsertWithOptions(collection, "id", "value", &TransactionInsertOptions{
				Transcoder: &txnFailingTranscoder{},
			})
			return err
		},
		"replace": func(ctx *TransactionAttemptContext) error {
			_, err := ctx.ReplaceWithOptions(&TransactionGetResult{collection: collection, docID: "id"}, "value",
				&TransactionReplaceOptions{Transcoder: &txnFailingTranscoder{}})
			return err
		},
	}
	for name, op := range ops {
		for _, swallow := range []bool{false, true} {
			txns := suite.newTxnTestTransactions()
			_, err := txns.Run(func(ctx *TransactionAttemptContext) error {
				err := op(ctx)
				suite.assertOperationFailedWith(err, errTxnTestEncode, "%s, swallowed: %t", name, swallow)
				suite.Assert().False(ctx.canCommit(), "%s, swallowed: %t: a failed attempt may not be committed",
					name, swallow)
				if swallow {
					return nil
				}
				return err
			}, txnTestOptions(30*time.Second, nil), false)

			var failedErr *TransactionFailedError
			suite.Assert().ErrorAs(err, &failedErr, "%s, swallowed: %t", name, swallow)
			if !swallow {
				suite.Assert().ErrorIs(err, errTxnTestEncode, name)
			}
			suite.Require().NoError(txns.close())
		}
	}
}
