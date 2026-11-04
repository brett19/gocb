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

func (suite *UnitTestSuite) TestQueryModeStatementTimeoutIsStillExpired() {
	provider := &txnQueryTestProvider{suite: suite}
	provider.fn = func(statement string) (*QueryResult, error) {
		if statement == "UPDATE `default` SET foo = 1" {
			return nil, &TimeoutError{InnerError: ErrAmbiguousTimeout}
		}
		return nil, nil
	}
	txns, _ := suite.newQueryTestTransactions(provider)
	defer txns.close()

	_, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		_, err := ctx.Query("UPDATE `default` SET foo = 1", nil)
		return err
	}, txnTestOptions(30*time.Second, nil), false)

	var expiredErr *TransactionExpiredError
	suite.Assert().ErrorAs(err, &expiredErr)
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
