package gocb

import (
	"errors"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v10"
)

// txnTestHooks lets a test observe and control the HasExpiredClientSideHook.
type txnTestHooks struct {
	transactionsDefaultHooks

	lock   sync.Mutex
	stages []string
	fn     func(stage string) (bool, error)
}

func (h *txnTestHooks) HasExpiredClientSideHook(_ TransactionAttemptContext, stage string, _ string) (bool, error) {
	h.lock.Lock()
	h.stages = append(h.stages, stage)
	h.lock.Unlock()

	if h.fn == nil {
		return false, nil
	}
	return h.fn(stage)
}

func (h *txnTestHooks) sawStage(stage string) bool {
	h.lock.Lock()
	defer h.lock.Unlock()
	for _, s := range h.stages {
		if s == stage {
			return true
		}
	}
	return false
}

func (suite *UnitTestSuite) newTxnTestTransactions() *transactionsProviderCore {
	cli := new(mockConnectionManager)
	cli.On("close").Return(nil)

	tConfig := TransactionsConfig{}
	tConfig.CleanupConfig.DisableLostAttemptCleanup = true
	tConfig.CleanupConfig.DisableClientAttemptCleanup = true

	txns := &transactionsProviderCore{
		config: tConfig,
	}
	err := txns.Init(&Cluster{connectionManager: cli})
	suite.Require().NoError(err)

	return txns
}

func txnTestOptions(timeout time.Duration, hooks TransactionHooks) *TransactionOptions {
	opts := &TransactionOptions{
		DurabilityLevel: DurabilityLevelMajority,
		Timeout:         timeout,
	}
	opts.Internal.Hooks = hooks
	return opts
}

var errTxnTest = errors.New("transaction test error")

// retryableFailure returns an error from the lambda which asks for the attempt to be rolled back and retried.
func retryableFailure(ctx *TransactionAttemptContext) error {
	return operationFailed(transactionOperationFailedDef{
		ShouldNotRetry:    false,
		ShouldNotRollback: false,
		ShouldNotCommit:   true,
		Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
		ErrorCause:        errTxnTest,
	}, ctx)
}

func (suite *UnitTestSuite) TestTransactionsRetriesRetryableErrorBeforeExpiry() {
	txns := suite.newTxnTestTransactions()
	defer txns.close()

	hooks := &txnTestHooks{}
	attempts := 0
	res, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		attempts++
		if attempts == 1 {
			return retryableFailure(ctx)
		}
		return nil
	}, txnTestOptions(30*time.Second, hooks), false)
	suite.Require().NoError(err)
	suite.Require().NotNil(res)
	suite.Assert().Equal(2, attempts)
	suite.Assert().True(hooks.sawStage(transactionsHookBeforeRetry))
}

func (suite *UnitTestSuite) TestTransactionsNoRetryOnceExpired() {
	txns := suite.newTxnTestTransactions()
	defer txns.close()

	timeout := 50 * time.Millisecond
	attempts := 0
	_, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		attempts++
		time.Sleep(timeout + 20*time.Millisecond)
		return retryableFailure(ctx)
	}, txnTestOptions(timeout, nil), false)

	var expiredErr *TransactionExpiredError
	suite.Require().ErrorAs(err, &expiredErr)
	suite.Assert().ErrorIs(err, ErrAttemptExpired)
	suite.Assert().Contains(err.Error(), errTxnTest.Error())
	suite.Assert().Equal(1, attempts)
}

func (suite *UnitTestSuite) TestTransactionsNoRetryWhenHookExpiresBeforeRetry() {
	txns := suite.newTxnTestTransactions()
	defer txns.close()

	hooks := &txnTestHooks{
		fn: func(stage string) (bool, error) {
			return stage == transactionsHookBeforeRetry, nil
		},
	}
	attempts := 0
	_, err := txns.Run(func(ctx *TransactionAttemptContext) error {
		attempts++
		return retryableFailure(ctx)
	}, txnTestOptions(30*time.Second, hooks), false)

	var expiredErr *TransactionExpiredError
	suite.Require().ErrorAs(err, &expiredErr)
	suite.Assert().Equal(1, attempts)
	suite.Assert().True(hooks.sawStage(transactionsHookBeforeRetry))
}
