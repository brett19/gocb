package gocb

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	cavescli "github.com/couchbaselabs/gocaves/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

const (
	defaultServerVersion = "5.1.0"
)

var globalBucket *Bucket
var globalCollection *Collection
var globalScope *Scope
var globalCluster *testCluster

type IntegrationTestSuite struct {
	suite.Suite
}

func (suite *IntegrationTestSuite) SetupSuite() {
	var err error
	var connStr string
	var mock *cavescli.Client
	var mockID string
	var auth PasswordAuthenticator
	if globalConfig.Server == "" {
		if globalConfig.Version != "" {
			panic("version cannot be specified with mock")
		}

		mock, err = cavescli.NewClient(cavescli.NewClientOptions{
			Path: "/Users/brettlawson/couchsdk/gocaves/main.go",
		})
		if err != nil {
			panic(err.Error())
		}

		mockID = uuid.New().String()
		connStr, err = mock.CreateCluster(mockID)
		if err != nil {
			panic(err.Error())
		}

		globalConfig.Bucket = "default"
		globalConfig.Version = "1.5.6"
		globalConfig.Server = connStr
		auth = PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		}
	} else {
		connStr = globalConfig.Server

		auth = PasswordAuthenticator{
			Username: globalConfig.User,
			Password: globalConfig.Password,
		}

		if globalConfig.Version == "" {
			globalConfig.Version = defaultServerVersion
		}
	}

	cluster, err := Connect(connStr, ClusterOptions{Authenticator: auth})
	if err != nil {
		panic(err.Error())
	}

	nodeVersion, err := newNodeVersion(globalConfig.Version, mock != nil)
	if err != nil {
		panic(err.Error())
	}

	globalCluster = &testCluster{
		Cluster:      cluster,
		Mock:         mock,
		MockID:       mockID,
		Version:      nodeVersion,
		FeatureFlags: globalConfig.FeatureFlags,
	}

	globalBucket = globalCluster.Bucket(globalConfig.Bucket)

	if globalConfig.Scope != "" {
		globalScope = globalBucket.Scope(globalConfig.Scope)
	} else {
		globalScope = globalBucket.DefaultScope()
	}

	if globalConfig.Collection != "" {
		globalCollection = globalScope.Collection(globalConfig.Collection)
	} else {
		globalCollection = globalScope.Collection("_default")
	}
}

func (suite *IntegrationTestSuite) TearDownSuite() {
	err := globalCluster.Close(nil)
	suite.Require().Nil(err, err)

	if globalCluster.Mock != nil {
		err = globalCluster.Mock.Shutdown()
		suite.Require().Nil(err, err)
	}

}

func (suite *IntegrationTestSuite) createBreweryDataset(datasetName, service, scope, collection string) (int, error) {
	var dataset []testBreweryDocument
	err := loadJSONTestDataset(datasetName, &dataset)
	if err != nil {
		return 0, err
	}

	if scope == "" {
		scope = "_default"
	}
	if collection == "" {
		collection = "_default"
	}

	scp := globalBucket.Scope(scope)
	col := scp.Collection(collection)

	for i, doc := range dataset {
		doc.Service = service

		_, err := col.Upsert(fmt.Sprintf("%s%d", service, i), doc, nil)
		if err != nil {
			return 0, err
		}
	}

	return len(dataset), nil
}

func (suite *IntegrationTestSuite) tryUntil(deadline time.Time, interval time.Duration, fn func() bool) bool {
	for {
		success := fn()
		if success {
			return true
		}

		sleepDeadline := time.Now().Add(interval)
		if sleepDeadline.After(deadline) {
			return false
		}
		time.Sleep(sleepDeadline.Sub(time.Now()))
	}
}

func (suite *IntegrationTestSuite) skipIfUnsupported(code FeatureCode) {
	if globalCluster.NotSupportsFeature(code) {
		suite.T().Skipf("Skipping test because feature %s unsupported or disabled", code)
	}
}

type UnitTestSuite struct {
	suite.Suite
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		return
	}

	suite.Run(t, new(IntegrationTestSuite))
}

func TestUnit(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}

func (suite *UnitTestSuite) defaultTimeoutConfig() TimeoutsConfig {
	return TimeoutsConfig{
		KVTimeout:         1000 * time.Second,
		KVDurableTimeout:  1000 * time.Second,
		AnalyticsTimeout:  1000 * time.Second,
		QueryTimeout:      1000 * time.Second,
		SearchTimeout:     1000 * time.Second,
		ManagementTimeout: 1000 * time.Second,
		ViewTimeout:       1000 * time.Second,
	}
}

func (suite *UnitTestSuite) bucket(name string, timeouts TimeoutsConfig, cli *mockConnectionManager) *Bucket {
	b := &Bucket{
		bucketName: name,
		timeoutsConfig: TimeoutsConfig{
			KVTimeout:         timeouts.KVTimeout,
			KVDurableTimeout:  timeouts.KVDurableTimeout,
			AnalyticsTimeout:  timeouts.AnalyticsTimeout,
			QueryTimeout:      timeouts.QueryTimeout,
			SearchTimeout:     timeouts.SearchTimeout,
			ManagementTimeout: timeouts.ManagementTimeout,
			ViewTimeout:       timeouts.ViewTimeout,
		},
		transcoder:           NewJSONTranscoder(),
		retryStrategyWrapper: newRetryStrategyWrapper(NewBestEffortRetryStrategy(nil)),
		tracer:               &noopTracer{},
		useServerDurations:   true,
		useMutationTokens:    true,

		connectionManager: cli,
	}

	return b
}

func (suite *UnitTestSuite) newCluster(cli connectionManager) *Cluster {
	cluster := clusterFromOptions(ClusterOptions{
		Tracer: &noopTracer{},
	})
	cluster.connectionManager = cli

	return cluster
}

func (suite *UnitTestSuite) newScope(b *Bucket, name string) *Scope {
	return newScope(b, name)
}

func (suite *UnitTestSuite) mustConvertToBytes(val interface{}) []byte {
	b, err := json.Marshal(val)
	suite.Require().Nil(err)

	return b
}

func (suite *UnitTestSuite) kvProvider(provider kvProvider, err error) func() (kvProvider, error) {
	return func() (kvProvider, error) {
		return provider, err
	}
}
