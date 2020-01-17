package gocb

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbaselabs/gocbconnstr"
	"github.com/pkg/errors"
)

// Cluster represents a connection to a specific Couchbase cluster.
type Cluster struct {
	cSpec gocbconnstr.ConnSpec
	auth  Authenticator

	connectionsLock sync.RWMutex
	connections     map[string]client
	clusterClient   client

	clusterLock sync.RWMutex
	queryCache  map[string]*queryCacheEntry

	sb stateBlock

	supportsEnhancedStatements int32

	supportsGCCCP bool
}

// ClusterOptions is the set of options available for creating a Cluster.
type ClusterOptions struct {
	Authenticator Authenticator

	ConnectTimeout    time.Duration
	KVTimeout         time.Duration
	ViewTimeout       time.Duration
	QueryTimeout      time.Duration
	AnalyticsTimeout  time.Duration
	SearchTimeout     time.Duration
	ManagementTimeout time.Duration

	// Transcoder is used for trancoding data used in KV operations.
	Transcoder Transcoder

	DisableMutationTokens bool

	RetryStrategy RetryStrategy

	// Orphan logging records when the SDK receives responses for requests that are no longer in the system (usually
	// due to being timed out).
	OrphanLoggerDisabled   bool
	OrphanLoggerInterval   time.Duration
	OrphanLoggerSampleSize int

	ThresholdLoggerDisabled bool
	ThresholdLoggingOptions *ThresholdLoggingOptions

	CircuitBreakerConfig CircuitBreakerConfig
}

// ClusterCloseOptions is the set of options available when disconnecting from a Cluster.
type ClusterCloseOptions struct {
}

// Connect creates and returns a Cluster instance created using the provided options and connection string.
// The connection string properties are copied from (and should stay in sync with) the gocbcore agent.FromConnStr comment.
// Supported connSpecStr options are:
//   cacertpath (string) - Path to the CA certificate
//   certpath (string) - Path to your authentication certificate
//   keypath (string) - Path to your authentication key
//   config_total_timeout (int) - Maximum period to attempt to connect to cluster in ms.
//   config_node_timeout (int) - Maximum period to attempt to connect to a node in ms.
//   http_redial_period (int) - Maximum period to keep HTTP config connections open in ms.
//   http_retry_delay (int) - Period to wait between retrying nodes for HTTP config in ms.
//   config_poll_floor_interval (int) - Minimum time to wait between fetching configs via CCCP in ms.
//   config_poll_interval (int) - Period to wait between CCCP config polling in ms.
//   kv_pool_size (int) - The number of connections to establish per node.
//   max_queue_size (int) - The maximum size of the operation queues per node.
//   use_kverrmaps (bool) - Whether to enable error maps from the server.
//   use_enhanced_errors (bool) - Whether to enable enhanced error information.
//   fetch_mutation_tokens (bool) - Whether to fetch mutation tokens for operations.
//   compression (bool) - Whether to enable network-wise compression of documents.
//   compression_min_size (int) - The minimal size of the document to consider compression.
//   compression_min_ratio (float64) - The minimal compress ratio (compressed / original) for the document to be sent compressed.
//   server_duration (bool) - Whether to enable fetching server operation durations.
//   http_max_idle_conns (int) - Maximum number of idle http connections in the pool.
//   http_max_idle_conns_per_host (int) - Maximum number of idle http connections in the pool per host.
//   http_idle_conn_timeout (int) - Maximum length of time for an idle connection to stay in the pool in ms.
//   network (string) - The network type to use.
//   orphaned_response_logging (bool) - Whether to enable orphan response logging.
//   orphaned_response_logging_interval (int) - How often to log orphan responses in ms.
//   orphaned_response_logging_sample_size (int) - The number of samples to include in each orphaned response log.
func Connect(connStr string, opts ClusterOptions) (*Cluster, error) {
	connSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return nil, err
	}

	connectTimeout := 10000 * time.Millisecond
	kvTimeout := 2500 * time.Millisecond
	viewTimeout := 75000 * time.Millisecond
	queryTimeout := 75000 * time.Millisecond
	analyticsTimeout := 75000 * time.Millisecond
	searchTimeout := 75000 * time.Millisecond
	managementTimeout := 75000 * time.Millisecond
	if opts.ConnectTimeout > 0 {
		connectTimeout = opts.ConnectTimeout
	}
	if opts.KVTimeout > 0 {
		kvTimeout = opts.KVTimeout
	}
	if opts.ViewTimeout > 0 {
		viewTimeout = opts.ViewTimeout
	}
	if opts.QueryTimeout > 0 {
		queryTimeout = opts.QueryTimeout
	}
	if opts.AnalyticsTimeout > 0 {
		analyticsTimeout = opts.AnalyticsTimeout
	}
	if opts.SearchTimeout > 0 {
		searchTimeout = opts.SearchTimeout
	}
	if opts.ManagementTimeout > 0 {
		managementTimeout = opts.SearchTimeout
	}
	if opts.Transcoder == nil {
		opts.Transcoder = NewJSONTranscoder()
	}
	if opts.RetryStrategy == nil {
		opts.RetryStrategy = NewBestEffortRetryStrategy(nil)
	}

	useServerDurations := true
	var initialTracer requestTracer
	if opts.ThresholdLoggerDisabled {
		initialTracer = &noopTracer{}
	} else {
		// When we expose tracing we will need to setup a composite tracer here in the user also has
		// a tracer set.
		initialTracer = newThresholdLoggingTracer(opts.ThresholdLoggingOptions)
		if opts.ThresholdLoggingOptions != nil && opts.ThresholdLoggingOptions.ServerDurationDisabled {
			useServerDurations = false
		}
	}
	tracerAddRef(initialTracer)

	cluster := &Cluster{
		cSpec:       connSpec,
		auth:        opts.Authenticator,
		connections: make(map[string]client),
		sb: stateBlock{
			ConnectTimeout:         connectTimeout,
			QueryTimeout:           queryTimeout,
			AnalyticsTimeout:       analyticsTimeout,
			SearchTimeout:          searchTimeout,
			ViewTimeout:            viewTimeout,
			KvTimeout:              kvTimeout,
			DuraTimeout:            40000 * time.Millisecond,
			DuraPollTimeout:        100 * time.Millisecond,
			Transcoder:             opts.Transcoder,
			UseMutationTokens:      !opts.DisableMutationTokens,
			ManagementTimeout:      managementTimeout,
			RetryStrategyWrapper:   newRetryStrategyWrapper(opts.RetryStrategy),
			OrphanLoggerEnabled:    !opts.OrphanLoggerDisabled,
			OrphanLoggerInterval:   opts.OrphanLoggerInterval,
			OrphanLoggerSampleSize: opts.OrphanLoggerSampleSize,
			UseServerDurations:     useServerDurations,
			Tracer:                 initialTracer,
			CircuitBreakerConfig:   opts.CircuitBreakerConfig,
		},

		queryCache: make(map[string]*queryCacheEntry),
	}

	err = cluster.parseExtraConnStrOptions(connSpec)
	if err != nil {
		return nil, err
	}

	csb := &clientStateBlock{
		BucketName: "",
	}
	cli := newClient(cluster, csb)
	err = cli.buildConfig()
	if err != nil {
		return nil, err
	}

	err = cli.connect()
	if err != nil {
		return nil, err
	}
	cluster.clusterClient = cli
	cluster.supportsGCCCP = cli.supportsGCCCP()

	return cluster, nil
}

func (c *Cluster) parseExtraConnStrOptions(spec gocbconnstr.ConnSpec) error {
	fetchOption := func(name string) (string, bool) {
		optValue := spec.Options[name]
		if len(optValue) == 0 {
			return "", false
		}
		return optValue[len(optValue)-1], true
	}

	if valStr, ok := fetchOption("n1ql_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("n1ql_timeout option must be a number")
		}
		c.sb.QueryTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("analytics_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("analytics_timeout option must be a number")
		}
		c.sb.AnalyticsTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("search_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("search_timeout option must be a number")
		}
		c.sb.SearchTimeout = time.Duration(val) * time.Millisecond
	}

	if valStr, ok := fetchOption("view_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("view_timeout option must be a number")
		}
		c.sb.ViewTimeout = time.Duration(val) * time.Millisecond
	}

	return nil
}

// Bucket connects the cluster to server(s) and returns a new Bucket instance.
func (c *Cluster) Bucket(bucketName string, opts *bucketOptions) *Bucket {
	if opts == nil {
		opts = &bucketOptions{}
	}
	b := newBucket(&c.sb, bucketName, *opts)
	cli := c.takeClusterClient()
	if cli == nil {
		// We've already taken the cluster client for a different bucket or something like that so
		// we need to connect a new client.
		cli = c.getClient(&b.sb.clientStateBlock)
		err := cli.buildConfig()
		if err == nil {
			err = cli.connect()
			if err != nil {
				cli.setBootstrapError(err)
			}
		} else {
			cli.setBootstrapError(err)
		}
	} else {
		err := cli.selectBucket(bucketName)
		if err != nil {
			cli.setBootstrapError(err)
		}
	}
	c.connectionsLock.Lock()
	c.connections[b.hash()] = cli
	c.connectionsLock.Unlock()
	b.cacheClient(cli)

	return b
}

func (c *Cluster) takeClusterClient() client {
	c.connectionsLock.Lock()
	defer c.connectionsLock.Unlock()

	if c.clusterClient != nil {
		cli := c.clusterClient
		c.clusterClient = nil
		return cli
	}

	return nil
}

func (c *Cluster) getClient(sb *clientStateBlock) client {
	c.connectionsLock.Lock()

	hash := sb.Hash()
	if cli, ok := c.connections[hash]; ok {
		c.connectionsLock.Unlock()
		return cli
	}
	c.connectionsLock.Unlock()

	cli := newClient(c, sb)

	return cli
}

func (c *Cluster) randomClient() (client, error) {
	c.connectionsLock.RLock()
	if len(c.connections) == 0 {
		c.connectionsLock.RUnlock()
		return nil, errors.New("not connected to cluster")
	}
	var randomClient client
	var firstError error
	for _, c := range c.connections { // This is ugly
		if c.connected() {
			randomClient = c
			break
		} else if firstError == nil {
			firstError = c.getBootstrapError()
		}
	}
	c.connectionsLock.RUnlock()
	if randomClient == nil {
		if firstError == nil {
			return nil, errors.New("not connected to cluster")
		}

		return nil, firstError
	}

	return randomClient, nil
}

func (c *Cluster) authenticator() Authenticator {
	return c.auth
}

func (c *Cluster) connSpec() gocbconnstr.ConnSpec {
	return c.cSpec
}

// Close shuts down all buckets in this cluster and invalidates any references this cluster has.
func (c *Cluster) Close(opts *ClusterCloseOptions) error {
	var overallErr error

	c.clusterLock.Lock()
	for key, conn := range c.connections {
		err := conn.close()
		if err != nil {
			logWarnf("Failed to close a client in cluster close: %s", err)
			overallErr = err
		}

		delete(c.connections, key)
	}
	if c.clusterClient != nil {
		err := c.clusterClient.close()
		if err != nil {
			logWarnf("Failed to close cluster client in cluster close: %s", err)
			overallErr = err
		}
	}
	c.clusterLock.Unlock()

	if c.sb.Tracer != nil {
		tracerDecRef(c.sb.Tracer)
		c.sb.Tracer = nil
	}

	return overallErr
}

func (c *Cluster) clusterOrRandomClient() (client, error) {
	var cli client
	c.connectionsLock.RLock()
	if c.clusterClient == nil {
		c.connectionsLock.RUnlock()
		var err error
		cli, err = c.randomClient()
		if err != nil {
			return nil, err
		}
	} else {
		cli = c.clusterClient
		c.connectionsLock.RUnlock()
		if !cli.supportsGCCCP() {
			return nil, errors.New("cluster-level operations not supported due to cluster version")
		}
	}

	return cli, nil
}

func (c *Cluster) getDiagnosticsProvider() (diagnosticsProvider, error) {
	cli, err := c.clusterOrRandomClient()
	if err != nil {
		return nil, err
	}

	provider, err := cli.getDiagnosticsProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) getQueryProvider() (queryProvider, error) {
	cli, err := c.clusterOrRandomClient()
	if err != nil {
		return nil, err
	}

	provider, err := cli.getQueryProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) getAnalyticsProvider() (analyticsProvider, error) {
	cli, err := c.clusterOrRandomClient()
	if err != nil {
		return nil, err
	}

	provider, err := cli.getAnalyticsProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) getSearchProvider() (searchProvider, error) {
	cli, err := c.clusterOrRandomClient()
	if err != nil {
		return nil, err
	}

	provider, err := cli.getSearchProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) getHTTPProvider() (httpProvider, error) {
	cli, err := c.clusterOrRandomClient()
	if err != nil {
		return nil, err
	}

	provider, err := cli.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}

func (c *Cluster) supportsEnhancedPreparedStatements() bool {
	return atomic.LoadInt32(&c.supportsEnhancedStatements) > 0
}

func (c *Cluster) setSupportsEnhancedPreparedStatements(supports bool) {
	if supports {
		atomic.StoreInt32(&c.supportsEnhancedStatements, 1)
	} else {
		atomic.StoreInt32(&c.supportsEnhancedStatements, 0)
	}
}

// Users returns a UserManager for managing users.
// Volatile: This API is subject to change at any time.
func (c *Cluster) Users() (*UserManager, error) {
	provider, err := c.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return &UserManager{
		httpClient:           provider,
		globalTimeout:        c.sb.ManagementTimeout,
		defaultRetryStrategy: c.sb.RetryStrategyWrapper,
		tracer:               c.sb.Tracer,
	}, nil
}

// Buckets returns a BucketManager for managing buckets.
// Volatile: This API is subject to change at any time.
func (c *Cluster) Buckets() (*BucketManager, error) {
	provider, err := c.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return &BucketManager{
		httpClient:           provider,
		globalTimeout:        c.sb.ManagementTimeout,
		defaultRetryStrategy: c.sb.RetryStrategyWrapper,
		tracer:               c.sb.Tracer,
	}, nil
}

// AnalyticsIndexes returns an AnalyticsIndexManager for managing analytics indexes.
// Volatile: This API is subject to change at any time.
func (c *Cluster) AnalyticsIndexes() (*AnalyticsIndexManager, error) {
	return &AnalyticsIndexManager{
		cluster: c,
		tracer:  c.sb.Tracer,
	}, nil
}

// QueryIndexes returns a QueryIndexManager for managing N1QL indexes.
// Volatile: This API is subject to change at any time.
func (c *Cluster) QueryIndexes() (*QueryIndexManager, error) {
	return &QueryIndexManager{
		cluster: c,
		tracer:  c.sb.Tracer,
	}, nil
}

// SearchIndexes returns a SearchIndexManager for managing Search indexes.
// Volatile: This API is subject to change at any time.
func (c *Cluster) SearchIndexes() (*SearchIndexManager, error) {
	return &SearchIndexManager{
		cluster: c,
		tracer:  c.sb.Tracer,
	}, nil
}
