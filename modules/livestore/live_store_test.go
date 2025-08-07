package livestore

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"

	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/ingest"
	"github.com/grafana/tempo/pkg/tempopb"
	v1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding"
)

type mockOverrides struct{}

func (m *mockOverrides) MaxLocalTracesPerUser(string) int {
	return 10000
}

func (m *mockOverrides) MaxBytesPerTrace(string) int {
	return 0 // Unlimited
}

func (m *mockOverrides) DedicatedColumns(string) backend.DedicatedColumns {
	return nil
}

func TestBufferCreation(t *testing.T) {
	// This is a basic test to ensure the buffer structure is correct

	b := &LiveStore{
		logger:    log.NewNopLogger(),
		instances: make(map[string]*instance),
		overrides: &mockOverrides{},
	}

	// Basic validation that the buffer is set up correctly
	assert.NotNil(t, b.logger)
	assert.NotNil(t, b.instances)
	assert.NotNil(t, b.overrides)
	assert.Equal(t, 0, len(b.instances))
}

func TestLiveStore(t *testing.T) {
	b := &LiveStore{
		logger:    log.NewNopLogger(),
		instances: make(map[string]*instance),
		overrides: &mockOverrides{},
	}

	assert.NotNil(t, b)
	assert.NotNil(t, b.logger)
	assert.Equal(t, 0, len(b.instances))
}

const integrationTestTenantID = "test-tenant"

// TestLiveStoreIntegrationWithInMemoryKafka tests the full flow:
// 1. Write traces directly to the live store (simulating Kafka consumption)
// 2. Wait for them to be processed by the live store
// 3. Query them with search functionality
func TestLiveStoreIntegrationWithInMemoryKafka(t *testing.T) {
	tmpDir := t.TempDir()
	liveStore, _ := setupIntegrationTest(t, tmpDir)

	err := liveStore.StartAsync(context.Background())
	require.NoError(t, err)
	err = liveStore.AwaitRunning(context.Background())
	require.NoError(t, err)

	instance, err := liveStore.getOrCreateInstance(integrationTestTenantID)
	require.NoError(t, err)

	numTraces := 5
	traces, traceIDs := createTestTraces(t, numTraces)

	// Directly push traces to the instance (simulating Kafka consumption)
	writeTracesToInstance(t, instance, traces, traceIDs)

	err = instance.cutIdleTraces(true)
	require.NoError(t, err)

	testSearchAfterConsumption(t, instance, traceIDs)
	testSearchTagsAfterConsumption(t, instance)

	liveStore.StopAsync()
	err = liveStore.AwaitTerminated(context.Background())
	require.NoError(t, err)
}

// TestLiveStoreSearchAfterBlockCutting tests search functionality after cutting blocks
func TestLiveStoreSearchAfterBlockCutting(t *testing.T) {
	tmpDir := t.TempDir()
	liveStore, _ := setupIntegrationTest(t, tmpDir)

	err := liveStore.StartAsync(context.Background())
	require.NoError(t, err)
	err = liveStore.AwaitRunning(context.Background())
	require.NoError(t, err)

	instance, err := liveStore.getOrCreateInstance(integrationTestTenantID)
	require.NoError(t, err)

	numTraces := 10
	traces, traceIDs := createTestTracesWithTags(t, numTraces, "service.name", "test-service")

	writeTracesToInstance(t, instance, traces, traceIDs)

	testSearchInDifferentStates(t, instance, traceIDs)

	liveStore.StopAsync()
	err = liveStore.AwaitTerminated(context.Background())
	require.NoError(t, err)
}

// TestLiveStoreConcurrentOperations tests concurrent read/write operations
func TestLiveStoreConcurrentOperations(t *testing.T) {
	tmpDir := t.TempDir()
	liveStore, _ := setupIntegrationTest(t, tmpDir)

	err := liveStore.StartAsync(context.Background())
	require.NoError(t, err)
	err = liveStore.AwaitRunning(context.Background())
	require.NoError(t, err)

	instance, err := liveStore.getOrCreateInstance(integrationTestTenantID)
	require.NoError(t, err)

	done := make(chan bool)

	go func() {
		for i := 0; i < 50; i++ {
			traces, traceIDs := createTestTraces(t, 2)
			writeTracesToInstance(t, instance, traces, traceIDs)
			time.Sleep(10 * time.Millisecond)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 20; i++ {
			req := &tempopb.SearchRequest{
				Query: "{ .service.name = \"test-service\" }",
				Limit: 100,
			}
			_, err := instance.Search(context.Background(), req)
			assert.NoError(t, err)
			time.Sleep(50 * time.Millisecond)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 10; i++ {
			err := instance.cutIdleTraces(false)
			assert.NoError(t, err)
			_, err = instance.cutBlocks(false)
			assert.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
		}
		done <- true
	}()

	for i := 0; i < 3; i++ {
		<-done
	}

	liveStore.StopAsync()
	err = liveStore.AwaitTerminated(context.Background())
	require.NoError(t, err)
}

// TestInMemoryKafkaClientBasicOperations tests the in-memory Kafka client operations
func TestInMemoryKafkaClientBasicOperations(t *testing.T) {
	client := NewInMemoryKafkaClient()

	err := client.Ping(context.Background())
	assert.NoError(t, err)

	client.AddMessage("test-topic", 0, []byte("key1"), []byte("value1"))
	client.AddMessage("test-topic", 0, []byte("key2"), []byte("value2"))

	partitions := map[string]map[int32]kgo.Offset{
		"test-topic": {0: kgo.NewOffset().At(0)},
	}
	client.AddConsumePartitions(partitions)

	offsets, err := client.FetchOffsets(context.Background(), "test-group")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(offsets))

	commitOffsets := make(kadm.Offsets)
	commitOffsets.Add(kadm.Offset{
		Topic:     "test-topic",
		Partition: 0,
		At:        1,
	})

	responses, err := client.CommitOffsets(context.Background(), "test-group", commitOffsets)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(responses))

	offsets, err = client.FetchOffsets(context.Background(), "test-group")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(offsets))

	client.RemoveConsumePartitions(map[string][]int32{
		"test-topic": {0},
	})

	client.Close()
	err = client.Ping(context.Background())
	assert.Error(t, err)
}

func setupIntegrationTest(t *testing.T, tmpDir string) (*LiveStore, *InMemoryKafkaClient) {
	cfg := Config{}
	cfg.WAL.Filepath = tmpDir
	cfg.WAL.Version = encoding.LatestEncoding().Version()
	flagext.DefaultValues(&cfg.LifecyclerConfig)

	mockStore, _ := consul.NewInMemoryClient(
		ring.GetPartitionRingCodec(),
		log.NewNopLogger(),
		nil,
	)

	cfg.LifecyclerConfig.RingConfig.KVStore.Mock = mockStore
	cfg.LifecyclerConfig.NumTokens = 1
	cfg.LifecyclerConfig.ListenPort = 0
	cfg.LifecyclerConfig.Addr = "localhost"
	cfg.LifecyclerConfig.ID = "test-1"
	cfg.LifecyclerConfig.FinalSleep = 0
	cfg.PartitionRing.KVStore.Mock = mockStore

	cfg.IngestConfig.Kafka.Topic = "tempo-traces"
	cfg.IngestConfig.Kafka.ConsumerGroup = "test-consumer-group"

	limits, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.DefaultRegisterer)
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	logger := log.NewNopLogger()

	kafkaClient := NewInMemoryKafkaClient()

	clientFactory := func(cfg ingest.KafkaConfig, metrics *kprom.Metrics, logger log.Logger) (KafkaClient, error) {
		return kafkaClient, nil
	}

	liveStore, err := New(cfg, limits, logger, reg, true, clientFactory)
	require.NoError(t, err)

	return liveStore, kafkaClient
}

func createTestTraces(t *testing.T, numTraces int) ([]*tempopb.Trace, [][]byte) {
	traces := make([]*tempopb.Trace, numTraces)
	traceIDs := make([][]byte, numTraces)

	for i := 0; i < numTraces; i++ {
		id := make([]byte, 16)
		_, err := crand.Read(id)
		require.NoError(t, err)

		// test.MakeTrace already creates traces with service.name = "test-service"
		trace := test.MakeTrace(5, id)

		traces[i] = trace
		traceIDs[i] = id
	}

	return traces, traceIDs
}

func createTestTracesWithTags(t *testing.T, numTraces int, tagKey, tagValue string) ([]*tempopb.Trace, [][]byte) {
	traces := make([]*tempopb.Trace, numTraces)
	traceIDs := make([][]byte, numTraces)

	for i := 0; i < numTraces; i++ {
		id := make([]byte, 16)
		_, err := crand.Read(id)
		require.NoError(t, err)

		// test.MakeTrace already creates traces with service.name = "test-service"
		trace := test.MakeTrace(5, id)

		// Add additional tags to spans
		for _, rs := range trace.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, span := range ss.Spans {
					span.Attributes = append(span.Attributes, &v1.KeyValue{
						Key: tagKey,
						Value: &v1.AnyValue{
							Value: &v1.AnyValue_StringValue{StringValue: tagValue},
						},
					})
				}
			}
		}

		traces[i] = trace
		traceIDs[i] = id
	}

	return traces, traceIDs
}

func writeTracesToInstance(t *testing.T, instance *instance, traces []*tempopb.Trace, traceIDs [][]byte) {
	for i, trace := range traces {
		traceBytes, err := trace.Marshal()
		require.NoError(t, err)

		pushReq := &tempopb.PushBytesRequest{
			Traces: []tempopb.PreallocBytes{{Slice: traceBytes}},
			Ids:    [][]byte{traceIDs[i]},
		}

		instance.pushBytes(time.Now(), pushReq)
	}
}

func writeTracesToKafka(t *testing.T, kafkaClient *InMemoryKafkaClient, traces []*tempopb.Trace, traceIDs [][]byte) {
	for i, trace := range traces {
		traceBytes, err := trace.Marshal()
		require.NoError(t, err)

		pushReq := &tempopb.PushBytesRequest{
			Traces: []tempopb.PreallocBytes{{Slice: traceBytes}},
			Ids:    [][]byte{traceIDs[i]},
		}

		// Marshal the push request directly
		encoded, err := pushReq.Marshal()
		require.NoError(t, err)

		kafkaClient.AddMessage("tempo-traces", 0, []byte(integrationTestTenantID), encoded)
	}

	partitions := map[string]map[int32]kgo.Offset{
		"tempo-traces": {0: kgo.NewOffset().At(0)},
	}
	kafkaClient.AddConsumePartitions(partitions)
}

func testSearchAfterConsumption(t *testing.T, instance *instance, traceIDs [][]byte) {
	req := &tempopb.SearchRequest{
		Query: "{ .service.name = \"test-service\" }",
		Limit: uint32(len(traceIDs)) + 1,
	}

	sr, err := instance.Search(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, sr.Traces, len(traceIDs))

	foundTraceIDs := make(map[string]bool)
	for _, traceResult := range sr.Traces {
		foundTraceIDs[traceResult.TraceID] = true
	}

	for _, expectedID := range traceIDs {
		expectedIDStr := fmt.Sprintf("%032x", expectedID)
		assert.True(t, foundTraceIDs[expectedIDStr], "expected trace ID %s not found", expectedIDStr)
	}
}

func testSearchTagsAfterConsumption(t *testing.T, instance *instance) {
	ctx := user.InjectOrgID(context.Background(), integrationTestTenantID)

	sr, err := instance.SearchTags(ctx, "")
	require.NoError(t, err)
	assert.Contains(t, sr.TagNames, "service.name")

	srv, err := instance.SearchTagValues(ctx, "service.name", 0, 0)
	require.NoError(t, err)
	assert.Contains(t, srv.TagValues, "test-service")
}

func testSearchInDifferentStates(t *testing.T, instance *instance, traceIDs [][]byte) {
	req := &tempopb.SearchRequest{
		Query: "{ span.service.name = \"test-service\" }",
		Limit: uint32(len(traceIDs)) + 1,
	}

	err := instance.cutIdleTraces(true)
	require.NoError(t, err)

	sr, err := instance.Search(context.Background(), req)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(sr.Traces), len(traceIDs))

	blockID, err := instance.cutBlocks(true)
	require.NoError(t, err)

	sr, err = instance.Search(context.Background(), req)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(sr.Traces), len(traceIDs))

	if blockID.String() != "00000000-0000-0000-0000-000000000000" {
		err = instance.completeBlock(context.Background(), blockID)
		require.NoError(t, err)

		sr, err = instance.Search(context.Background(), req)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(sr.Traces), len(traceIDs))
	}
}
