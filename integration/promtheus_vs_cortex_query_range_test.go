//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestPrometheusVSCortexQueryRange(t *testing.T) {
	const blockRangePeriod = 5 * time.Minute

	t.Run("Prometheus VS Cortex query range dps test", func(t *testing.T) {
		s, err := e2e.NewScenario(networkName)
		require.NoError(t, err)
		defer s.Close()

		// Start dependencies.
		consul := e2edb.NewConsul()
		etcd := e2edb.NewETCD()
		minio := e2edb.NewMinio(9000, bucketName)
		require.NoError(t, s.StartAndWaitReady(consul, etcd, minio))

		// Configure the querier to only look in ingester
		// and enbale distributor ha tracker with mixed samples.
		distributorFlags := map[string]string{
			"-distributor.ha-tracker.enable": "false",
			// "-distributor.ha-tracker.enable-for-all-users":          "true",
			// "-experimental.distributor.ha-tracker.mixed-ha-samples": "true",
			// "-distributor.ha-tracker.cluster":                       "cluster",
			// "-distributor.ha-tracker.replica":                       "__replica__",
			// "-distributor.ha-tracker.store":                         "etcd",
			// "-distributor.ha-tracker.etcd.endpoints":                "etcd:2379",
		}
		querierFlags := mergeFlags(BlocksStorageFlags(), map[string]string{
			"-querier.query-store-after": (400 * time.Hour).String(),
		})
		flags := mergeFlags(BlocksStorageFlags(), map[string]string{
			// "-blocks-storage.tsdb.block-ranges-period":          blockRangePeriod.String(),
			// "-blocks-storage.tsdb.ship-interval":                "5s",
			// "-blocks-storage.tsdb.retention-period":             ((blockRangePeriod * 2) - 1).String(),
			// "-blocks-storage.bucket-store.max-chunk-pool-bytes": "1",
			"-ingester.out-of-order-time-window": "400h",
		})

		// Start Cortex components.
		distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), distributorFlags, "")
		ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
		require.NoError(t, s.StartAndWaitReady(distributor, ingester))

		// Wait until both the distributor and ingester have updated the ring.
		require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

		distributorClient, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
		require.NoError(t, err)

		// Push some series to Cortex.
		lastTimestamp := time.Now().Truncate(24 * time.Hour).Add(-24 * time.Hour).Add(30 * time.Minute)
		firstTimestamp := lastTimestamp
		var dps = make([]prompb.TimeSeries, 0)
		for i := 0; i <= 5*24; i++ {
			ts := lastTimestamp.Add(time.Duration(-i) * time.Hour)
			// series, _ := generateSeries("prometheus_vs_cortex_query_range", timestamp)
			dps = append(dps, prompb.TimeSeries{
				Labels: []prompb.Label{
					{Name: labels.MetricName, Value: "prometheus_vs_cortex_query_range"},
				},
				Samples: []prompb.Sample{
					{Value: 1.0, Timestamp: TimeToMilliseconds(ts)},
				},
			})
			firstTimestamp = ts
		}
		fmt.Println("Dps", dps)

		res, err := distributorClient.Push(dps)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		// Wait until the samples have been deduped.
		// require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(2), "cortex_distributor_deduped_samples_total"))
		// require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(3), "cortex_distributor_non_ha_samples_received_total"))

		// Start the querier and store-gateway, and configure them to frequently sync blocks fast enough to trigger consistency check.
		storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
		querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(querierFlags, flags), "")
		require.NoError(t, s.StartAndWaitReady(querier, storeGateway))

		// Wait until the querier and store-gateway have updated the ring, and wait until the blocks are old enough for consistency check
		require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512*2), "cortex_ring_tokens_total"))
		require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

		time.Sleep(10 * time.Second)

		// Query back the series.
		querierClient, err := e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
		require.NoError(t, err)

		// Query back the series (only in the ingesters).
		fmt.Println("First timestamp", firstTimestamp)
		fmt.Println("Last timestamp", firstTimestamp)
		fmt.Println("Start", firstTimestamp.Add(-30*time.Minute).Add(19*time.Hour))
		fmt.Println("End", firstTimestamp.Add(-30*time.Minute).Add(5*24*time.Hour).Add(19*time.Hour))
		fmt.Println("Step", 24*time.Hour)
		result, err := querierClient.QueryRange("sum(sum_over_time(prometheus_vs_cortex_query_range[86399999ms] offset 1ms))",
			firstTimestamp.Add(-30*time.Minute).Add(19*time.Hour),
			firstTimestamp.Add(-30*time.Minute).Add(5*24*time.Hour).Add(19*time.Hour),
			24*time.Hour)
		require.NoError(t, err)

		require.Equal(t, model.ValMatrix, result.Type())
		m := result.(model.Matrix)
		fmt.Println("Response matrix", m)

		// Ensure no service-specific metrics prefix is used by the wrong service.
		assertServiceMetricsPrefixes(t, Distributor, distributor)
		assertServiceMetricsPrefixes(t, Ingester, ingester)
		assertServiceMetricsPrefixes(t, StoreGateway, storeGateway)
		assertServiceMetricsPrefixes(t, Querier, querier)
	})
}

func TimeToMilliseconds(t time.Time) int64 {
	// Convert to seconds.
	sec := float64(t.Unix()) + float64(t.Nanosecond())/1e9

	// Parse seconds.
	s, ns := math.Modf(sec)

	// Round nanoseconds part.
	ns = math.Round(ns*1000) / 1000

	// Convert to millis.
	return (int64(s) * 1e3) + (int64(ns * 1e3))
}
