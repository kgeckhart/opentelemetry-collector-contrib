// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/service/telemetry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
)

// TestStalenessMarkersEndToEnd verifies that staleness markers are emitted for every
// distinct series that disappears and not for series that remain. The scrape server
// exposes three series; two disappear after 5 scrapes, one remains throughout.
// See https://github.com/open-telemetry/opentelemetry-collector/issues/3413
func TestStalenessMarkersEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("This test can take a long time")
	}

	ctx, cancel := context.WithCancel(t.Context())

	// 1. Setup the server that sends series 3, two of which disappear after 5 scrapes.
	n := &atomic.Uint64{}
	scrapeServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		// Increment the scrape count atomically per scrape.
		i := n.Add(1)

		select {
		case <-ctx.Done():
			return
		default:
		}

		if i <= 5 {
			fmt.Fprintf(rw, "# TYPE test_gauge gauge\ntest_gauge{series=\"0\"} %d\n", i)
			fmt.Fprintf(rw, "# TYPE test_gauge gauge\ntest_gauge{series=\"1\"} %d\n", i)
		}
		fmt.Fprintf(rw, "# TYPE test_gauge gauge\ntest_gauge{series=\"2\"} %d\n", i)
	}))
	defer scrapeServer.Close()

	serverURL, err := url.Parse(scrapeServer.URL)
	require.NoError(t, err)

	// 2. Set up the Prometheus RemoteWrite endpoint.
	prweUploads := make(chan *prompb.WriteRequest)
	prweServer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		// Snappy decode the uploads.
		payload, rerr := io.ReadAll(req.Body)
		assert.NoError(t, rerr)

		recv := make([]byte, len(payload))
		decoded, derr := snappy.Decode(recv, payload)
		assert.NoError(t, derr)

		writeReq := new(prompb.WriteRequest)
		assert.NoError(t, proto.Unmarshal(decoded, writeReq))

		select {
		case <-ctx.Done():
			return
		case prweUploads <- writeReq:
		}
	}))
	defer prweServer.Close()

	// 3. Configure the OpenTelemetry Collector pipeline.
	cfg := fmt.Sprintf(`
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: 'test'
          scrape_interval: 100ms
          static_configs:
            - targets: [%q]

exporters:
  prometheusremotewrite:
    endpoint: %q
    tls:
      insecure: true

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      exporters: [prometheusremotewrite]`, serverURL.Host, prweServer.URL)

	confFile, err := os.CreateTemp(os.TempDir(), "conf-")
	require.NoError(t, err)
	defer os.Remove(confFile.Name())
	_, err = confFile.WriteString(cfg)
	require.NoError(t, err)

	// 4. Run the OpenTelemetry Collector.
	receivers, err := otelcol.MakeFactoryMap[receiver.Factory](prometheusreceiver.NewFactory())
	require.NoError(t, err)
	exporters, err := otelcol.MakeFactoryMap[exporter.Factory](prometheusremotewriteexporter.NewFactory())
	require.NoError(t, err)
	processors, err := otelcol.MakeFactoryMap[processor.Factory](batchprocessor.NewFactory())
	require.NoError(t, err)

	factories := otelcol.Factories{
		Receivers:  receivers,
		Exporters:  exporters,
		Processors: processors,
		Telemetry: telemetry.NewFactory(
			func() component.Config { return struct{}{} },
		),
	}

	appSettings := otelcol.CollectorSettings{
		Factories: func() (otelcol.Factories, error) { return factories, nil },
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:              []string{confFile.Name()},
				ProviderFactories: []confmap.ProviderFactory{fileprovider.NewFactory()},
			},
		},
		BuildInfo: component.BuildInfo{
			Command:     "otelcol",
			Description: "OpenTelemetry Collector",
			Version:     "tests",
		},
		LoggingOptions: []zap.Option{
			// Turn off the verbose logging from the collector.
			zap.WrapCore(func(zapcore.Core) zapcore.Core {
				return zapcore.NewNopCore()
			}),
		},
	}

	app, err := otelcol.NewCollector(appSettings)
	require.NoError(t, err)

	go func() {
		assert.NoError(t, app.Run(t.Context()))
	}()
	defer app.Shutdown()

	// Wait until the collector has actually started.
	require.Eventually(t, func() bool {
		state := app.GetState()
		return state == otelcol.StateRunning || state == otelcol.StateClosed || state == otelcol.StateClosing
	}, 30*time.Second, 10*time.Millisecond, "collector did not start")

	// 5. Let's wait on 10 fetches so series 0 and 1 have had time to go stale.
	var allReqs []*prompb.WriteRequest
	for range 10 {
		allReqs = append(allReqs, <-prweUploads)
	}
	defer cancel()

	// 6. Assert that we encounter the stale markers per series.
	stalePerSeries := make(map[string]bool)
	for _, req := range allReqs {
		for _, ts := range req.Timeseries {
			var metricName, seriesLabel string
			for _, lbl := range ts.Labels {
				switch lbl.Name {
				case "__name__":
					metricName = lbl.Value
				case "series":
					seriesLabel = lbl.Value
				}
			}
			if metricName != "test_gauge" {
				continue
			}
			for _, sample := range ts.Samples {
				if value.IsStaleNaN(sample.Value) {
					stalePerSeries[seriesLabel] = true
				}
			}
		}
	}

	assert.True(t, stalePerSeries["0"], "series=0 did not receive a staleness marker")
	assert.True(t, stalePerSeries["1"], "series=1 did not receive a staleness marker")
	assert.False(t, stalePerSeries["2"], "series=2 should not have received a staleness marker")
}
