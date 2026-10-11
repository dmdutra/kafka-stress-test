package metrics

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/prompb"
)

// StartRemoteWrite starts a background process to push metrics to a Prometheus Remote Write endpoint.
func StartRemoteWrite(ctx context.Context, url string, interval time.Duration, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	client := &http.Client{Timeout: 10 * time.Second}

	for {
		select {
		case <-ctx.Done():
			// Final flush
			_ = pushRemoteWrite(client, url)
			return
		case <-ticker.C:
			if err := pushRemoteWrite(client, url); err != nil {
				// We drop the error intentionally to prevent spamming logs
				// A real production tool might have verbose logging modes.
			}
		}
	}
}

func pushRemoteWrite(client *http.Client, url string) error {
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return err
	}

	ts := translate(mfs, time.Now().UnixMilli())
	if len(ts) == 0 {
		return nil
	}

	req := prompb.WriteRequest{Timeseries: ts}
	data, err := proto.Marshal(&req)
	if err != nil {
		return err
	}

	compressed := snappy.Encode(nil, data)

	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(compressed))
	if err != nil {
		return err
	}

	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("remote write returned status %d", resp.StatusCode)
	}

	return nil
}

func translate(families []*dto.MetricFamily, tsMs int64) []prompb.TimeSeries {
	var timeseries []prompb.TimeSeries
	for _, mf := range families {
		name := mf.GetName()
		for _, m := range mf.GetMetric() {
			baseLabels := []prompb.Label{{Name: "__name__", Value: name}}
			for _, l := range m.GetLabel() {
				baseLabels = append(baseLabels, prompb.Label{Name: l.GetName(), Value: l.GetValue()})
			}

			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				if m.Counter != nil {
					timeseries = append(timeseries, prompb.TimeSeries{
						Labels:  baseLabels,
						Samples: []prompb.Sample{{Value: m.Counter.GetValue(), Timestamp: tsMs}},
					})
				}
			case dto.MetricType_GAUGE:
				if m.Gauge != nil {
					timeseries = append(timeseries, prompb.TimeSeries{
						Labels:  baseLabels,
						Samples: []prompb.Sample{{Value: m.Gauge.GetValue(), Timestamp: tsMs}},
					})
				}
			case dto.MetricType_HISTOGRAM:
				if m.Histogram != nil {
					// count
					countLabels := make([]prompb.Label, len(baseLabels))
					copy(countLabels, baseLabels)
					countLabels[0].Value = name + "_count"
					timeseries = append(timeseries, prompb.TimeSeries{
						Labels:  countLabels,
						Samples: []prompb.Sample{{Value: float64(m.Histogram.GetSampleCount()), Timestamp: tsMs}},
					})

					// sum
					sumLabels := make([]prompb.Label, len(baseLabels))
					copy(sumLabels, baseLabels)
					sumLabels[0].Value = name + "_sum"
					timeseries = append(timeseries, prompb.TimeSeries{
						Labels:  sumLabels,
						Samples: []prompb.Sample{{Value: m.Histogram.GetSampleSum(), Timestamp: tsMs}},
					})

					// buckets
					for _, b := range m.Histogram.GetBucket() {
						bucketLabels := make([]prompb.Label, len(baseLabels))
						copy(bucketLabels, baseLabels)
						bucketLabels[0].Value = name + "_bucket"
						bucketLabels = append(bucketLabels, prompb.Label{Name: "le", Value: fmt.Sprintf("%g", b.GetUpperBound())})
						timeseries = append(timeseries, prompb.TimeSeries{
							Labels:  bucketLabels,
							Samples: []prompb.Sample{{Value: float64(b.GetCumulativeCount()), Timestamp: tsMs}},
						})
					}

					// +Inf bucket (implicit in protobuf, must be explicitly sent to Remote Write)
					infLabels := make([]prompb.Label, len(baseLabels))
					copy(infLabels, baseLabels)
					infLabels[0].Value = name + "_bucket"
					infLabels = append(infLabels, prompb.Label{Name: "le", Value: "+Inf"})
					timeseries = append(timeseries, prompb.TimeSeries{
						Labels:  infLabels,
						Samples: []prompb.Sample{{Value: float64(m.Histogram.GetSampleCount()), Timestamp: tsMs}},
					})
				}
			}
		}
	}
	return timeseries
}
