package writer

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/jghiloni/go-commonutils/v2/slices"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/prompb"
)

type valued interface {
	GetValue() float64
}

type hasExemplar interface {
	GetExemplar() *dto.Exemplar
}

// WriteMetrics takes all the metrics from the gatherers specified when the RemoteMetricsWriter was created,
// converts them into a list of Timeseries and Metadata, then serializes and compresses it before sending to
// the target endpoint. If sent successfully, it will return the number of timeseries actually sent to the
// server. If an error occurs, no partial data will be sent, and the number returned will always be 0.
func (w *writerImpl) WriteMetrics(ctx context.Context) (int, error) {
	if ctx == nil {
		return 0, ErrNilContext
	}

	if len(w.gatherers) == 0 {
		return 0, ErrNoGatherersDefined
	}

	metricFamilies, err := w.gatherers.Gather()
	if err != nil {
		return 0, err
	}

	ts := make([]prompb.TimeSeries, 0, len(metricFamilies))
	metadata := make([]prompb.MetricMetadata, 0, len(metricFamilies))

	if len(metricFamilies) == 0 {
		return 0, nil
	}

	for _, metricsFamily := range metricFamilies {
		metadata = append(metadata, prompb.MetricMetadata{
			Type:             prompb.MetricMetadata_MetricType(metricsFamily.GetType()),
			MetricFamilyName: metricsFamily.GetName(),
			Help:             metricsFamily.GetHelp(),
			Unit:             metricsFamily.GetUnit(),
		})

		ts = append(ts, getTimeseries(metricsFamily)...)
	}

	wr := prompb.WriteRequest{
		Timeseries: ts,
		Metadata:   metadata,
	}

	uncompressed, err := w.format.Marshal(wr)
	if err != nil {
		return 0, err
	}

	compressed, err := w.encoding.Compress(uncompressed)
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.targetURL, bytes.NewBuffer(compressed))
	if err != nil {
		return 0, err
	}

	req.Header.Add("X-Prometheus-Remote-Write-Version", w.version)
	w.format.UpdateRequest(req)
	w.encoding.UpdateRequest(req)

	resp, err := w.hc.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return 0, fmt.Errorf("expected 2xx HTTP code, but got %s", resp.Status)
	}

	return len(ts), nil
}

func convertLabels(dtoLabels []*dto.LabelPair) []prompb.Label {
	return slices.Map(dtoLabels, func(lp *dto.LabelPair) prompb.Label {
		return prompb.Label{
			Name:  lp.GetName(),
			Value: lp.GetValue(),
		}
	})
}

func convertExemplar(e *dto.Exemplar) prompb.Exemplar {
	return prompb.Exemplar{
		Labels:    convertLabels(e.GetLabel()),
		Value:     e.GetValue(),
		Timestamp: e.GetTimestamp().AsTime().UnixMilli(),
	}
}

func convertBucketSpans(spans []*dto.BucketSpan) []prompb.BucketSpan {
	return slices.Map(spans, func(s *dto.BucketSpan) prompb.BucketSpan {
		return prompb.BucketSpan{
			Offset: s.GetOffset(),
			Length: s.GetLength(),
		}
	})
}

func getTimeseries(family *dto.MetricFamily) []prompb.TimeSeries {
	if family.GetMetric() == nil {
		return nil
	}

	ts := make([]prompb.TimeSeries, len(family.GetMetric()))
	for i, metric := range family.GetMetric() {
		prompbLabels := convertLabels(metric.Label)
		prompbLabels = append(prompbLabels, prompb.Label{Name: "__name__", Value: family.GetName()})

		var samplerMetric any
		var histogram *dto.Histogram
		switch {
		case metric.GetGauge() != nil:
			samplerMetric = metric.GetGauge()
		case metric.GetCounter() != nil:
			samplerMetric = metric.GetCounter()
		case metric.GetUntyped() != nil:
			samplerMetric = metric.GetUntyped()
		case metric.GetHistogram() != nil:
			samplerMetric = nil
			histogram = metric.GetHistogram()
		}

		samples := make([]prompb.Sample, 0, 1)
		exemplars := make([]prompb.Exemplar, 0, 1)
		histograms := make([]prompb.Histogram, 0, 1)

		if samplerMetric != nil {
			v, ok := samplerMetric.(valued)
			if ok {
				samples = append(samples, prompb.Sample{
					Value:     v.GetValue(),
					Timestamp: metric.GetTimestampMs(),
				})
			}

			e, ok := samplerMetric.(hasExemplar)
			if ok {
				exemplars = append(exemplars, convertExemplar(e.GetExemplar()))
			}
		}

		if histogram != nil {
			samples = nil
			exemplars = slices.Map(histogram.GetExemplars(), convertExemplar)
			histograms = append(histograms, prompb.Histogram{
				Sum:            histogram.GetSampleSum(),
				Schema:         histogram.GetSchema(),
				ZeroThreshold:  histogram.GetZeroThreshold(),
				NegativeSpans:  convertBucketSpans(histogram.GetNegativeSpan()),
				NegativeDeltas: histogram.GetNegativeDelta(),
				NegativeCounts: histogram.GetNegativeCount(),
				PositiveSpans:  convertBucketSpans(histogram.GetPositiveSpan()),
				PositiveDeltas: histogram.GetPositiveDelta(),
				PositiveCounts: histogram.GetPositiveCount(),
				Timestamp:      histogram.GetCreatedTimestamp().AsTime().UnixMilli(),
			})
		}

		ts[i] = prompb.TimeSeries{
			Labels:     prompbLabels,
			Samples:    samples,
			Exemplars:  exemplars,
			Histograms: histograms,
		}
	}

	return ts
}
