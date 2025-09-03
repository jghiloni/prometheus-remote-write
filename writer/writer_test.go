package writer_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/golang/snappy"
	"github.com/jghiloni/prometheus-remote-write/writer"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
)

func receiveMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		http.NotFound(w, r)
	}
	defer r.Body.Close()

	var encoded []byte
	var err error
	if r.Body != nil {
		encoded, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	var decoded []byte
	encoding := r.Header.Get("Content-Encoding")
	switch strings.ToLower(encoding) {
	case "gzip":
		gr, err := gzip.NewReader(bytes.NewReader(encoded))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		decoded, err = io.ReadAll(gr)
		gr.Close()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case "snappy":
		decoded, err = snappy.Decode(nil, encoded)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	ct := r.Header.Get("Content-Type")
	var wr prompb.WriteRequest
	switch strings.ToLower(ct) {
	case "application/json":
		err = json.Unmarshal(decoded, &wr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case "application/x-protobuf":
		err = wr.Unmarshal(decoded)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	http.Error(w, "OK", http.StatusOK)
}

var _ = Describe("Writer", func() {
	var s *httptest.Server
	var c prometheus.Counter
	var g prometheus.Gauge
	var h prometheus.Histogram

	BeforeEach(func() {
		s = httptest.NewServer(http.HandlerFunc(receiveMetrics))

		c = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "foo",
			Subsystem:   "bar",
			Name:        "baz",
			Help:        "quxx",
			ConstLabels: nil,
		})

		g = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "foo",
			Subsystem: "bar",
			Name:      "wbbl",
			Help:      "asf",
			ConstLabels: prometheus.Labels{
				"label1": "value1",
				"label2": "value2",
			},
		})

		h = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "foo",
			Subsystem: "hist",
			Name:      "testhist",
			Help:      "",
			ConstLabels: prometheus.Labels{
				"label1": "value1",
				"label2": "value2",
			},
			Buckets: []float64{0, 0.05, 0.2, 0.5, 0.9, 0.95, 0.99},
		})
	})

	AfterEach(func() {
		s.Close()
	})

	It("Handles the default registry with snappy protobuf", func() {
		err := prometheus.Register(c)
		Expect(err).ShouldNot(HaveOccurred())
		err = prometheus.Register(g)
		Expect(err).ShouldNot(HaveOccurred())
		err = prometheus.Register(h)
		Expect(err).ShouldNot(HaveOccurred())

		c.Add(0.5)
		g.Add(1.0)
		h.Observe(0.35)

		w, err := writer.NewRemoteMetricsWriter(writer.RemoteMetricsWriterOptions{
			TargetURL:      s.URL,
			HTTPClient:     s.Client(),
			OutputFormat:   writer.Protobuf,
			OutputEncoding: writer.Snappy,
		})
		Expect(err).ShouldNot(HaveOccurred())

		tsWritten, err := w.WriteMetrics(context.Background())
		Expect(err).ShouldNot(HaveOccurred())
		Expect(tsWritten).Should(BeNumerically(">", 3))
	})

	It("Handles a custom registry with gzip json", func() {
		r := prometheus.NewRegistry()
		err := r.Register(c)
		Expect(err).ShouldNot(HaveOccurred())
		err = r.Register(g)
		Expect(err).ShouldNot(HaveOccurred())
		err = r.Register(h)
		Expect(err).ShouldNot(HaveOccurred())

		c.Add(0.5)
		g.Add(1.0)
		h.Observe(0.35)

		w, err := writer.NewRemoteMetricsWriter(writer.RemoteMetricsWriterOptions{
			TargetURL:      s.URL,
			HTTPClient:     s.Client(),
			OutputFormat:   writer.JSON,
			OutputEncoding: writer.Gzip,
		}, r)
		Expect(err).ShouldNot(HaveOccurred())

		tsWritten, err := w.WriteMetrics(context.Background())
		Expect(err).ShouldNot(HaveOccurred())
		Expect(tsWritten).Should(Equal(3))

	})
})
