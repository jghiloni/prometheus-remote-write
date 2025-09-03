package writer

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

const DefaultRemoteWriteVersion = "0.1.0"

// RemoteMetricsWriter knows how to marshal a set of metrics and send them to a remote
// prometheus endpoint
type RemoteMetricsWriter interface {
	WriteMetrics(context.Context) (int, error)
}

type writerImpl struct {
	hc        *http.Client
	targetURL string
	gatherers prometheus.Gatherers
	format    Format
	encoding  Compression
	version   string
}

// Format represents the format to which metrics will be marshalled before sending to Prometheus
type Format int

const (
	// Protobuf serializes to the protobuf description from the Prometheus github repository
	Protobuf Format = iota + 1
	// JSON serializes to standard JSON according to the Prometheus objects' JSON tags
	JSON
)

// Compression is the compression algorithm used on the marshalled data before sending
type Compression int

const (
	// None tells the engine not to compress at all
	None Compression = iota
	// Snappy uses the snappy compression algorithm described at https://github.com/google/snappy
	Snappy
	// Gzip uses the standard Gzip compression algorithm with default compression level
	Gzip
)

// RemoteMetricsWriterOptions are the optional settings for a RemoteMetricsWriter.
//
//	If HTTPClient is not set, http.DefaultClient is used
//	If Format is not set, it defaults to Protobuf
//	If Compression is not set, it defaults to None
//	If RemoteWriteVersion is not set, it defaults to DefaultRemoteWriteVersion (0.1.0, currently). This should never change
type RemoteMetricsWriterOptions struct {
	HTTPClient         *http.Client
	Format             Format
	Compression        Compression
	RemoteWriteVersion string
}

// NewRemoteMetricsWriter attempts to create and return a new RemoteMetricsWriter, and will do so unless targetURL is
// the empty string (or only whitespace). If no gatherers are specified, prometheus.DefaultGatherer is used.
func NewRemoteMetricsWriter(targetURL string, options RemoteMetricsWriterOptions, gatherers ...prometheus.Gatherer) (RemoteMetricsWriter, error) {
	if strings.TrimSpace(targetURL) == "" {
		return nil, errors.New("options.TargetURL must be set")
	}

	if options.HTTPClient == nil {
		options.HTTPClient = http.DefaultClient
	}

	if options.Format == 0 {
		options.Format = Protobuf
	}

	if gatherers == nil {
		gatherers = []prometheus.Gatherer{prometheus.DefaultGatherer}
	}

	if strings.TrimSpace(options.RemoteWriteVersion) == "" {
		options.RemoteWriteVersion = DefaultRemoteWriteVersion
	}

	return &writerImpl{
		hc:        options.HTTPClient,
		targetURL: targetURL,
		gatherers: gatherers,
		format:    options.Format,
		encoding:  options.Compression,
		version:   options.RemoteWriteVersion,
	}, nil
}
