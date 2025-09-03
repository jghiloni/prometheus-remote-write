package writer

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type RemoteMetricsWriter interface {
	WriteMetrics(context.Context) (int, error)
}

type writerImpl struct {
	hc        *http.Client
	targetURL string
	gatherers prometheus.Gatherers
	format    WriterFormat
	encoding  WriterEncoding
	version   string
}

type WriterFormat int

const (
	Protobuf WriterFormat = iota + 1
	JSON
)

type WriterEncoding int

const (
	None WriterEncoding = iota
	Snappy
	Gzip
)

type RemoteMetricsWriterOptions struct {
	TargetURL          string
	HTTPClient         *http.Client
	OutputFormat       WriterFormat
	OutputEncoding     WriterEncoding
	RemoteWriteVersion string
}

func NewRemoteMetricsWriter(options RemoteMetricsWriterOptions, gatherers ...prometheus.Gatherer) (RemoteMetricsWriter, error) {
	if strings.TrimSpace(options.TargetURL) == "" {
		return nil, errors.New("options.TargetURL must be set")
	}

	if options.HTTPClient == nil {
		options.HTTPClient = http.DefaultClient
	}

	if options.OutputFormat == 0 {
		options.OutputFormat = Protobuf
	}

	if gatherers == nil {
		gatherers = []prometheus.Gatherer{prometheus.DefaultGatherer}
	}

	if strings.TrimSpace(options.RemoteWriteVersion) == "" {
		options.RemoteWriteVersion = "0.1.0"
	}

	return &writerImpl{
		hc:        options.HTTPClient,
		targetURL: options.TargetURL,
		gatherers: gatherers,
		format:    options.OutputFormat,
		encoding:  options.OutputEncoding,
		version:   options.RemoteWriteVersion,
	}, nil
}
