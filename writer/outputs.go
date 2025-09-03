package writer

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func (f WriterFormat) String() string {
	switch f {
	case Protobuf:
		return "protobuf"
	case JSON:
		return "json"
	default:
		return fmt.Sprintf("%%INVALID!(%d)", f)
	}
}

func (f WriterFormat) Marshal(wr prompb.WriteRequest) ([]byte, error) {
	switch f {
	case Protobuf:
		return wr.Marshal()
	case JSON:
		return json.Marshal(wr)
	default:
		return nil, fmt.Errorf("unrecognized format %s", f)
	}
}

func (f WriterFormat) UpdateRequest(req *http.Request) {
	contentType := "application/octet-stream"
	switch f {
	case Protobuf:
		contentType = "application/x-protobuf"
	case JSON:
		contentType = "application/json"
	}
	req.Header.Set("Content-Type", contentType)
}

func (e WriterEncoding) String() string {
	switch e {
	case None:
		return "none"
	case Snappy:
		return "snappy"
	case Gzip:
		return "gzip"
	default:
		return fmt.Sprintf("%%INVALID!(%d)", e)
	}
}

func (e WriterEncoding) Compress(data []byte) ([]byte, error) {
	switch e {
	case None:
		return data, nil
	case Snappy:
		return snappy.Encode(nil, data), nil
	case Gzip:
		buf := &bytes.Buffer{}
		w := gzip.NewWriter(buf)
		_, err := w.Write(data)
		if err != nil {
			return nil, err
		}
		w.Close()

		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported encoding %s", e)
	}
}

func (e WriterEncoding) UpdateRequest(req *http.Request) {
	if e != None {
		req.Header.Set("Content-Encoding", e.String())
	}
}
