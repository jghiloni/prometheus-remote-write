package writer

import (
	"errors"
)

var (
	ErrNilContext         = errors.New("nil context passed")
	ErrNoGatherersDefined = errors.New("no gatherers were defined")
)
