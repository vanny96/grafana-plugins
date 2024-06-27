package jsonframer

import "errors"

var (
	ErrInvalidRootSelector = errors.New("failed to compile JSONata expression")
	ErrEvaluatingJSONata   = errors.New("error evaluating JSONata expression")
	ErrInvalidJSONContent  = errors.New("invalid/empty JSON")
)
