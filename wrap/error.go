package wrap

import "fmt"

func Err(msg string, e *error) { //nolint:gocritic
	if e == nil || *e == nil {
		return
	}
	*e = fmt.Errorf(msg+": %w", *e)
}

func Errf(format string, e *error, values ...interface{}) { //nolint:gocritic
	if e == nil || *e == nil {
		return
	}
	*e = fmt.Errorf(format+": %w", append(values, *e)...)
}
