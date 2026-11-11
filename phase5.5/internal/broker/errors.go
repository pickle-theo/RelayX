package broker

import "errors"

// PermanentError marks a failure that retrying cannot fix: the record is
// malformed, violates the event contract or references data that does not
// exist. Such a record must not block its partition forever, so the consumer
// moves it to the dead-letter topic instead of retrying indefinitely.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string {
	if e == nil || e.Err == nil {
		return "broker: permanent error"
	}
	return "broker: permanent error: " + e.Err.Error()
}

// Unwrap exposes the wrapped cause to errors.Is and errors.As.
func (e *PermanentError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Permanent wraps err so the consumer routes the record to the dead-letter
// topic. A nil error stays nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// IsPermanent reports whether err, or any error it wraps, is permanent.
func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}
