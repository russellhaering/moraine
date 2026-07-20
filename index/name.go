package index

import (
	"errors"
	"fmt"
	"strings"
)

var errUnsafeName = errors.New("index: name must be a safe path segment")

// ValidateName checks that name is safe for use as a local index path segment.
func ValidateName(name string) error {
	if name == "" || name == "." || name == ".." {
		return errUnsafeName
	}
	if strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("%w: %q", errUnsafeName, name)
	}
	return nil
}
