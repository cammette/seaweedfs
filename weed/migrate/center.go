package migrate

import (
	"fmt"
	"strings"
)

// centerSep separates a cluster id from the original needle name: "a::photo.jpg".
const centerSep = "::"

// FileCenter returns the leading center id when name is "id::rest".
func FileCenter(name string) (center string, ok bool) {
	i := strings.Index(name, centerSep)
	if i <= 0 {
		return "", false
	}
	center = name[:i]
	if strings.Contains(center, ":") || center == "" {
		return "", false
	}
	return center, true
}

// StampCenter prefixes name with center when it does not already carry one.
func StampCenter(name, center string) string {
	if center == "" {
		return name
	}
	if _, ok := FileCenter(name); ok {
		return name
	}
	return center + centerSep + name
}

// SkipForeignCenter reports whether name is already owned by targetCenter.
// Those needles are not copied or deleted on the target.
func SkipForeignCenter(name, targetCenter string) bool {
	if targetCenter == "" {
		return false
	}
	center, ok := FileCenter(name)
	return ok && center == targetCenter
}

func validCenterID(id string) error {
	if id == "" {
		return fmt.Errorf("center id is empty")
	}
	if strings.Contains(id, ":") || strings.ContainsAny(id, "/\\") {
		return fmt.Errorf("center id %q must not contain ':' or a path separator", id)
	}
	return nil
}
