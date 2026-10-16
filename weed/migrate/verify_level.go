package migrate

import (
	"fmt"
	"strings"
)

// VerifyLevel is the comparison depth for migrate-time and offline verify.
type VerifyLevel int

const (
	VerifyNone VerifyLevel = iota
	VerifyExists
	VerifySize
	// VerifyCRC compares the SeaweedFS needle CRC32 (volume ETag) and is blob-only.
	// It covers size, and does not cover md5.
	VerifyCRC
	VerifyMD5
)

func ParseVerifyLevel(s string) (VerifyLevel, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "meta", "size":
		return VerifySize, nil
	case "none", "off", "false":
		return VerifyNone, nil
	case "exists", "exist":
		return VerifyExists, nil
	case "md5":
		return VerifyMD5, nil
	case "crc", "checksum", "etag":
		return VerifyCRC, nil
	default:
		return VerifyNone, fmt.Errorf("unknown verify level %q (want none|exists|size|crc|md5)", s)
	}
}

func (l VerifyLevel) String() string {
	switch l {
	case VerifyNone:
		return "none"
	case VerifyExists:
		return "exists"
	case VerifySize:
		return "size"
	case VerifyMD5:
		return "md5"
	case VerifyCRC:
		return "crc"
	default:
		return fmt.Sprintf("VerifyLevel(%d)", int(l))
	}
}

// AtLeast reports whether l includes the checks of other.
func (l VerifyLevel) AtLeast(other VerifyLevel) bool {
	return l >= other
}
