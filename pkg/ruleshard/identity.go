package ruleshard

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// AutoIndex is the sentinel value for --rules.shard-index meaning "derive the
// index from the environment".
const AutoIndex = "auto"

// IndexEnvVar is the explicit override for the shard index. It takes precedence
// over hostname-derived detection.
const IndexEnvVar = "PROMXY_SHARD_INDEX"

// podNameEnvVar is the conventional Downward API variable name used by the
// promxy Kubernetes manifests.
const podNameEnvVar = "POD_NAME"

// ordinalSuffix matches the trailing ordinal that a Kubernetes StatefulSet
// appends to its pod names ("promxy-2" -> 2).
var ordinalSuffix = regexp.MustCompile(`-(\d+)$`)

// ResolveIndex turns the --rules.shard-index flag value into a concrete index.
//
// A literal integer is used as-is. The value "auto" derives the index from, in
// order of precedence:
//
//  1. $PROMXY_SHARD_INDEX
//  2. the trailing ordinal of $POD_NAME (Kubernetes StatefulSet convention)
//  3. the trailing ordinal of the hostname
//
// Resolution deliberately fails rather than defaulting to 0. Silently
// collapsing every replica onto shard 0 would leave the rule set evaluated N
// times over and (N-1)/N of it never evaluated at all -- precisely the failure
// sharding exists to prevent, but much harder to notice than a startup error.
//
// When count <= 1 sharding is disabled and the index is irrelevant, so "auto"
// resolves to 0 without consulting the environment.
func ResolveIndex(flagValue string, count int) (int, error) {
	if count <= 1 {
		if flagValue == "" || flagValue == AutoIndex {
			return 0, nil
		}
		// Still validate an explicitly supplied value so typos surface.
		i, err := strconv.Atoi(flagValue)
		if err != nil {
			return 0, fmt.Errorf("invalid rules shard index %q: must be an integer or %q", flagValue, AutoIndex)
		}
		return i, nil
	}

	if flagValue != "" && flagValue != AutoIndex {
		i, err := strconv.Atoi(flagValue)
		if err != nil {
			return 0, fmt.Errorf("invalid rules shard index %q: must be an integer or %q", flagValue, AutoIndex)
		}
		return i, nil
	}

	if v := strings.TrimSpace(os.Getenv(IndexEnvVar)); v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("invalid %s=%q: must be an integer", IndexEnvVar, v)
		}
		return i, nil
	}

	if v := strings.TrimSpace(os.Getenv(podNameEnvVar)); v != "" {
		if i, ok := ordinalFrom(v); ok {
			return i, nil
		}
		return 0, fmt.Errorf("cannot derive rules shard index from %s=%q: expected a trailing ordinal such as \"promxy-0\" (StatefulSet naming); set --rules.shard-index or %s explicitly", podNameEnvVar, v, IndexEnvVar)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return 0, fmt.Errorf("cannot derive rules shard index: reading hostname failed: %w; set --rules.shard-index or %s explicitly", err, IndexEnvVar)
	}
	if i, ok := ordinalFrom(hostname); ok {
		return i, nil
	}
	return 0, fmt.Errorf("cannot derive rules shard index from hostname %q: expected a trailing ordinal such as \"promxy-0\" (StatefulSet naming); set --rules.shard-index or %s explicitly", hostname, IndexEnvVar)
}

// ordinalFrom extracts the trailing ordinal of a StatefulSet-style name.
func ordinalFrom(name string) (int, bool) {
	m := ordinalSuffix.FindStringSubmatch(name)
	if m == nil {
		return 0, false
	}
	i, err := strconv.Atoi(m[1])
	if err != nil {
		// Unreachable for a \d+ capture of sane length, but a pathologically
		// long digit run would overflow.
		return 0, false
	}
	return i, true
}
