// Package ruleshard distributes rule-group evaluation across multiple promxy
// replicas.
//
// By default every promxy replica evaluates every rule group. For alerting
// rules that is mostly harmless (Alertmanager deduplicates identical alerts by
// fingerprint), but for recording rules it means N replicas each write the same
// series to the configured remote_write endpoint, and it means the cost of rule
// evaluation scales with the number of replicas rather than with the number of
// rules.
//
// A Sharder assigns each rule group to exactly one shard index. Replicas
// configured with the same shard count but distinct indices therefore partition
// the rule set between themselves with no coordination, no leader election and
// no shared state -- the assignment is a pure function of the group identity and
// the shard count, so every replica independently computes the same answer.
//
// Assignment uses rendezvous (highest-random-weight) hashing rather than a
// simple modulo so that changing the shard count only reassigns roughly
// 1/max(old,new) of the groups instead of nearly all of them.
//
// Note that a group is owned by exactly one shard: there is no replication. If
// the owning replica is down, its groups are not evaluated until it comes back
// or is rescheduled. See the package documentation in the README for the
// operational requirements (remote_write plus --rules.alertbackfill) that make
// this safe for alerting rules with a `for` duration.
package ruleshard

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/rules"
)

// Sharder decides which rule groups belong to the local replica.
//
// The zero value is not usable; construct one with New. A nil *Sharder owns
// everything, which makes it safe to use as a "sharding disabled" sentinel.
type Sharder struct {
	index int
	count int
}

// New builds a Sharder for the given shard index and total shard count.
//
// A count of 0 or 1 disables sharding; the returned Sharder owns every group
// and EvalIterationFunc returns nil so that the rules manager keeps its exact
// default behaviour.
func New(index, count int) (*Sharder, error) {
	if count < 0 {
		return nil, fmt.Errorf("rules shard count must not be negative, got %d", count)
	}
	if count <= 1 {
		// Sharding disabled. Tolerate any index so that operators can leave
		// --rules.shard-index set while scaling the ruler back down to one.
		return &Sharder{index: 0, count: 1}, nil
	}
	if index < 0 || index >= count {
		return nil, fmt.Errorf("rules shard index %d out of range for shard count %d (must be 0 <= index < count)", index, count)
	}
	return &Sharder{index: index, count: count}, nil
}

// Index returns the shard index of this replica.
func (s *Sharder) Index() int {
	if s == nil {
		return 0
	}
	return s.index
}

// Count returns the total number of shards.
func (s *Sharder) Count() int {
	if s == nil {
		return 1
	}
	return s.count
}

// Enabled reports whether sharding is actually partitioning anything.
func (s *Sharder) Enabled() bool { return s.Count() > 1 }

// Owns reports whether the local replica is responsible for evaluating the
// named group.
//
// Only the base name of the rule file participates in the hash. Rule files are
// supplied to the rules manager as absolute paths produced by glob expansion,
// and those paths can legitimately differ between replicas (different mount
// points, Kubernetes ConfigMap symlink indirection, operator-managed rule
// directories). Hashing the full path would make replicas silently disagree
// about ownership, which would produce both double-evaluated and orphaned
// groups. Hashing the base name keeps the decision stable. See
// CheckFileCollisions for the cost of that choice.
func (s *Sharder) Owns(file, group string) bool {
	if !s.Enabled() {
		return true
	}
	return s.shardFor(file, group) == s.index
}

// shardFor returns the owning shard index using rendezvous hashing: the group
// is assigned to the shard with the highest score, where the score mixes the
// group key with the shard index.
func (s *Sharder) shardFor(file, group string) int {
	// Hash the group once; the per-shard scores are derived from that digest so
	// that assignment costs one hash regardless of the shard count.
	h := xxhash.Sum64String(groupKey(file, group))

	best := 0
	bestScore := score(h, 0)
	for i := 1; i < s.count; i++ {
		// Ties are broken towards the lower index, which keeps the result
		// deterministic across replicas and architectures.
		if sc := score(h, i); sc > bestScore {
			best, bestScore = i, sc
		}
	}
	return best
}

// groupKey is the stable identity of a rule group across replicas.
func groupKey(file, group string) string {
	return filepath.Base(file) + ";" + group
}

// goldenGamma is the odd increment from SplitMix64, used to decorrelate the
// per-shard scores derived from a single group digest.
const goldenGamma = 0x9e3779b97f4a7c15

// score produces the rendezvous weight of a group (given its digest) for a
// particular shard index. The SplitMix64 finalizer gives us well-distributed,
// mutually independent scores per shard from one hash of the group key.
func score(groupDigest uint64, shard int) uint64 {
	z := groupDigest + uint64(shard+1)*goldenGamma
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// EvalIterationFunc returns the hook to pass to rules.Manager.Update.
//
// It returns nil when sharding is disabled so that the rules manager falls back
// to its own default and the unsharded code path stays byte-for-byte identical
// to upstream behaviour.
func (s *Sharder) EvalIterationFunc(m *Metrics) rules.GroupEvalIterationFunc {
	if !s.Enabled() {
		return nil
	}
	return func(ctx context.Context, g *rules.Group, evalTimestamp time.Time) {
		if !s.Owns(g.File(), g.Name()) {
			m.ObserveSkipped(g.File(), g.Name())
			return
		}
		rules.DefaultEvalIterationFunc(ctx, g, evalTimestamp)
	}
}

// CheckFileCollisions returns an error describing any rule files that share a
// base name, since Owns hashes only the base name and such files would be
// indistinguishable to the sharder.
//
// This is not fatal for correctness -- colliding files still hash consistently
// across replicas, so no group is ever double-owned or orphaned -- but it skews
// the distribution and is almost always an operator mistake, so the caller
// should surface it.
func CheckFileCollisions(files []string) error {
	seen := make(map[string]string, len(files))
	for _, f := range files {
		base := filepath.Base(f)
		if prev, ok := seen[base]; ok && prev != f {
			return fmt.Errorf("rule files %q and %q share the base name %q; rule sharding hashes base names, so these files are indistinguishable to the sharder", prev, f, base)
		}
		seen[base] = f
	}
	return nil
}
