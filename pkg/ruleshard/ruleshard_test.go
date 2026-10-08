package ruleshard

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func mustNew(t *testing.T, index, count int) *Sharder {
	t.Helper()
	s, err := New(index, count)
	if err != nil {
		t.Fatalf("New(%d, %d): %v", index, count, err)
	}
	return s
}

// shardSet builds the full set of sharders for a given count, as the replicas
// of a deployment would be.
func shardSet(t *testing.T, count int) []*Sharder {
	t.Helper()
	out := make([]*Sharder, count)
	for i := range out {
		out[i] = mustNew(t, i, count)
	}
	return out
}

func testGroups(n int) [][2]string {
	out := make([][2]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, [2]string{
			fmt.Sprintf("/etc/promxy/rules/file-%d.yaml", i%7),
			fmt.Sprintf("group-%d", i),
		})
	}
	return out
}

func TestNewValidation(t *testing.T) {
	for _, tc := range []struct {
		index, count int
		wantErr      bool
	}{
		{0, 1, false},
		{0, 0, false}, // count 0 means disabled
		{5, 1, false}, // index irrelevant when disabled
		{0, 3, false}, //
		{2, 3, false}, //
		{3, 3, true},  // index == count
		{-1, 3, true}, //
		{0, -1, true}, // negative count
	} {
		_, err := New(tc.index, tc.count)
		if gotErr := err != nil; gotErr != tc.wantErr {
			t.Errorf("New(%d, %d) err=%v, wantErr=%v", tc.index, tc.count, err, tc.wantErr)
		}
	}
}

func TestDisabledOwnsEverything(t *testing.T) {
	for _, s := range []*Sharder{nil, mustNew(t, 0, 1), mustNew(t, 0, 0)} {
		if s.Enabled() {
			t.Fatalf("sharder %+v should be disabled", s)
		}
		for _, g := range testGroups(50) {
			if !s.Owns(g[0], g[1]) {
				t.Fatalf("disabled sharder must own %v", g)
			}
		}
		if fn := s.EvalIterationFunc(nil); fn != nil {
			t.Error("disabled sharder must return a nil GroupEvalIterationFunc so the rules manager uses its default")
		}
	}
}

// TestPartition is the core invariant: across a full set of shards, every group
// is owned by exactly one.
func TestPartition(t *testing.T) {
	groups := testGroups(500)
	for _, count := range []int{2, 3, 5, 8, 17} {
		shards := shardSet(t, count)
		for _, g := range groups {
			owners := 0
			for _, s := range shards {
				if s.Owns(g[0], g[1]) {
					owners++
				}
			}
			if owners != 1 {
				t.Errorf("count=%d group=%v owned by %d shards, want exactly 1", count, g, owners)
			}
		}
	}
}

func TestDeterministic(t *testing.T) {
	a := mustNew(t, 3, 11)
	b := mustNew(t, 3, 11)
	for _, g := range testGroups(200) {
		if a.Owns(g[0], g[1]) != b.Owns(g[0], g[1]) {
			t.Fatalf("ownership of %v is not deterministic", g)
		}
	}
}

// TestOwnsIgnoresDirectory guards the decision to hash only the file base name:
// replicas can legitimately see the same rule file at different absolute paths.
func TestOwnsIgnoresDirectory(t *testing.T) {
	a := mustNew(t, 2, 6)
	for _, g := range testGroups(200) {
		base := filepath.Base(g[0])
		for _, dir := range []string{"/etc/promxy", "/mnt/configmap/..data", "relative/dir", ""} {
			if a.Owns(filepath.Join(dir, base), g[1]) != a.Owns(g[0], g[1]) {
				t.Fatalf("ownership of %v changed with directory %q", g, dir)
			}
		}
	}
}

// TestDistribution checks the assignment is not badly skewed. With rendezvous
// hashing over 2000 groups the per-shard share should sit close to 1/count.
func TestDistribution(t *testing.T) {
	groups := testGroups(2000)
	for _, count := range []int{2, 4, 8, 16} {
		counts := make([]int, count)
		for _, g := range groups {
			counts[mustNew(t, 0, count).shardFor(g[0], g[1])]++
		}
		expected := float64(len(groups)) / float64(count)
		for i, c := range counts {
			if dev := math.Abs(float64(c)-expected) / expected; dev > 0.25 {
				t.Errorf("count=%d shard=%d got %d groups, expected ~%.0f (%.0f%% deviation)", count, i, c, expected, dev*100)
			}
		}
	}
}

// TestReshardMovement is why we use rendezvous hashing instead of modulo:
// growing the shard count by one should move roughly 1/new of the groups, not
// almost all of them.
func TestReshardMovement(t *testing.T) {
	groups := testGroups(2000)
	for _, tc := range []struct{ from, to int }{{2, 3}, {3, 4}, {4, 5}, {8, 9}, {9, 8}} {
		moved := 0
		for _, g := range groups {
			if mustNew(t, 0, tc.from).shardFor(g[0], g[1]) != mustNew(t, 0, tc.to).shardFor(g[0], g[1]) {
				moved++
			}
		}
		ratio := float64(moved) / float64(len(groups))
		// The theoretical bound is 1/max(from,to); allow generous slack for
		// hashing noise while still failing loudly on a modulo-like reshuffle.
		bound := 1.0/float64(max(tc.from, tc.to)) + 0.1
		if ratio > bound {
			t.Errorf("resharding %d->%d moved %.1f%% of groups, want <= %.1f%%", tc.from, tc.to, ratio*100, bound*100)
		}
	}
}

func TestCheckFileCollisions(t *testing.T) {
	if err := CheckFileCollisions([]string{"/a/x.yaml", "/a/y.yaml", "/b/z.yaml"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// The same file listed twice (overlapping globs) is not a collision.
	if err := CheckFileCollisions([]string{"/a/x.yaml", "/a/x.yaml"}); err != nil {
		t.Errorf("unexpected error for duplicate path: %v", err)
	}
	if err := CheckFileCollisions([]string{"/a/x.yaml", "/b/x.yaml"}); err == nil {
		t.Error("expected a collision error for the same base name in two directories")
	}
}

func TestResolveIndex(t *testing.T) {
	for _, tc := range []struct {
		name    string
		flag    string
		count   int
		env     map[string]string
		want    int
		wantErr bool
	}{
		{name: "explicit", flag: "4", count: 8, want: 4},
		{name: "explicit beats env", flag: "4", count: 8, env: map[string]string{IndexEnvVar: "7"}, want: 4},
		{name: "env", flag: AutoIndex, count: 8, env: map[string]string{IndexEnvVar: "7"}, want: 7},
		{name: "env wins over pod name", flag: AutoIndex, count: 8, env: map[string]string{IndexEnvVar: "7", "POD_NAME": "promxy-2"}, want: 7},
		{name: "pod name ordinal", flag: AutoIndex, count: 8, env: map[string]string{"POD_NAME": "promxy-2"}, want: 2},
		{name: "pod name with dashes", flag: AutoIndex, count: 8, env: map[string]string{"POD_NAME": "promxy-ruler-ha-13"}, want: 13},
		{name: "pod name without ordinal", flag: AutoIndex, count: 8, env: map[string]string{"POD_NAME": "promxy-abc123"}, wantErr: true},
		{name: "bad env", flag: AutoIndex, count: 8, env: map[string]string{IndexEnvVar: "nope"}, wantErr: true},
		{name: "bad flag", flag: "nope", count: 8, wantErr: true},
		// Sharding disabled: the index is irrelevant and must not depend on the
		// environment, so a non-StatefulSet hostname must not be fatal.
		{name: "disabled auto", flag: AutoIndex, count: 1, env: map[string]string{"POD_NAME": "no-ordinal-here"}, want: 0},
		{name: "disabled bad flag", flag: "nope", count: 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Clear both inputs so the host environment cannot leak in.
			t.Setenv(IndexEnvVar, "")
			t.Setenv("POD_NAME", "")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			got, err := ResolveIndex(tc.flag, tc.count)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("ResolveIndex(%q, %d) err=%v, wantErr=%v", tc.flag, tc.count, err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Errorf("ResolveIndex(%q, %d) = %d, want %d", tc.flag, tc.count, got, tc.want)
			}
		})
	}
}

// TestResolveIndexFallsBackToHostname exercises the last resolution step. It
// only asserts consistency with the actual hostname, since that is environment
// dependent.
func TestResolveIndexFallsBackToHostname(t *testing.T) {
	t.Setenv(IndexEnvVar, "")
	t.Setenv("POD_NAME", "")

	hostname, err := os.Hostname()
	if err != nil {
		t.Skipf("cannot read hostname: %v", err)
	}
	want, ok := ordinalFrom(hostname)

	got, err := ResolveIndex(AutoIndex, 4)
	if !ok {
		if err == nil {
			t.Fatalf("hostname %q has no ordinal, expected an error rather than a silent default", hostname)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("ResolveIndex = %d, want %d (from hostname %q)", got, want, hostname)
	}
}

func TestOrdinalFrom(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int
		ok   bool
	}{
		{"promxy-0", 0, true},
		{"promxy-12", 12, true},
		{"a-b-c-7", 7, true},
		{"promxy", 0, false},
		{"promxy-", 0, false},
		{"promxy-1a", 0, false},
		{"", 0, false},
	} {
		got, ok := ordinalFrom(tc.in)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("ordinalFrom(%q) = (%d, %v), want (%d, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}
