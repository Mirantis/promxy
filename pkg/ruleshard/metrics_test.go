package ruleshard

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/rules"
)

func newTestGroup(t *testing.T, file, name string) *rules.Group {
	t.Helper()
	return rules.NewGroup(rules.GroupOptions{
		Name: name,
		File: file,
		Opts: &rules.ManagerOptions{Registerer: prometheus.NewRegistry()},
	})
}

func gaugeValue(t *testing.T, c prometheus.Collector, want map[string]string) float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	c.Collect(ch)
	close(ch)

	for m := range ch {
		var pb dto.Metric
		if err := m.Write(&pb); err != nil {
			t.Fatalf("writing metric: %v", err)
		}
		match := true
		for _, lp := range pb.GetLabel() {
			if v, ok := want[lp.GetName()]; ok && v != lp.GetValue() {
				match = false
				break
			}
		}
		if !match || len(pb.GetLabel()) != len(want) {
			continue
		}
		if pb.Gauge != nil {
			return pb.Gauge.GetValue()
		}
		if pb.Counter != nil {
			return pb.Counter.GetValue()
		}
	}
	t.Fatalf("no metric found matching %v", want)
	return 0
}

// TestEvalIterationFuncSkipsUnowned verifies the hook short-circuits before
// touching the group for non-owned groups, and counts the skip.
func TestEvalIterationFuncSkipsUnowned(t *testing.T) {
	const count = 4
	m := NewMetrics(prometheus.NewRegistry())

	groups := make([]*rules.Group, 0, 40)
	for _, g := range testGroups(40) {
		groups = append(groups, newTestGroup(t, g[0], g[1]))
	}

	for idx := 0; idx < count; idx++ {
		s := mustNew(t, idx, count)
		fn := s.EvalIterationFunc(m)
		if fn == nil {
			t.Fatal("expected a non-nil eval func when sharding is enabled")
		}
		for _, g := range groups {
			if s.Owns(g.File(), g.Name()) {
				// Skip owned groups: DefaultEvalIterationFunc would need a
				// fully wired manager. Ownership itself is covered elsewhere.
				continue
			}
			// Must not panic despite the group having no queryable/appendable,
			// which proves we returned before delegating upstream.
			fn(context.Background(), g, time.Now())
		}
	}

	// Each group is unowned by exactly count-1 shards.
	for _, g := range groups {
		got := gaugeValue(t, m.skipped, map[string]string{
			"rule_group_file": FileIdentity(g.File()),
			"rule_group":      g.Name(),
		})
		if got != float64(count-1) {
			t.Errorf("group %s/%s skipped %v times, want %d", g.File(), g.Name(), got, count-1)
		}
	}
}

func TestSyncGroups(t *testing.T) {
	const count = 3
	groups := make([]*rules.Group, 0, 60)
	for _, g := range testGroups(60) {
		groups = append(groups, newTestGroup(t, g[0], g[1]))
	}

	totalOwned := 0
	for idx := 0; idx < count; idx++ {
		m := NewMetrics(prometheus.NewRegistry())
		s := mustNew(t, idx, count)
		owned := m.SyncGroups(s, groups)
		totalOwned += owned

		// The gauge must be published for every group, owned or not, so that
		// summing across replicas can prove each group has exactly one owner.
		sum := 0.0
		for _, g := range groups {
			sum += gaugeValue(t, m.owned, map[string]string{
				"rule_group_file": FileIdentity(g.File()),
				"rule_group":      g.Name(),
			})
		}
		if sum != float64(owned) {
			t.Errorf("shard %d: ownership gauges sum to %v, want %d", idx, sum, owned)
		}
	}

	if totalOwned != len(groups) {
		t.Errorf("shards collectively own %d groups, want %d", totalOwned, len(groups))
	}
}

// TestSyncGroupsResets ensures groups removed from the config stop being
// reported, otherwise stale series break the "exactly one owner" invariant.
func TestSyncGroupsResets(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	s := mustNew(t, 0, 1)

	a := newTestGroup(t, "/rules/a.yaml", "g1")
	b := newTestGroup(t, "/rules/b.yaml", "g2")

	m.SyncGroups(s, []*rules.Group{a, b})
	m.SyncGroups(s, []*rules.Group{a})

	ch := make(chan prometheus.Metric, 16)
	m.owned.Collect(ch)
	close(ch)
	if n := len(ch); n != 1 {
		t.Errorf("got %d ownership series after removing a group, want 1", n)
	}
}

// TestMetricLabelsArePathIndependent pins the invariant that the exported
// labels use the same identity the sharder hashes. Replicas may mount the same
// rule files at different paths; if the labels carried the raw path, summing
// promxy_rule_group_shard_owned across replicas would not yield 1 per group.
func TestMetricLabelsArePathIndependent(t *testing.T) {
	s := mustNew(t, 0, 3)

	a := NewMetrics(prometheus.NewRegistry())
	b := NewMetrics(prometheus.NewRegistry())

	a.SyncGroups(s, []*rules.Group{newTestGroup(t, "/etc/promxy/rules/a.yaml", "g1")})
	b.SyncGroups(s, []*rules.Group{newTestGroup(t, "/var/run/configmap/..data/a.yaml", "g1")})

	want := map[string]string{"rule_group_file": "a.yaml", "rule_group": "g1"}
	if av, bv := gaugeValue(t, a.owned, want), gaugeValue(t, b.owned, want); av != bv {
		t.Errorf("same group at different paths produced %v and %v", av, bv)
	}

	a.ObserveSkipped("/etc/promxy/rules/a.yaml", "g1")
	b.ObserveSkipped("/var/run/configmap/..data/a.yaml", "g1")
	if av, bv := gaugeValue(t, a.skipped, want), gaugeValue(t, b.skipped, want); av != 1 || bv != 1 {
		t.Errorf("skip counters diverged by path: %v and %v", av, bv)
	}
}

// TestNilMetrics documents that a nil *Metrics is usable.
func TestNilMetrics(t *testing.T) {
	var m *Metrics
	s := mustNew(t, 1, 3)
	m.SetShard(s)
	m.ObserveSkipped("f", "g")
	g := newTestGroup(t, "/rules/a.yaml", "g1")
	if owned := m.SyncGroups(s, []*rules.Group{g}); owned != boolToInt(s.Owns(g.File(), g.Name())) {
		t.Error("nil Metrics must still compute the owned count")
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
