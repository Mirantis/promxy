package ruleshard

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/rules"
)

// Metrics exposes the shard assignment so that operators can verify that every
// rule group is owned by exactly one replica.
//
// The ownership gauge is intentionally emitted for *every* known group, not
// just the owned ones, so that summing it across replicas yields 1 per group. A
// group summing to 0 means it is being evaluated nowhere.
//
// The rule_group_file label carries the sharder's file identity (see
// FileIdentity), not the raw path. Replicas may mount the same rule files at
// different paths, and labelling with the raw path would split one logical
// group into several label sets, breaking the cross-replica sum above.
type Metrics struct {
	index   prometheus.Gauge
	count   prometheus.Gauge
	owned   *prometheus.GaugeVec
	skipped *prometheus.CounterVec
	groups  prometheus.Gauge
}

// NewMetrics registers the sharding metrics. A nil *Metrics is safe to use and
// records nothing, which keeps tests and the sharding-disabled path simple.
func NewMetrics(r prometheus.Registerer) *Metrics {
	m := &Metrics{
		index: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "promxy_rules_shard_index",
			Help: "The rule-evaluation shard index of this promxy replica.",
		}),
		count: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "promxy_rules_shard_count",
			Help: "The total number of rule-evaluation shards promxy is configured with. 1 means sharding is disabled.",
		}),
		owned: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "promxy_rule_group_shard_owned",
			Help: "Whether this promxy replica is responsible for evaluating a rule group. The rule_group_file label is the rule file base name, which is the identity the sharder hashes. Summed across all replicas this should be exactly 1 for every group.",
		}, []string{"rule_group_file", "rule_group"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "promxy_rule_group_evaluations_skipped_total",
			Help: "Total number of rule group evaluation iterations skipped because the group belongs to another shard. The rule_group_file label is the rule file base name, which is the identity the sharder hashes.",
		}, []string{"rule_group_file", "rule_group"}),
		groups: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "promxy_rule_groups_owned",
			Help: "Number of rule groups this promxy replica is responsible for evaluating.",
		}),
	}
	if r != nil {
		r.MustRegister(m.index, m.count, m.owned, m.skipped, m.groups)
	}
	return m
}

// SetShard records the static shard identity of this replica.
func (m *Metrics) SetShard(s *Sharder) {
	if m == nil {
		return
	}
	m.index.Set(float64(s.Index()))
	m.count.Set(float64(s.Count()))
}

// ObserveSkipped records that an evaluation iteration was skipped.
func (m *Metrics) ObserveSkipped(file, group string) {
	if m == nil {
		return
	}
	m.skipped.WithLabelValues(FileIdentity(file), group).Inc()
}

// SyncGroups republishes the ownership gauges for the currently loaded rule
// groups and returns the number owned locally.
//
// It resets the gauge vector first so that groups removed from the config stop
// being reported; stale series here would make the "owned by exactly one
// replica" invariant unverifiable.
func (m *Metrics) SyncGroups(s *Sharder, groups []*rules.Group) int {
	owned := 0
	if m != nil {
		m.owned.Reset()
	}
	for _, g := range groups {
		isOwned := s.Owns(g.File(), g.Name())
		if isOwned {
			owned++
		}
		if m != nil {
			v := 0.0
			if isOwned {
				v = 1.0
			}
			m.owned.WithLabelValues(FileIdentity(g.File()), g.Name()).Set(v)
		}
	}
	if m != nil {
		m.groups.Set(float64(owned))
	}
	return owned
}
