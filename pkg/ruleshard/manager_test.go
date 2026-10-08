package ruleshard

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
)

// TestManagerOnlyEvaluatesOwnedGroups wires real rules.Managers -- one per
// shard -- against a shared rule file and asserts that each group is actually
// queried by exactly one manager. This is the integration-level counterpart to
// TestPartition: it proves the GroupEvalIterationFunc is honoured end to end by
// the upstream manager, not just that Owns() partitions correctly.
func TestManagerOnlyEvaluatesOwnedGroups(t *testing.T) {
	const (
		shardCount = 3
		groupCount = 12
		interval   = 50 * time.Millisecond
	)

	dir := t.TempDir()
	file := filepath.Join(dir, "rules.yaml")

	content := "groups:\n"
	for i := 0; i < groupCount; i++ {
		// The expression is unique per group so that QueryFunc, which only sees
		// the expression, can attribute each evaluation back to its group.
		content += "  - name: group-" + strconv.Itoa(i) + "\n" +
			"    interval: 50ms\n" +
			"    rules:\n" +
			"      - alert: alert-" + strconv.Itoa(i) + "\n" +
			"        expr: vector(" + strconv.Itoa(i) + ")\n"
	}
	if err := os.WriteFile(file, []byte(content), 0o644); err != nil {
		t.Fatalf("writing rule file: %v", err)
	}

	var mtx sync.Mutex
	// evaluated[group] is the set of shard indices that queried that group.
	evaluated := map[string]map[int]struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	managers := make([]*rules.Manager, 0, shardCount)
	for idx := 0; idx < shardCount; idx++ {
		s := mustNew(t, idx, shardCount)
		shard := idx

		m := rules.NewManager(&rules.ManagerOptions{
			Context:    ctx,
			Appendable: noopAppendable{},
			Queryable:  noopQueryable{},
			Registerer: prometheus.NewRegistry(),
			QueryFunc: func(ctx context.Context, q string, ts time.Time) (promql.Vector, error) {
				mtx.Lock()
				defer mtx.Unlock()
				// The expression is unique per group, so it identifies it.
				if evaluated[q] == nil {
					evaluated[q] = map[int]struct{}{}
				}
				evaluated[q][shard] = struct{}{}
				return nil, nil
			},
			NotifyFunc: func(ctx context.Context, expr string, alerts ...*rules.Alert) {},
		})

		if err := m.Update(interval, []string{file}, labels.EmptyLabels(), "", s.EvalIterationFunc(nil)); err != nil {
			t.Fatalf("shard %d: Update: %v", idx, err)
		}
		go m.Run()
		managers = append(managers, m)
	}
	defer func() {
		for _, m := range managers {
			m.Stop()
		}
	}()

	// Give every group several evaluation intervals to fire.
	deadline := time.After(10 * time.Second)
	for {
		mtx.Lock()
		got := len(evaluated)
		mtx.Unlock()
		if got >= groupCount {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d of %d groups were evaluated before the deadline", got, groupCount)
		case <-time.After(interval):
		}
	}

	mtx.Lock()
	defer mtx.Unlock()
	for q, shards := range evaluated {
		if len(shards) != 1 {
			t.Errorf("query %q was evaluated by %d shards (%v), want exactly 1", q, len(shards), shards)
		}
	}
}

type noopAppendable struct{}

func (noopAppendable) Appender(context.Context) storage.Appender { return noopAppender{} }

type noopAppender struct{ storage.Appender }

func (noopAppender) Append(storage.SeriesRef, labels.Labels, int64, float64) (storage.SeriesRef, error) {
	return 0, nil
}
func (noopAppender) Commit() error   { return nil }
func (noopAppender) Rollback() error { return nil }

type noopQueryable struct{}

func (noopQueryable) Querier(int64, int64) (storage.Querier, error) { return noopQuerier{}, nil }

type noopQuerier struct{ storage.Querier }

func (noopQuerier) Select(context.Context, bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}
func (noopQuerier) Close() error { return nil }
