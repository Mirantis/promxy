package servergroup

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
	"gopkg.in/yaml.v2"
)

// fakeProbeAPI is a queryAPI whose reachability can be flipped at will.
type fakeProbeAPI struct {
	mtx   sync.Mutex
	err   error
	calls int
	// block, when non-nil, is waited on before returning, to exercise the
	// probe timeout.
	block chan struct{}
}

func (f *fakeProbeAPI) setErr(err error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.err = err
}

func (f *fakeProbeAPI) Query(ctx context.Context, query string, ts time.Time) storage.SeriesSet {
	f.mtx.Lock()
	err := f.err
	f.calls++
	block := f.block
	f.mtx.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return storage.ErrSeriesSet(ctx.Err())
		}
	}
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return storage.EmptySeriesSet()
}

// collectAlerts installs a sink that records everything it receives, and
// returns a getter plus a cleanup to detach it.
func collectAlerts(t *testing.T) (func() []HealthAlert, func()) {
	t.Helper()
	var mtx sync.Mutex
	var got []HealthAlert
	SetHealthAlertSink(func(alerts ...HealthAlert) {
		mtx.Lock()
		defer mtx.Unlock()
		got = append(got, alerts...)
	})
	return func() []HealthAlert {
			mtx.Lock()
			defer mtx.Unlock()
			return append([]HealthAlert(nil), got...)
		}, func() {
			SetHealthAlertSink(nil)
		}
}

func durPtr(d time.Duration) *time.Duration { return &d }

func probeConfig(t *testing.T, yml string) *Config {
	t.Helper()
	cfg := &Config{}
	if err := yaml.Unmarshal([]byte(yml), cfg); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	return cfg
}

// TestHealthProbeConfigDefaults pins the defaults that the feature's safety
// depends on: it must be off unless asked for, and once on it must alert by
// default (an enabled probe that silently does nothing would be worse than no
// probe at all).
func TestHealthProbeConfigDefaults(t *testing.T) {
	cfg := probeConfig(t, `
static_configs:
  - targets: ["localhost:9090"]
`)
	if cfg.HealthProbe.Enabled {
		t.Fatal("health probe must be disabled unless explicitly enabled")
	}
	if cfg.HealthProbe.ShouldSendAlerts() {
		t.Fatal("a disabled probe must not send alerts")
	}

	cfg = probeConfig(t, `
static_configs:
  - targets: ["localhost:9090"]
health_probe:
  enabled: true
`)
	if got, want := cfg.HealthProbe.Interval, DefaultHealthProbeInterval; got != want {
		t.Errorf("interval = %v, want %v", got, want)
	}
	if got, want := cfg.HealthProbe.Timeout, DefaultHealthProbeTimeout; got != want {
		t.Errorf("timeout = %v, want %v", got, want)
	}
	if got, want := cfg.HealthProbe.Query, DefaultHealthProbeQuery; got != want {
		t.Errorf("query = %q, want %q", got, want)
	}
	if got, want := cfg.HealthProbe.AlertName, DefaultHealthProbeAlertName; got != want {
		t.Errorf("alert name = %q, want %q", got, want)
	}
	if !cfg.HealthProbe.ShouldSendAlerts() {
		t.Error("an enabled probe must send alerts by default")
	}
	// An omitted `for:` must get the flap-damping default, not 0. Getting this
	// wrong would make every probe alert on its first failed tick.
	if got, want := cfg.HealthProbe.ForDuration(), DefaultHealthProbeFor; got != want {
		t.Errorf("for = %v, want %v when omitted", got, want)
	}
}

// TestHealthProbeExplicitZeroFor pins the distinction between "omitted" and
// "explicitly 0": the latter is a legitimate request to alert immediately and
// must not be silently replaced by the default.
func TestHealthProbeExplicitZeroFor(t *testing.T) {
	cfg := probeConfig(t, `
static_configs:
  - targets: ["localhost:9090"]
health_probe:
  enabled: true
  for: 0
`)
	if got := cfg.HealthProbe.ForDuration(); got != 0 {
		t.Fatalf("for = %v, want 0 when set explicitly", got)
	}
}

// TestHealthProbeConfigSendAlertsOptOut verifies the explicit opt-out is
// distinguishable from "unset", which is why SendAlerts is a *bool.
func TestHealthProbeConfigSendAlertsOptOut(t *testing.T) {
	cfg := probeConfig(t, `
static_configs:
  - targets: ["localhost:9090"]
health_probe:
  enabled: true
  send_alerts: false
`)
	if cfg.HealthProbe.ShouldSendAlerts() {
		t.Fatal("send_alerts: false must be honoured")
	}
}

// TestHealthProbeConfigValidation covers the timing combinations that would
// otherwise produce a probe that silently behaves unlike its configuration.
func TestHealthProbeConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		yml     string
		wantErr string
	}{
		{
			name: "timeout at interval",
			yml: `
health_probe:
  enabled: true
  interval: 5s
  timeout: 5s
`,
			wantErr: "must be less than",
		},
		{
			name: "timeout above interval",
			yml: `
health_probe:
  enabled: true
  interval: 5s
  timeout: 30s
`,
			wantErr: "must be less than",
		},
		{
			name: "for below interval can never be satisfied",
			yml: `
health_probe:
  enabled: true
  interval: 30s
  timeout: 5s
  for: 10s
`,
			wantErr: "can never be satisfied",
		},
		{
			name: "disabled probe is not validated",
			yml: `
health_probe:
  enabled: false
  interval: 5s
  timeout: 30s
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			err := yaml.Unmarshal([]byte(tt.yml+"\nstatic_configs:\n  - targets: [\"localhost:9090\"]\n"), cfg)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

// newProbeFixture builds a prober over a single fake target.
func newProbeFixture(t *testing.T, cfg *Config) (*healthProber, *fakeProbeAPI, func() *ServerGroupState) {
	t.Helper()
	api := &fakeProbeAPI{}
	state := &ServerGroupState{
		Targets:      []string{"backend:443"},
		probeTargets: []probeTarget{{Host: "backend:443", API: api}},
	}
	return &healthProber{}, api, func() *ServerGroupState { return state }
}

// TestHealthProbeDetectsUnreachableBackend is the core case: a backend that
// stops answering must drive server_group_up to 0 and raise an alert.
func TestHealthProbeDetectsUnreachableBackend(t *testing.T) {
	getAlerts, cleanup := collectAlerts(t)
	defer cleanup()

	cfg := &Config{
		Ordinal: 0,
		Name:    "regional-adopted",
		Labels:  model.LabelSet{"promxyCluster": "regional-adopted"},
	}
	cfg.HealthProbe = HealthProbeConfig{Enabled: true}
	cfg.HealthProbe.applyDefaults()
	// Fire on the first failed probe so the test does not have to wait.
	cfg.HealthProbe.For = durPtr(0)

	h, api, getState := newProbeFixture(t, cfg)
	getCfg := func() *Config { return cfg }
	ctx := context.Background()

	// Healthy to begin with.
	h.probeOnce(ctx, getState, getCfg, testLogger())
	if got := testutil.ToFloat64(serverGroupUp.WithLabelValues("0", "regional-adopted")); got != 1 {
		t.Fatalf("server_group_up = %v, want 1 while healthy", got)
	}
	if len(getAlerts()) != 0 {
		t.Fatalf("no alert expected while healthy, got %d", len(getAlerts()))
	}

	// Partition the backend.
	api.setErr(fmt.Errorf("dial tcp 172.18.0.7:443: i/o timeout"))
	h.probeOnce(ctx, getState, getCfg, testLogger())

	if got := testutil.ToFloat64(serverGroupUp.WithLabelValues("0", "regional-adopted")); got != 0 {
		t.Errorf("server_group_up = %v, want 0 while unreachable", got)
	}
	if got := testutil.ToFloat64(serverGroupTargetUp.WithLabelValues("0", "regional-adopted", "backend:443")); got != 0 {
		t.Errorf("server_group_target_up = %v, want 0", got)
	}

	alerts := getAlerts()
	if len(alerts) != 1 {
		t.Fatalf("got %d alerts, want 1", len(alerts))
	}
	a := alerts[0]
	if got, want := a.Labels[alertLabelName], model.LabelValue(DefaultHealthProbeAlertName); got != want {
		t.Errorf("alertname = %q, want %q", got, want)
	}
	// The server group's own labels must ride along, so the alert can be
	// attributed to a specific region downstream.
	if got, want := a.Labels["promxyCluster"], model.LabelValue("regional-adopted"); got != want {
		t.Errorf("promxyCluster = %q, want %q", got, want)
	}
	if got, want := a.Labels[alertLabelServerGroup], model.LabelValue("regional-adopted"); got != want {
		t.Errorf("server_group = %q, want %q", got, want)
	}
	if !a.EndsAt.IsZero() {
		t.Error("a firing alert must not carry EndsAt")
	}
	if !strings.Contains(string(a.Annotations["description"]), "i/o timeout") {
		t.Errorf("description should carry the underlying error, got %q", a.Annotations["description"])
	}
}

// TestHealthProbeResolvesOnRecovery verifies the alert is explicitly resolved
// rather than being left to expire via Alertmanager's resolve_timeout.
func TestHealthProbeResolvesOnRecovery(t *testing.T) {
	getAlerts, cleanup := collectAlerts(t)
	defer cleanup()

	cfg := &Config{Ordinal: 3, Name: "recovery-group"}
	cfg.HealthProbe = HealthProbeConfig{Enabled: true}
	cfg.HealthProbe.applyDefaults()
	cfg.HealthProbe.For = durPtr(0)

	h, api, getState := newProbeFixture(t, cfg)
	getCfg := func() *Config { return cfg }
	ctx := context.Background()

	api.setErr(fmt.Errorf("connection refused"))
	h.probeOnce(ctx, getState, getCfg, testLogger())
	if len(getAlerts()) != 1 {
		t.Fatalf("expected a firing alert, got %d", len(getAlerts()))
	}

	api.setErr(nil)
	h.probeOnce(ctx, getState, getCfg, testLogger())

	alerts := getAlerts()
	if len(alerts) != 2 {
		t.Fatalf("expected a resolve alert, got %d total", len(alerts))
	}
	if alerts[1].EndsAt.IsZero() {
		t.Error("resolve alert must carry EndsAt")
	}
	if got := testutil.ToFloat64(serverGroupUp.WithLabelValues("3", "recovery-group")); got != 1 {
		t.Errorf("server_group_up = %v, want 1 after recovery", got)
	}

	// A second healthy probe must not emit anything further.
	h.probeOnce(ctx, getState, getCfg, testLogger())
	if len(getAlerts()) != 2 {
		t.Errorf("healthy probes must not emit alerts, got %d", len(getAlerts()))
	}
}

// TestHealthProbeHonoursFor ensures a single failed probe does not alert when
// `for` is set -- the damping that keeps a blip from paging someone.
func TestHealthProbeHonoursFor(t *testing.T) {
	getAlerts, cleanup := collectAlerts(t)
	defer cleanup()

	cfg := &Config{Ordinal: 7, Name: "damped"}
	cfg.HealthProbe = HealthProbeConfig{Enabled: true}
	cfg.HealthProbe.applyDefaults()
	cfg.HealthProbe.For = durPtr(time.Hour)

	h, api, getState := newProbeFixture(t, cfg)
	getCfg := func() *Config { return cfg }
	ctx := context.Background()

	api.setErr(fmt.Errorf("boom"))
	h.probeOnce(ctx, getState, getCfg, testLogger())
	h.probeOnce(ctx, getState, getCfg, testLogger())

	if n := len(getAlerts()); n != 0 {
		t.Fatalf("expected no alert before `for` elapses, got %d", n)
	}
	// The metric must still report the outage immediately, even though the
	// alert is damped.
	if got := testutil.ToFloat64(serverGroupUp.WithLabelValues("7", "damped")); got != 0 {
		t.Errorf("server_group_up = %v, want 0 (metric must not be damped)", got)
	}

	// Backdate the outage past `for`; the next probe should fire.
	h.mtx.Lock()
	h.downSince = time.Now().Add(-2 * time.Hour)
	h.mtx.Unlock()
	h.probeOnce(ctx, getState, getCfg, testLogger())
	if n := len(getAlerts()); n != 1 {
		t.Fatalf("expected 1 alert once `for` elapsed, got %d", n)
	}
}

// TestHealthProbeResendDamping checks a still-firing alert is refreshed
// periodically (so Alertmanager does not expire it) but not on every tick.
func TestHealthProbeResendDamping(t *testing.T) {
	getAlerts, cleanup := collectAlerts(t)
	defer cleanup()

	cfg := &Config{Ordinal: 9, Name: "resend"}
	cfg.HealthProbe = HealthProbeConfig{Enabled: true}
	cfg.HealthProbe.applyDefaults()
	cfg.HealthProbe.For = durPtr(0)

	h, api, getState := newProbeFixture(t, cfg)
	getCfg := func() *Config { return cfg }
	ctx := context.Background()

	api.setErr(fmt.Errorf("down"))
	h.probeOnce(ctx, getState, getCfg, testLogger())
	h.probeOnce(ctx, getState, getCfg, testLogger())
	h.probeOnce(ctx, getState, getCfg, testLogger())
	if n := len(getAlerts()); n != 1 {
		t.Fatalf("expected re-send damping to collapse to 1 alert, got %d", n)
	}

	// Backdate the last send past the resend interval.
	h.mtx.Lock()
	h.lastSent = time.Now().Add(-2 * healthResendInterval)
	h.mtx.Unlock()
	h.probeOnce(ctx, getState, getCfg, testLogger())
	if n := len(getAlerts()); n != 2 {
		t.Fatalf("expected a re-send after the interval, got %d", n)
	}
}

// TestHealthProbeZeroTargetsIsDown covers the case where discovery resolves to
// nothing: the group cannot serve a query, so it must count as down.
func TestHealthProbeZeroTargetsIsDown(t *testing.T) {
	getAlerts, cleanup := collectAlerts(t)
	defer cleanup()

	cfg := &Config{Ordinal: 11, Name: "empty"}
	cfg.HealthProbe = HealthProbeConfig{Enabled: true}
	cfg.HealthProbe.applyDefaults()
	cfg.HealthProbe.For = durPtr(0)

	h := &healthProber{}
	state := &ServerGroupState{}
	getState := func() *ServerGroupState { return state }
	getCfg := func() *Config { return cfg }

	h.probeOnce(context.Background(), getState, getCfg, testLogger())

	if got := testutil.ToFloat64(serverGroupUp.WithLabelValues("11", "empty")); got != 0 {
		t.Errorf("server_group_up = %v, want 0 for a group with no targets", got)
	}
	alerts := getAlerts()
	if len(alerts) != 1 {
		t.Fatalf("expected an alert for a zero-target group, got %d", len(alerts))
	}
	if !strings.Contains(string(alerts[0].Annotations["description"]), "no targets") {
		t.Errorf("description should explain the zero-target case, got %q", alerts[0].Annotations["description"])
	}
}

// TestHealthProbeNilStateIsUnknown ensures startup (before service discovery
// has produced anything) is not mistaken for an outage.
func TestHealthProbeNilStateIsUnknown(t *testing.T) {
	getAlerts, cleanup := collectAlerts(t)
	defer cleanup()

	cfg := &Config{Ordinal: 13, Name: "starting"}
	cfg.HealthProbe = HealthProbeConfig{Enabled: true}
	cfg.HealthProbe.applyDefaults()
	cfg.HealthProbe.For = durPtr(0)

	h := &healthProber{}
	h.probeOnce(
		context.Background(),
		func() *ServerGroupState { return nil },
		func() *Config { return cfg },
		testLogger(),
	)

	if n := len(getAlerts()); n != 0 {
		t.Fatalf("pre-discovery state must not alert, got %d", n)
	}
	h.mtx.Lock()
	defer h.mtx.Unlock()
	if !h.downSince.IsZero() {
		t.Error("pre-discovery state must not start the outage timer")
	}
}

// TestHealthProbePartialGroupStaysUp verifies one dead replica does not
// declare the whole group down -- promxy can still serve from the survivor.
func TestHealthProbePartialGroupStaysUp(t *testing.T) {
	getAlerts, cleanup := collectAlerts(t)
	defer cleanup()

	cfg := &Config{Ordinal: 15, Name: "ha-pair"}
	cfg.HealthProbe = HealthProbeConfig{Enabled: true}
	cfg.HealthProbe.applyDefaults()
	cfg.HealthProbe.For = durPtr(0)

	dead := &fakeProbeAPI{err: fmt.Errorf("refused")}
	alive := &fakeProbeAPI{}
	state := &ServerGroupState{
		Targets: []string{"a:443", "b:443"},
		probeTargets: []probeTarget{
			{Host: "a:443", API: dead},
			{Host: "b:443", API: alive},
		},
	}

	h := &healthProber{}
	h.probeOnce(
		context.Background(),
		func() *ServerGroupState { return state },
		func() *Config { return cfg },
		testLogger(),
	)

	if got := testutil.ToFloat64(serverGroupUp.WithLabelValues("15", "ha-pair")); got != 1 {
		t.Errorf("server_group_up = %v, want 1 with one surviving target", got)
	}
	if got := testutil.ToFloat64(serverGroupTargetsUp.WithLabelValues("15", "ha-pair")); got != 1 {
		t.Errorf("server_group_targets_up = %v, want 1", got)
	}
	if got := testutil.ToFloat64(serverGroupTargetUp.WithLabelValues("15", "ha-pair", "a:443")); got != 0 {
		t.Errorf("dead target up = %v, want 0", got)
	}
	if n := len(getAlerts()); n != 0 {
		t.Errorf("a partially-up group must not alert, got %d", n)
	}
}

// TestHealthProbeTimeout ensures a hung backend is reported as down rather
// than hanging the probe loop forever.
func TestHealthProbeTimeout(t *testing.T) {
	cfg := &Config{Ordinal: 17, Name: "hung"}
	cfg.HealthProbe = HealthProbeConfig{Enabled: true}
	cfg.HealthProbe.applyDefaults()
	cfg.HealthProbe.Timeout = 50 * time.Millisecond
	cfg.HealthProbe.For = durPtr(0)

	// A blocking API that is never released simulates a black-holed route,
	// which is what an iptables DROP looks like to a client.
	api := &fakeProbeAPI{block: make(chan struct{})}
	state := &ServerGroupState{
		Targets:      []string{"hung:443"},
		probeTargets: []probeTarget{{Host: "hung:443", API: api}},
	}

	h := &healthProber{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.probeOnce(
			context.Background(),
			func() *ServerGroupState { return state },
			func() *Config { return cfg },
			testLogger(),
		)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("probe did not honour its timeout")
	}

	if got := testutil.ToFloat64(serverGroupUp.WithLabelValues("17", "hung")); got != 0 {
		t.Errorf("server_group_up = %v, want 0 for a hung backend", got)
	}
}

// TestHealthProbeDropsStaleTargetMetrics ensures a target removed by service
// discovery does not leave a frozen series behind reporting it as down.
func TestHealthProbeDropsStaleTargetMetrics(t *testing.T) {
	cleanupSink := func() { SetHealthAlertSink(nil) }
	defer cleanupSink()

	cfg := &Config{Ordinal: 19, Name: "churn"}
	cfg.HealthProbe = HealthProbeConfig{Enabled: true}
	cfg.HealthProbe.applyDefaults()
	cfg.HealthProbe.For = durPtr(time.Hour) // keep alerts out of this test

	old := &fakeProbeAPI{err: fmt.Errorf("gone")}
	state := &ServerGroupState{
		Targets:      []string{"old:443"},
		probeTargets: []probeTarget{{Host: "old:443", API: old}},
	}
	getState := func() *ServerGroupState { return state }
	getCfg := func() *Config { return cfg }

	h := &healthProber{}
	h.probeOnce(context.Background(), getState, getCfg, testLogger())

	if got := testutil.ToFloat64(serverGroupTargetUp.WithLabelValues("19", "churn", "old:443")); got != 0 {
		t.Fatalf("old target up = %v, want 0", got)
	}
	before := testutil.CollectAndCount(serverGroupTargetUp)

	// Service discovery replaces the target.
	state = &ServerGroupState{
		Targets:      []string{"new:443"},
		probeTargets: []probeTarget{{Host: "new:443", API: &fakeProbeAPI{}}},
	}
	h.probeOnce(context.Background(), getState, getCfg, testLogger())

	if got := testutil.ToFloat64(serverGroupTargetUp.WithLabelValues("19", "churn", "new:443")); got != 1 {
		t.Errorf("new target up = %v, want 1", got)
	}
	// The departed target's series must be deleted, not frozen at 0 forever:
	// one series went away and one appeared, so the total is unchanged.
	if after := testutil.CollectAndCount(serverGroupTargetUp); after != before {
		t.Errorf("series count = %d, want %d (stale target series was not deleted)", after, before)
	}
}

// TestHealthProbeDisabledDoesNothing guards the opt-in guarantee.
func TestHealthProbeDisabledDoesNothing(t *testing.T) {
	getAlerts, cleanup := collectAlerts(t)
	defer cleanup()

	cfg := &Config{Ordinal: 21, Name: "off"}
	api := &fakeProbeAPI{err: fmt.Errorf("down")}
	state := &ServerGroupState{
		Targets:      []string{"x:443"},
		probeTargets: []probeTarget{{Host: "x:443", API: api}},
	}

	h := &healthProber{}
	h.probeOnce(
		context.Background(),
		func() *ServerGroupState { return state },
		func() *Config { return cfg },
		testLogger(),
	)

	if n := len(getAlerts()); n != 0 {
		t.Fatalf("a disabled probe must not alert, got %d", n)
	}
	api.mtx.Lock()
	defer api.mtx.Unlock()
	if api.calls != 0 {
		t.Fatalf("a disabled probe must not query the backend, got %d calls", api.calls)
	}
}

// TestHealthProbeAlertLabelsCannotMaskIdentity ensures operator-supplied alert
// labels cannot overwrite the labels that identify which group is down.
func TestHealthProbeAlertLabelsCannotMaskIdentity(t *testing.T) {
	cfg := &Config{Ordinal: 23, Name: "real-name"}
	cfg.HealthProbe = HealthProbeConfig{
		Enabled: true,
		AlertLabels: map[string]string{
			"severity":            "critical",
			alertLabelServerGroup: "a-lie",
			alertLabelName:        "NotTheRealAlert",
		},
	}
	cfg.HealthProbe.applyDefaults()

	h := &healthProber{}
	a := h.buildAlert(cfg, 1, 0, []string{"x:443: boom"})

	if got, want := a.Labels[alertLabelServerGroup], model.LabelValue("real-name"); got != want {
		t.Errorf("server_group = %q, want %q (must not be overridable)", got, want)
	}
	if got, want := a.Labels[alertLabelName], model.LabelValue(DefaultHealthProbeAlertName); got != want {
		t.Errorf("alertname = %q, want %q (must not be overridable)", got, want)
	}
	if got, want := a.Labels["severity"], model.LabelValue("critical"); got != want {
		t.Errorf("severity = %q, want %q (operator labels must pass through)", got, want)
	}
}

// TestHealthProbeAlertAnnotations covers the annotation passthrough used for
// runbook and dashboard links, and ensures the probe's own diagnostic
// annotations cannot be displaced by them.
func TestHealthProbeAlertAnnotations(t *testing.T) {
	cfg := &Config{Ordinal: 25, Name: "annotated"}
	cfg.HealthProbe = HealthProbeConfig{
		Enabled: true,
		AlertAnnotations: map[string]string{
			"Dashboard": "https://grafana.example.com/d/o11y-victoria-metrics-cluster?var-cluster=annotated",
			"runbook":   "https://docs.example.com/region-down",
			// Must not win over the probe's own diagnostic text.
			"description": "overwritten",
			"summary":     "overwritten",
		},
	}
	cfg.HealthProbe.applyDefaults()

	h := &healthProber{}
	a := h.buildAlert(cfg, 1, 0, []string{"x:443: boom"})

	if got, want := a.Annotations["Dashboard"], model.LabelValue("https://grafana.example.com/d/o11y-victoria-metrics-cluster?var-cluster=annotated"); got != want {
		t.Errorf("Dashboard = %q, want %q", got, want)
	}
	if got, want := a.Annotations["runbook"], model.LabelValue("https://docs.example.com/region-down"); got != want {
		t.Errorf("runbook = %q, want %q", got, want)
	}
	if got := string(a.Annotations["description"]); !strings.Contains(got, "could not reach any target") {
		t.Errorf("description must keep the probe's diagnostic text, got %q", got)
	}
	if got := string(a.Annotations["summary"]); !strings.Contains(got, "is unreachable") {
		t.Errorf("summary must keep the probe's text, got %q", got)
	}
}

// TestSetHealthAlertSinkNilDetaches ensures detaching the sink is safe and
// stops delivery (a reload must not panic mid-probe).
func TestSetHealthAlertSinkNilDetaches(t *testing.T) {
	var called int
	SetHealthAlertSink(func(alerts ...HealthAlert) { called++ })
	sendHealthAlerts(HealthAlert{})
	if called != 1 {
		t.Fatalf("sink not invoked, called = %d", called)
	}
	SetHealthAlertSink(nil)
	sendHealthAlerts(HealthAlert{})
	if called != 1 {
		t.Fatalf("detached sink still invoked, called = %d", called)
	}
}
