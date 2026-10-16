package servergroup

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
	"github.com/sirupsen/logrus"
)

// Label names used on the health metrics and on the raised alert.
const (
	healthLabelOrdinal     = "ordinal"
	healthLabelName        = "name"
	healthLabelTarget      = "target"
	alertLabelName         = "alertname"
	alertLabelServerGroup  = "server_group"
	alertLabelGroupOrdinal = "server_group_ordinal"
)

// healthResendInterval bounds how often a still-firing alert is re-sent to
// Alertmanager. Alertmanager expires an alert after its own resolve_timeout
// (5m by default) unless it is refreshed, so this must stay comfortably below
// that; it is a var so tests can shorten it.
var healthResendInterval = 1 * time.Minute

var (
	// serverGroupUp is the headline signal: 1 when at least one target in the
	// group answered its last probe, 0 when none did. This is the series to
	// alert on (`server_group_up == 0`) when promxy's metrics are scraped into
	// storage that survives the outage.
	serverGroupUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "server_group_up",
		Help: "Whether a server group has at least one reachable target (1) or none (0). Only reported for groups with health_probe enabled.",
	}, []string{healthLabelOrdinal, healthLabelName})

	// serverGroupTargetUp is the per-target breakdown behind serverGroupUp,
	// for telling "the whole region is gone" apart from "one replica is gone".
	serverGroupTargetUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "server_group_target_up",
		Help: "Whether an individual server group target answered its last health probe (1) or not (0).",
	}, []string{healthLabelOrdinal, healthLabelName, healthLabelTarget})

	// serverGroupTargetsUp counts reachable targets, so a partially degraded
	// group is visible without aggregating the per-target series.
	serverGroupTargetsUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "server_group_targets_up",
		Help: "Number of targets in a server group that answered their last health probe.",
	}, []string{healthLabelOrdinal, healthLabelName})

	// serverGroupProbeDuration records probe latency, which degrades before
	// reachability does and is therefore the earlier warning.
	serverGroupProbeDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "server_group_health_probe_duration_seconds",
		Help:    "Duration of health probes sent to server group targets.",
		Buckets: prometheus.DefBuckets,
	}, []string{healthLabelOrdinal, healthLabelName, healthLabelTarget})

	// serverGroupLastSuccess is the timestamp of the last successful probe,
	// which survives promxy restarts of the *remote* and lets a dashboard show
	// "unreachable for 12m" rather than just "unreachable".
	serverGroupLastSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "server_group_last_successful_probe_timestamp_seconds",
		Help: "Unix timestamp of the last successful health probe against any target in the server group.",
	}, []string{healthLabelOrdinal, healthLabelName})

	// serverGroupProbesTotal counts probes by outcome, giving a rate of
	// intermittent failures that the point-in-time up gauge cannot show.
	serverGroupProbesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "server_group_health_probes_total",
		Help: "Total health probes sent to server group targets, by result.",
	}, []string{healthLabelOrdinal, healthLabelName, healthLabelTarget, "result"})
)

func init() {
	prometheus.MustRegister(
		serverGroupUp,
		serverGroupTargetUp,
		serverGroupTargetsUp,
		serverGroupProbeDuration,
		serverGroupLastSuccess,
		serverGroupProbesTotal,
	)
}

// HealthAlert is a single alert raised by the health probe, in a form that is
// independent of the notifier implementation. cmd/promxy converts these into
// notifier.Alert values and hands them to the Alertmanager notifier.
type HealthAlert struct {
	Labels      model.LabelSet
	Annotations model.LabelSet
	StartsAt    time.Time
	// EndsAt is always set. While the alert is firing it is an expiry in the
	// future that comfortably covers the next probe round and re-send, so a
	// continuous outage cannot be expired by Alertmanager's resolve_timeout
	// (5m by default) just because the configured probe interval is longer
	// than that. On recovery it is set to the resolution time, which resolves
	// the alert explicitly rather than waiting for the expiry to lapse.
	EndsAt time.Time
}

// HealthAlertFunc receives alerts raised by server group health probes.
type HealthAlertFunc func(alerts ...HealthAlert)

// healthAlertSink is the process-wide destination for probe alerts. It is a
// package-level hook rather than a per-ServerGroup field because server groups
// are constructed deep inside config reloading, while the notifier that
// consumes these lives in main; threading it through every construction site
// would touch far more code than it is worth. It matches how this package
// already handles its Prometheus metrics.
var healthAlertSink atomic.Pointer[HealthAlertFunc]

// SetHealthAlertSink installs the destination for health probe alerts. Passing
// nil detaches the sink. Safe to call concurrently with running probes.
func SetHealthAlertSink(f HealthAlertFunc) {
	if f == nil {
		healthAlertSink.Store(nil)
		return
	}
	healthAlertSink.Store(&f)
}

func sendHealthAlerts(alerts ...HealthAlert) {
	if len(alerts) == 0 {
		return
	}
	if f := healthAlertSink.Load(); f != nil {
		(*f)(alerts...)
	}
}

// probeTarget pairs a target's host with the per-target API client used to
// probe it. The client is taken from before the group-level IgnoreError /
// DowngradeError wrappers are applied -- those exist to keep a failing backend
// from failing user queries, and honouring them here would blind the probe to
// exactly the failure it is meant to detect.
type probeTarget struct {
	Host string
	API  queryAPI
}

// queryAPI is the slice of promclient.API the probe uses. Errors on this path
// arrive as storage.ErrSeriesSet, so Err() is populated immediately on a
// transport, auth or downstream failure -- no iteration required.
type queryAPI interface {
	Query(ctx context.Context, query string, ts time.Time) storage.SeriesSet
}

// healthIdentity is the identity of the server group a prober speaks for. It
// is exactly the label set of the group-level health metrics, so two probers
// sharing an identity publish the same series and raise the same alert: on a
// configuration reload they must hand state over rather than compete.
type healthIdentity struct {
	ordinal int
	name    string
}

func identityForConfig(cfg *Config) healthIdentity {
	return healthIdentity{ordinal: cfg.Ordinal, name: cfg.Name}
}

func (id healthIdentity) ordinalString() string {
	return strconv.Itoa(id.ordinal)
}

// healthRegistry records which prober currently owns the published metrics and
// the alert state machine for each server group identity.
//
// ProxyStorage.ApplyConfig builds a complete new set of ServerGroups and only
// then cancels the old ones, so the replacement claims an identity before its
// predecessor shuts down. That ordering is what makes ownership work: the
// outgoing prober sees it is no longer the owner and neither deletes the
// replacement's series nor publishes over them, while the replacement inherits
// the outage state so an unchanged group keeps counting towards `for:` across
// reloads instead of restarting its state machine.
var healthRegistry = struct {
	mtx    sync.Mutex
	owners map[healthIdentity]*healthProber
}{owners: make(map[healthIdentity]*healthProber)}

// healthProber runs the periodic liveness probe for one server group and owns
// the alert state machine derived from it.
//
// A nil *healthProber is valid and inert, so a ServerGroup that never enables
// the probe carries no extra behaviour.
type healthProber struct {
	startOnce sync.Once

	// identity is set once, when the prober claims ownership in start, and
	// read without the lock afterwards. claimed distinguishes "owns the zero
	// identity" from "never claimed one".
	identity healthIdentity
	claimed  bool

	// mtx guards the alert state machine below. Only the probe goroutine
	// mutates it, but tests inspect it.
	mtx sync.Mutex
	// downSince is when the group was first observed down in the current
	// outage; zero when the group is up.
	downSince time.Time
	// firing records whether an alert is currently outstanding, so recovery
	// can send an explicit resolve exactly once.
	firing bool
	// lastSent is when the firing alert was last pushed, for re-send damping.
	lastSent time.Time
	// knownTargets is the target set reflected in the per-target metrics, so
	// series for targets that service discovery has dropped can be deleted
	// rather than left behind at a stale value forever.
	knownTargets map[string]struct{}
}

// claim makes h the owner of id's metrics and alert state, adopting whatever
// the previous owner had accumulated. Returns the predecessor, if any.
func (h *healthProber) claim(id healthIdentity) *healthProber {
	healthRegistry.mtx.Lock()
	prev := healthRegistry.owners[id]
	healthRegistry.owners[id] = h
	healthRegistry.mtx.Unlock()

	h.identity = id
	h.claimed = true
	if prev == nil || prev == h {
		return nil
	}

	// Snapshot the predecessor and copy it over. The two probers are locked
	// one at a time, never nested, so there is no ordering to get wrong.
	prev.mtx.Lock()
	downSince, firing, lastSent := prev.downSince, prev.firing, prev.lastSent
	knownTargets := prev.knownTargets
	prev.mtx.Unlock()

	h.mtx.Lock()
	h.downSince, h.firing, h.lastSent = downSince, firing, lastSent
	h.knownTargets = knownTargets
	h.mtx.Unlock()

	return prev
}

// isOwner reports whether h is still the prober responsible for its identity.
// A prober that has been superseded must stop publishing: its results describe
// a configuration that is no longer in effect, and its series are now written
// by its replacement.
func (h *healthProber) isOwner() bool {
	if !h.claimed {
		return true
	}
	healthRegistry.mtx.Lock()
	defer healthRegistry.mtx.Unlock()
	return healthRegistry.owners[h.identity] == h
}

// release is called when the prober's context is cancelled. If h is still the
// owner then the server group is genuinely going away (removed from the
// config, or promxy shutting down) and its series must be retired: a left
// behind `server_group_up 0` would keep metric-based outage alerts firing
// forever. If a replacement has already claimed the identity, h must leave
// those series alone -- they belong to the replacement now.
func (h *healthProber) release() {
	if !h.claimed {
		return
	}
	healthRegistry.mtx.Lock()
	stillOwner := healthRegistry.owners[h.identity] == h
	if stillOwner {
		delete(healthRegistry.owners, h.identity)
	}
	healthRegistry.mtx.Unlock()

	if !stillOwner {
		return
	}

	h.mtx.Lock()
	targets := h.knownTargets
	h.knownTargets = nil
	h.firing = false
	h.downSince = time.Time{}
	h.lastSent = time.Time{}
	h.mtx.Unlock()

	ordinal, name := h.identity.ordinalString(), h.identity.name
	for host := range targets {
		deleteTargetMetrics(ordinal, name, host)
	}
	serverGroupUp.DeleteLabelValues(ordinal, name)
	serverGroupTargetsUp.DeleteLabelValues(ordinal, name)
	serverGroupLastSuccess.DeleteLabelValues(ordinal, name)
}

func deleteTargetMetrics(ordinal, name, host string) {
	serverGroupTargetUp.DeleteLabelValues(ordinal, name, host)
	serverGroupProbeDuration.DeleteLabelValues(ordinal, name, host)
	serverGroupProbesTotal.DeleteLabelValues(ordinal, name, host, "success")
	serverGroupProbesTotal.DeleteLabelValues(ordinal, name, host, "failure")
}

// start launches the probe loop the first time it is called; later calls are
// no-ops, so repeated ApplyConfig cycles do not stack up goroutines. getState
// and getCfg are called on each tick because both are republished as service
// discovery and configuration change.
func (h *healthProber) start(ctx context.Context, getState func() *ServerGroupState, getCfg func() *Config, logger *logrus.Entry) {
	if h == nil {
		return
	}
	cfg := getCfg()
	if cfg == nil || !cfg.HealthProbe.Enabled {
		return
	}
	h.startOnce.Do(func() {
		// Claim ownership synchronously, before the goroutine starts: the
		// caller's ApplyConfig ordering is what guarantees the replacement
		// claims ahead of the predecessor's cancellation.
		h.claim(identityForConfig(cfg))
		go h.run(ctx, getState, getCfg, logger)
	})
}

func (h *healthProber) run(ctx context.Context, getState func() *ServerGroupState, getCfg func() *Config, logger *logrus.Entry) {
	defer h.release()

	cfg := getCfg()
	interval := probeInterval(cfg)

	t := time.NewTicker(interval)
	defer t.Stop()

	// Probe once immediately so a backend that is already down at startup is
	// reported within `for`, rather than within `for` + one interval.
	h.probeOnce(ctx, getState, getCfg, logger)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.probeOnce(ctx, getState, getCfg, logger)
		}
	}
}

// probeInterval and probeTimeout resolve the configured timings, falling back
// to the defaults for configs that were built in code rather than parsed from
// YAML (UnmarshalYAML is what normally applies the defaults).
func probeInterval(cfg *Config) time.Duration {
	if cfg == nil || cfg.HealthProbe.Interval <= 0 {
		return DefaultHealthProbeInterval
	}
	return cfg.HealthProbe.Interval
}

func probeTimeout(cfg *Config) time.Duration {
	if cfg == nil || cfg.HealthProbe.Timeout <= 0 {
		return DefaultHealthProbeTimeout
	}
	return cfg.HealthProbe.Timeout
}

// healthAlertExpiry is how long a firing alert stays valid in Alertmanager
// without being refreshed. Alerts are only re-sent after a probe round, so a
// long `interval` would otherwise let Alertmanager's resolve_timeout expire a
// still-true alert between rounds; the expiry therefore covers several full
// rounds. Prometheus does the same thing with 4 * ResendDelay.
func healthAlertExpiry(cfg *Config) time.Duration {
	refresh := healthResendInterval
	if round := probeInterval(cfg) + probeTimeout(cfg); round > refresh {
		refresh = round
	}
	return 4 * refresh
}

// probeOnce runs one round of probes across all targets and feeds the result
// into the metrics and the alert state machine.
func (h *healthProber) probeOnce(ctx context.Context, getState func() *ServerGroupState, getCfg func() *Config, logger *logrus.Entry) {
	cfg := getCfg()
	if cfg == nil || !cfg.HealthProbe.Enabled {
		return
	}

	state := getState()
	if state == nil {
		// Service discovery has not produced a target set yet. This is
		// "unknown", not "down" -- treating startup as an outage would alert
		// on every slow-resolving SD mechanism. Leave the state machine alone
		// and let the next tick decide.
		return
	}

	ordinal := strconv.Itoa(cfg.Ordinal)
	targets := state.probeTargets

	// A group whose discovery resolved to nothing cannot serve a query, so it
	// counts as down. This is distinct from the nil-state case above: here SD
	// has run and genuinely returned no targets.
	results := make([]probeResult, len(targets))
	var wg sync.WaitGroup
	for i := range targets {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = h.probeTarget(ctx, cfg, targets[i])
		}(i)
	}
	wg.Wait()

	// A reload cancels the old ServerGroup's context, which aborts in-flight
	// probes with context.Canceled even against a perfectly healthy backend.
	// Publishing that round would report a fabricated outage (and with
	// `for: 0`, send an alert) on the way out. Individual probe timeouts are
	// unaffected: those come from the per-probe deadline, not from ctx.
	//
	// The ownership check covers the same handover from the other side: once a
	// replacement has claimed this group, these series and this alert belong
	// to it.
	if ctx.Err() != nil || !h.isOwner() {
		return
	}

	upCount := 0
	seen := make(map[string]struct{}, len(results))
	var failures []string
	for _, r := range results {
		seen[r.Host] = struct{}{}
		result := "success"
		up := 0.0
		if r.Err == nil {
			upCount++
			up = 1
		} else {
			result = "failure"
			failures = append(failures, fmt.Sprintf("%s: %v", r.Host, r.Err))
		}
		serverGroupTargetUp.WithLabelValues(ordinal, cfg.Name, r.Host).Set(up)
		serverGroupProbeDuration.WithLabelValues(ordinal, cfg.Name, r.Host).Observe(r.Duration.Seconds())
		serverGroupProbesTotal.WithLabelValues(ordinal, cfg.Name, r.Host, result).Inc()
	}

	h.reconcileTargetMetrics(ordinal, cfg.Name, seen)

	serverGroupTargetsUp.WithLabelValues(ordinal, cfg.Name).Set(float64(upCount))
	groupUp := upCount > 0
	if groupUp {
		serverGroupUp.WithLabelValues(ordinal, cfg.Name).Set(1)
		serverGroupLastSuccess.WithLabelValues(ordinal, cfg.Name).Set(float64(time.Now().Unix()))
	} else {
		serverGroupUp.WithLabelValues(ordinal, cfg.Name).Set(0)
	}

	sort.Strings(failures)
	h.updateAlertState(cfg, groupUp, len(targets), upCount, failures, logger)
}

type probeResult struct {
	Host     string
	Err      error
	Duration time.Duration
}

func (h *healthProber) probeTarget(ctx context.Context, cfg *Config, t probeTarget) probeResult {
	query := cfg.HealthProbe.Query
	if query == "" {
		query = DefaultHealthProbeQuery
	}
	timeout := probeTimeout(cfg)

	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	var err error
	if t.API == nil {
		err = fmt.Errorf("no api client for target")
	} else {
		// A SeriesSet carries its error lazily; Err() is what actually
		// surfaces a transport, auth or downstream failure.
		err = t.API.Query(pctx, query, start).Err()
	}
	return probeResult{Host: t.Host, Err: err, Duration: time.Since(start)}
}

// reconcileTargetMetrics drops per-target series for targets that are no
// longer discovered. Without this, a scaled-down or renamed target keeps
// reporting its last value forever and can hold an alert on indefinitely.
func (h *healthProber) reconcileTargetMetrics(ordinal, name string, seen map[string]struct{}) {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	for host := range h.knownTargets {
		if _, ok := seen[host]; ok {
			continue
		}
		deleteTargetMetrics(ordinal, name, host)
	}
	h.knownTargets = seen
}

// updateAlertState advances the firing/resolved state machine and pushes to
// the alert sink. It implements the same semantics as an alerting rule's
// `for:` clause: a group must be continuously down for that long before the
// alert fires, and any single successful probe resets the timer.
func (h *healthProber) updateAlertState(cfg *Config, groupUp bool, total, upCount int, failures []string, logger *logrus.Entry) {
	now := time.Now()

	h.mtx.Lock()
	defer h.mtx.Unlock()

	if groupUp {
		if h.firing {
			// Explicitly resolve rather than letting Alertmanager time the
			// alert out, so recovery is reflected promptly.
			alert := h.buildAlert(cfg, total, upCount, failures)
			alert.StartsAt = h.downSince
			alert.EndsAt = now
			h.firing = false
			h.downSince = time.Time{}
			h.lastSent = time.Time{}
			logger.Infof("ServerGroup %s recovered; resolving %s", configIdentifier(cfg), cfg.HealthProbe.AlertName)
			// Send outside the common path below; state is already consistent.
			sendHealthAlerts(alert)
			return
		}
		h.downSince = time.Time{}
		return
	}

	if h.downSince.IsZero() {
		h.downSince = now
		logger.Warnf("ServerGroup %s health probe failed (%d/%d targets up): %v", configIdentifier(cfg), upCount, total, failures)
	}

	if !cfg.HealthProbe.ShouldSendAlerts() {
		return
	}

	// Honour `for:` before firing.
	if now.Sub(h.downSince) < cfg.HealthProbe.ForDuration() {
		return
	}

	// Re-send periodically so Alertmanager does not expire a still-true alert.
	if h.firing && now.Sub(h.lastSent) < healthResendInterval {
		return
	}

	alert := h.buildAlert(cfg, total, upCount, failures)
	alert.StartsAt = h.downSince
	// Carry an expiry far enough ahead that Alertmanager cannot resolve this
	// alert between probe rounds; recovery resolves it explicitly.
	alert.EndsAt = now.Add(healthAlertExpiry(cfg))
	if !h.firing {
		logger.Errorf("ServerGroup %s unreachable for %s; raising %s", configIdentifier(cfg), now.Sub(h.downSince).Truncate(time.Second), cfg.HealthProbe.AlertName)
	}
	h.firing = true
	h.lastSent = now
	sendHealthAlerts(alert)
}

// buildAlert assembles the alert for a down group. The server group's
// configured `labels` are included so downstream routing can identify which
// backend is affected using the same identity it carries on its data (in KOF,
// for example, the region's cluster name).
func (h *healthProber) buildAlert(cfg *Config, total, upCount int, failures []string) HealthAlert {
	lbls := model.LabelSet{}

	// Operator-supplied alert labels first, so the probe's own identity labels
	// below cannot be overridden into something misleading.
	for k, v := range cfg.HealthProbe.AlertLabels {
		if ln := model.LabelName(k); ln.IsValid() {
			lbls[ln] = model.LabelValue(v)
		}
	}
	for k, v := range cfg.Labels {
		lbls[k] = v
	}

	alertName := cfg.HealthProbe.AlertName
	if alertName == "" {
		alertName = DefaultHealthProbeAlertName
	}
	lbls[alertLabelName] = model.LabelValue(alertName)
	lbls[alertLabelGroupOrdinal] = model.LabelValue(strconv.Itoa(cfg.Ordinal))
	if cfg.Name != "" {
		lbls[alertLabelServerGroup] = model.LabelValue(cfg.Name)
	}

	reason := "service discovery returned no targets"
	if len(failures) > 0 {
		reason = fmt.Sprintf("%v", failures)
	}

	annotations := model.LabelSet{}

	// Operator-supplied annotations first, so the probe's own summary and
	// description -- which carry the diagnostic detail -- cannot be replaced.
	for k, v := range cfg.HealthProbe.AlertAnnotations {
		if ln := model.LabelName(k); ln.IsValid() {
			annotations[ln] = model.LabelValue(v)
		}
	}

	annotations["summary"] = model.LabelValue(fmt.Sprintf("promxy server group %q is unreachable", identityForAlert(cfg)))
	annotations["description"] = model.LabelValue(fmt.Sprintf(
		"promxy could not reach any target in server group %s (%d/%d targets up). Queries against this backend are returning empty results, so alerts derived from its data will appear to resolve rather than fire. Last error: %s",
		identityForAlert(cfg), upCount, total, reason,
	))

	return HealthAlert{Labels: lbls, Annotations: annotations}
}

// identityForAlert picks the most human-meaningful identifier available.
func identityForAlert(cfg *Config) string {
	if cfg.Name != "" {
		return cfg.Name
	}
	return configIdentifier(cfg)
}
