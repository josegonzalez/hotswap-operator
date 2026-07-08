/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package prober runs the per-pod HTTP health checks a HotSwapPolicy declares,
// tracks each pod's health via a threshold state machine, and notifies the
// controller (over a channel event source) whenever a pod's health transitions.
package prober

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/josegonzalez/hotswap-operator/internal/metrics"
)

// Clock abstracts time so tests are deterministic.
type Clock interface {
	Now() time.Time
}

// RealClock is the production Clock.
type RealClock struct{}

// Now returns the current time.
func (RealClock) Now() time.Time { return time.Now() }

// PodTarget identifies a pod to probe. UID is the stable key; IP may be filled
// in once the pod is scheduled.
type PodTarget struct {
	Name string
	UID  types.UID
	IP   string
}

// PodState is the exported health snapshot for a single pod.
type PodState struct {
	PodName              string
	PodUID               types.UID
	PodIP                string
	Healthy              bool
	ConsecutiveFailures  int32
	ConsecutiveSuccesses int32
	SeenReady            bool
	LastProbe            time.Time
	LastTransition       time.Time
}

// podEntry is the internal, mutable per-pod record.
type podEntry struct {
	PodState
	started bool
	cancel  context.CancelFunc
}

type policyEntry struct {
	cfg  ProbeConfig
	pods map[types.UID]*podEntry
}

// Manager owns all probe goroutines and the in-memory health store. It is a
// controller-runtime leader-election Runnable, so probing only happens on the
// elected leader (matching where reconciliation runs).
type Manager struct {
	checker Checker
	clock   Clock
	log     logr.Logger

	events chan event.GenericEvent

	root   context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	policies map[string]*policyEntry
}

// NewManager builds a prober Manager. The event channel is buffered; a dropped
// transition only delays remediation to the reconciler's periodic resync.
func NewManager(checker Checker, clock Clock, log logr.Logger) *Manager {
	root, cancel := context.WithCancel(context.Background())
	return &Manager{
		checker:  checker,
		clock:    clock,
		log:      log,
		events:   make(chan event.GenericEvent, 1024),
		root:     root,
		cancel:   cancel,
		policies: map[string]*policyEntry{},
	}
}

// Events is the source channel the controller watches; a transition enqueues a
// reconcile of the owning policy.
func (m *Manager) Events() <-chan event.GenericEvent { return m.events }

// Start blocks until ctx is cancelled, then stops every probe goroutine. It
// implements manager.Runnable.
func (m *Manager) Start(ctx context.Context) error {
	<-ctx.Done()
	m.cancel()
	return nil
}

// NeedLeaderElection ensures probes run only on the elected leader.
func (m *Manager) NeedLeaderElection() bool { return true }

func key(nn types.NamespacedName) string { return nn.String() }

// Sync reconciles the set of probed pods for a policy to match targets and
// updates the probe config. Probers for pods no longer present are stopped;
// probers for new pods are started.
func (m *Manager) Sync(nn types.NamespacedName, cfg ProbeConfig, targets []PodTarget) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := key(nn)
	pe := m.policies[k]
	if pe == nil {
		pe = &policyEntry{pods: map[types.UID]*podEntry{}}
		m.policies[k] = pe
	}
	pe.cfg = cfg

	desired := make(map[types.UID]PodTarget, len(targets))
	for _, t := range targets {
		desired[t.UID] = t
	}

	// Stop probers for pods that disappeared.
	for uid, pod := range pe.pods {
		if _, ok := desired[uid]; !ok {
			if pod.cancel != nil {
				pod.cancel()
			}
			delete(pe.pods, uid)
		}
	}

	// Add/refresh probers for desired pods.
	for uid, t := range desired {
		if pod, ok := pe.pods[uid]; ok {
			pod.PodIP = t.IP
			pod.PodName = t.Name
			continue
		}
		pod := &podEntry{PodState: PodState{
			PodName: t.Name,
			PodUID:  t.UID,
			PodIP:   t.IP,
			Healthy: true, // assume healthy until failureThreshold failures
		}}
		pe.pods[uid] = pod
		ctx, cancel := context.WithCancel(m.root)
		pod.cancel = cancel
		go m.run(ctx, nn, uid)
	}
}

// SetSeenReady records that the reconciler observed this pod Ready at least
// once, which gates remediation (enforce-after-first-Ready grace).
func (m *Manager) SetSeenReady(nn types.NamespacedName, uid types.UID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if pe := m.policies[key(nn)]; pe != nil {
		if pod := pe.pods[uid]; pod != nil {
			pod.SeenReady = true
		}
	}
}

// Snapshot returns a copy of the current per-pod health for a policy.
func (m *Manager) Snapshot(nn types.NamespacedName) []PodState {
	m.mu.Lock()
	defer m.mu.Unlock()
	pe := m.policies[key(nn)]
	if pe == nil {
		return nil
	}
	out := make([]PodState, 0, len(pe.pods))
	for _, pod := range pe.pods {
		out = append(out, pod.PodState)
	}
	return out
}

// Forget stops all probers for a policy (called when the CR is deleted).
func (m *Manager) Forget(nn types.NamespacedName) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key(nn)
	if pe := m.policies[k]; pe != nil {
		for _, pod := range pe.pods {
			if pod.cancel != nil {
				pod.cancel()
			}
		}
		delete(m.policies, k)
	}
}

// run is one pod's probe loop.
func (m *Manager) run(ctx context.Context, nn types.NamespacedName, uid types.UID) {
	m.mu.Lock()
	pe := m.policies[key(nn)]
	if pe == nil {
		m.mu.Unlock()
		return
	}
	initialDelay := pe.cfg.InitialDelay
	period := pe.cfg.Period
	m.mu.Unlock()

	if period <= 0 {
		period = 10 * time.Second
	}
	if initialDelay > 0 {
		select {
		case <-time.After(initialDelay):
		case <-ctx.Done():
			return
		}
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		m.probeOnce(ctx, nn, uid)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

// probeOnce runs a single probe and records the result, emitting an event if
// the pod's health transitioned.
func (m *Manager) probeOnce(ctx context.Context, nn types.NamespacedName, uid types.UID) {
	// Read config + target IP under lock.
	m.mu.Lock()
	pe := m.policies[key(nn)]
	if pe == nil {
		m.mu.Unlock()
		return
	}
	pod := pe.pods[uid]
	if pod == nil {
		m.mu.Unlock()
		return
	}
	cfg := pe.cfg
	ip := pod.PodIP
	m.mu.Unlock()

	ok := m.checker.Probe(ctx, ip, cfg)
	now := m.clock.Now()

	result := "failure"
	if ok {
		result = "success"
	}
	metrics.ProbeTotal.WithLabelValues(nn.Namespace, nn.Name, result).Inc()

	m.mu.Lock()
	pe = m.policies[key(nn)]
	if pe == nil {
		m.mu.Unlock()
		return
	}
	pod = pe.pods[uid]
	if pod == nil {
		m.mu.Unlock()
		return
	}
	transitioned := record(pod, cfg, ok, now)
	m.mu.Unlock()

	if transitioned {
		select {
		case m.events <- event.GenericEvent{Object: &metav1.PartialObjectMetadata{
			ObjectMeta: metav1.ObjectMeta{Namespace: nn.Namespace, Name: nn.Name},
		}}:
		case <-ctx.Done():
		}
	}
}

// record applies one probe result to a pod's threshold state machine and
// returns whether the healthy<->unhealthy state flipped.
func record(pod *podEntry, cfg ProbeConfig, ok bool, now time.Time) bool {
	success := cfg.SuccessThreshold
	if success <= 0 {
		success = 1
	}
	failure := cfg.FailureThreshold
	if failure <= 0 {
		failure = 3
	}

	pod.LastProbe = now
	pod.started = true
	transitioned := false
	if ok {
		pod.ConsecutiveSuccesses++
		pod.ConsecutiveFailures = 0
		if !pod.Healthy && pod.ConsecutiveSuccesses >= success {
			pod.Healthy = true
			pod.LastTransition = now
			transitioned = true
		}
	} else {
		pod.ConsecutiveFailures++
		pod.ConsecutiveSuccesses = 0
		if pod.Healthy && pod.ConsecutiveFailures >= failure {
			pod.Healthy = false
			pod.LastTransition = now
			transitioned = true
		}
	}
	return transitioned
}
