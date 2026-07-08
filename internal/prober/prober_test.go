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

package prober

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
)

// fakeClock is a deterministic Clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// scriptedChecker returns a fixed health verdict, switchable between calls.
type scriptedChecker struct {
	mu      sync.Mutex
	healthy bool
}

func (s *scriptedChecker) set(h bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthy = h
}

func (s *scriptedChecker) Probe(context.Context, string, ProbeConfig) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.healthy
}

func TestRecordStateMachine(t *testing.T) {
	cfg := ProbeConfig{SuccessThreshold: 2, FailureThreshold: 3}
	now := time.Unix(0, 0)

	pod := &podEntry{PodState: PodState{Healthy: true}}

	// Two failures: not yet unhealthy (threshold is 3).
	if record(pod, cfg, false, now) || !pod.Healthy {
		t.Fatalf("after 1 failure: transitioned=%v healthy=%v, want false/true", false, pod.Healthy)
	}
	if record(pod, cfg, false, now) || !pod.Healthy {
		t.Fatalf("after 2 failures should still be healthy")
	}
	// Third failure flips to unhealthy.
	if !record(pod, cfg, false, now) || pod.Healthy {
		t.Fatalf("after 3 failures should transition to unhealthy")
	}
	// One success: not yet healthy (successThreshold is 2).
	if record(pod, cfg, true, now) || pod.Healthy {
		t.Fatalf("after 1 success should still be unhealthy")
	}
	// Second success flips back to healthy.
	if !record(pod, cfg, true, now) || !pod.Healthy {
		t.Fatalf("after 2 successes should transition to healthy")
	}
}

func TestRecordResetsCountersOnFlip(t *testing.T) {
	cfg := ProbeConfig{SuccessThreshold: 1, FailureThreshold: 2}
	now := time.Unix(0, 0)
	pod := &podEntry{PodState: PodState{Healthy: true}}

	record(pod, cfg, false, now) // 1 failure
	record(pod, cfg, true, now)  // success resets failure counter and (successThreshold=1) keeps healthy
	if pod.ConsecutiveFailures != 0 {
		t.Fatalf("success should reset consecutive failures, got %d", pod.ConsecutiveFailures)
	}
	// Now it takes a full 2 failures again to flip.
	if record(pod, cfg, false, now) {
		t.Fatalf("single failure after reset should not flip")
	}
	if !record(pod, cfg, false, now) {
		t.Fatalf("second failure should flip to unhealthy")
	}
}

func TestManagerProbeOnceEmitsOnTransition(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	checker := &scriptedChecker{healthy: true}
	m := NewManager(checker, clock, logr.Discard())

	nn := types.NamespacedName{Namespace: "ns", Name: "pol"}
	cfg := ProbeConfig{Period: time.Second, SuccessThreshold: 1, FailureThreshold: 2}
	m.Sync(nn, cfg, []PodTarget{{Name: "pod-a", UID: types.UID("uid-a"), IP: "10.0.0.1"}})

	ctx := context.Background()

	// Healthy probes: no transition (starts healthy), no event.
	m.probeOnce(ctx, nn, "uid-a")
	if got := m.Snapshot(nn); len(got) != 1 || !got[0].Healthy {
		t.Fatalf("expected healthy snapshot, got %+v", got)
	}
	select {
	case <-m.Events():
		t.Fatalf("did not expect an event while staying healthy")
	default:
	}

	// Two failures flip to unhealthy and emit exactly one event.
	checker.set(false)
	m.probeOnce(ctx, nn, "uid-a") // failure 1
	m.probeOnce(ctx, nn, "uid-a") // failure 2 -> transition
	if got := m.Snapshot(nn); got[0].Healthy {
		t.Fatalf("expected unhealthy after failureThreshold, got %+v", got)
	}
	select {
	case ev := <-m.Events():
		if ev.Object.GetName() != "pol" || ev.Object.GetNamespace() != "ns" {
			t.Fatalf("event targeted wrong policy: %s/%s", ev.Object.GetNamespace(), ev.Object.GetName())
		}
	default:
		t.Fatalf("expected a transition event")
	}
}

func TestManagerSyncAndForget(t *testing.T) {
	m := NewManager(&scriptedChecker{healthy: true}, &fakeClock{}, logr.Discard())
	nn := types.NamespacedName{Namespace: "ns", Name: "pol"}
	cfg := ProbeConfig{Period: time.Hour} // long period so goroutine ticks won't interfere

	m.Sync(nn, cfg, []PodTarget{
		{Name: "a", UID: "ua", IP: "1.1.1.1"},
		{Name: "b", UID: "ub", IP: "1.1.1.2"},
	})
	if got := len(m.Snapshot(nn)); got != 2 {
		t.Fatalf("expected 2 targets, got %d", got)
	}

	// Dropping a target removes it.
	m.Sync(nn, cfg, []PodTarget{{Name: "a", UID: "ua", IP: "1.1.1.1"}})
	if got := len(m.Snapshot(nn)); got != 1 {
		t.Fatalf("expected 1 target after drop, got %d", got)
	}

	// SeenReady is recorded.
	m.SetSeenReady(nn, "ua")
	if !m.Snapshot(nn)[0].SeenReady {
		t.Fatalf("expected SeenReady to be set")
	}

	m.Forget(nn)
	if got := m.Snapshot(nn); got != nil {
		t.Fatalf("expected nil snapshot after Forget, got %+v", got)
	}
}
