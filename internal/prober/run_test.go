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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
)

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// TestManagerRunLoop exercises the real probe goroutine end to end against an
// httptest server, including a health transition and the emitted event.
func TestManagerRunLoop(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if healthy.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	host, port := hostPort(t, ts.URL)

	m := NewManager(NewHTTPChecker(), RealClock{}, logr.Discard())
	if !m.NeedLeaderElection() {
		t.Fatalf("prober should require leader election")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = m.Start(ctx) }()

	nn := types.NamespacedName{Namespace: "ns", Name: "pol"}
	cfg := ProbeConfig{
		Path: "/", Scheme: "http", Port: port, Timeout: 500 * time.Millisecond,
		Period: 10 * time.Millisecond, SuccessThreshold: 1, FailureThreshold: 2,
	}
	m.Sync(nn, cfg, []PodTarget{{Name: "a", UID: types.UID("ua"), IP: host}})

	// Stays healthy while the server returns 200.
	waitFor(t, func() bool {
		s := m.Snapshot(nn)
		return len(s) == 1 && s[0].Healthy && !s[0].LastProbe.IsZero()
	}, 2*time.Second)

	// Flip the server unhealthy; the prober should transition and emit an event.
	healthy.Store(false)
	waitFor(t, func() bool {
		s := m.Snapshot(nn)
		return len(s) == 1 && !s[0].Healthy
	}, 2*time.Second)

	select {
	case ev := <-m.Events():
		if ev.Object.GetName() != "pol" {
			t.Fatalf("event for wrong policy: %s", ev.Object.GetName())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("expected a transition event")
	}
}

// TestManagerStartStopsProbers verifies Start returns on context cancel.
func TestManagerStartStopsProbers(t *testing.T) {
	m := NewManager(&scriptedChecker{healthy: true}, RealClock{}, logr.Discard())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = m.Start(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Start did not return after context cancel")
	}
}

// TestRunInitialDelay covers the initial-delay branch of run().
func TestRunInitialDelay(t *testing.T) {
	m := NewManager(&scriptedChecker{healthy: true}, RealClock{}, logr.Discard())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = m.Start(ctx) }()

	nn := types.NamespacedName{Namespace: "ns", Name: "delayed"}
	cfg := ProbeConfig{
		Path: "/", Scheme: "http", Port: 1, Timeout: 50 * time.Millisecond,
		Period: 10 * time.Millisecond, InitialDelay: 30 * time.Millisecond,
		SuccessThreshold: 1, FailureThreshold: 1,
	}
	m.Sync(nn, cfg, []PodTarget{{Name: "a", UID: types.UID("ua"), IP: "127.0.0.1"}})
	// Just ensure the prober eventually records a probe after the delay.
	waitFor(t, func() bool {
		s := m.Snapshot(nn)
		return len(s) == 1 && !s[0].LastProbe.IsZero()
	}, 2*time.Second)
}
