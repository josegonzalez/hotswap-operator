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
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func hostPort(t *testing.T, rawURL string) (string, int32) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	return host, int32(p)
}

func TestHTTPCheckerProbe(t *testing.T) {
	var code atomic.Int32
	code.Store(http.StatusOK)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(code.Load()))
	}))
	host, port := hostPort(t, ts.URL)

	c := NewHTTPChecker()
	cfg := ProbeConfig{Path: "health", Scheme: "http", Port: port, Timeout: time.Second} // no leading slash

	ctx := context.Background()
	if !c.Probe(ctx, host, cfg) {
		t.Fatalf("expected healthy on 200")
	}
	code.Store(http.StatusInternalServerError)
	if c.Probe(ctx, host, cfg) {
		t.Fatalf("expected unhealthy on 500")
	}
	if c.Probe(ctx, "", cfg) {
		t.Fatalf("empty IP should be unhealthy")
	}

	// Unreachable after close.
	ts.Close()
	if c.Probe(ctx, host, cfg) {
		t.Fatalf("closed server should be unhealthy")
	}
}

func TestRealClockNow(t *testing.T) {
	if (RealClock{}).Now().IsZero() {
		t.Fatalf("RealClock.Now returned zero time")
	}
}
