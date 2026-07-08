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
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// ProbeConfig is the normalized HTTP probe derived from a HotSwapPolicy's
// hotswapProbe (a core/v1 httpGet probe). Timing fields are already defaulted
// to the kubelet equivalents by the caller.
type ProbeConfig struct {
	Path             string
	Scheme           string // HTTP or HTTPS
	Port             int32
	Timeout          time.Duration
	Period           time.Duration
	InitialDelay     time.Duration
	SuccessThreshold int32
	FailureThreshold int32
	Headers          map[string]string
}

// Checker performs a single health probe against a pod IP and reports whether
// it was healthy. It never returns an error: an unreachable target or a non-2xx
// status is simply "not healthy", mirroring an ELB target health check.
type Checker interface {
	Probe(ctx context.Context, podIP string, cfg ProbeConfig) bool
}

// HTTPChecker is the production Checker: an HTTP GET that treats any 2xx/3xx
// response as healthy, matching kubelet httpGet semantics.
type HTTPChecker struct {
	client *http.Client
}

// NewHTTPChecker returns an HTTPChecker. A per-request timeout is applied via
// context in Probe, so the shared client has no global timeout.
func NewHTTPChecker() *HTTPChecker {
	return &HTTPChecker{
		client: &http.Client{
			Transport: &http.Transport{
				// Pods present pod-scoped certs we do not validate; the probe
				// only checks reachability + status, exactly like an ALB.
				TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // health probe, not a trust boundary
				DisableKeepAlives: true,
			},
			// Do not follow redirects; a 3xx is considered healthy like kubelet.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// Probe issues one GET to scheme://podIP:port/path and reports health.
func (c *HTTPChecker) Probe(ctx context.Context, podIP string, cfg ProbeConfig) bool {
	if podIP == "" {
		return false
	}
	scheme := strings.ToLower(cfg.Scheme)
	if scheme == "" {
		scheme = "http"
	}
	path := cfg.Path
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	url := fmt.Sprintf("%s://%s%s", scheme, net.JoinHostPort(podIP, fmt.Sprintf("%d", cfg.Port)), path)

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	// kubelet treats 200-399 as success.
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}
