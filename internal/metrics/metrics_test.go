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

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsRegisteredAndUsable(t *testing.T) {
	ProbeTotal.WithLabelValues("ns", "pol", "success").Inc()
	if got := testutil.ToFloat64(ProbeTotal.WithLabelValues("ns", "pol", "success")); got != 1 {
		t.Fatalf("ProbeTotal = %v, want 1", got)
	}
	RemediationsTotal.WithLabelValues("ns", "pol", "RolloutRestart").Inc()
	if got := testutil.ToFloat64(RemediationsTotal.WithLabelValues("ns", "pol", "RolloutRestart")); got != 1 {
		t.Fatalf("RemediationsTotal = %v, want 1", got)
	}
	CircuitOpen.WithLabelValues("ns", "pol").Set(1)
	if got := testutil.ToFloat64(CircuitOpen.WithLabelValues("ns", "pol")); got != 1 {
		t.Fatalf("CircuitOpen = %v, want 1", got)
	}
	TargetsHealthy.WithLabelValues("ns", "pol").Set(2)
	TargetsTotal.WithLabelValues("ns", "pol").Set(3)
	if got := testutil.ToFloat64(TargetsTotal.WithLabelValues("ns", "pol")); got != 3 {
		t.Fatalf("TargetsTotal = %v, want 3", got)
	}
}
