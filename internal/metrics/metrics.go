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

// Package metrics defines the Prometheus collectors hotswap exports, registered
// with the controller-runtime metrics registry.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// ProbeTotal counts probes by policy and result (success/failure).
	ProbeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hotswap_probe_total",
		Help: "Total hotswap health probes performed, by policy and result.",
	}, []string{"namespace", "policy", "result"})

	// RemediationsTotal counts remediations by policy and strategy.
	RemediationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hotswap_remediations_total",
		Help: "Total hotswap remediations performed, by policy and strategy.",
	}, []string{"namespace", "policy", "strategy"})

	// DryRunTotal counts remediations that would have happened but were skipped
	// because the policy is in dry-run mode.
	DryRunTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hotswap_dryrun_total",
		Help: "Total hotswap remediations simulated in dry-run mode, by policy and strategy.",
	}, []string{"namespace", "policy", "strategy"})

	// CircuitOpen is 1 while a policy's remediation circuit breaker is open.
	CircuitOpen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hotswap_circuit_open",
		Help: "1 when a hotswap policy's remediation circuit breaker is open.",
	}, []string{"namespace", "policy"})

	// TargetsHealthy reports the number of healthy targets per policy.
	TargetsHealthy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hotswap_targets_healthy",
		Help: "Number of healthy targets observed for a hotswap policy.",
	}, []string{"namespace", "policy"})

	// TargetsTotal reports the number of observed targets per policy.
	TargetsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hotswap_targets_total",
		Help: "Number of targets observed for a hotswap policy.",
	}, []string{"namespace", "policy"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		ProbeTotal,
		RemediationsTotal,
		DryRunTotal,
		CircuitOpen,
		TargetsHealthy,
		TargetsTotal,
	)
}
