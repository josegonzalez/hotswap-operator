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

package controller

import (
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	hotswapv1alpha1 "github.com/josegonzalez/hotswap-operator/api/v1alpha1"
	"github.com/josegonzalez/hotswap-operator/internal/prober"
)

// normalizeProbe validates a hotswapProbe (only httpGet is supported) and fills
// unset timing fields with the same defaults the kubelet applies.
func normalizeProbe(p corev1.Probe) (prober.ProbeConfig, error) {
	if p.HTTPGet == nil {
		return prober.ProbeConfig{}, fmt.Errorf("hotswapProbe.httpGet is required (exec/tcp/grpc probes are not supported)")
	}
	h := p.HTTPGet
	if h.Port.Type != intstr.Int || h.Port.IntVal <= 0 {
		return prober.ProbeConfig{}, fmt.Errorf("hotswapProbe.httpGet.port must be a numeric container port")
	}

	cfg := prober.ProbeConfig{
		Path:             h.Path,
		Scheme:           string(h.Scheme),
		Port:             h.Port.IntVal,
		Period:           seconds(p.PeriodSeconds, 10),
		Timeout:          seconds(p.TimeoutSeconds, 1),
		InitialDelay:     seconds(p.InitialDelaySeconds, 0),
		SuccessThreshold: threshold(p.SuccessThreshold, 1),
		FailureThreshold: threshold(p.FailureThreshold, 3),
	}
	if len(h.HTTPHeaders) > 0 {
		cfg.Headers = make(map[string]string, len(h.HTTPHeaders))
		for _, hd := range h.HTTPHeaders {
			cfg.Headers[hd.Name] = hd.Value
		}
	}
	return cfg, nil
}

func seconds(v, def int32) time.Duration {
	if v <= 0 {
		return time.Duration(def) * time.Second
	}
	return time.Duration(v) * time.Second
}

func threshold(v, def int32) int32 {
	if v <= 0 {
		return def
	}
	return v
}

// deploymentStable reports whether the Deployment has fully settled: its status
// reflects the current generation and every replica is updated and available
// with none surging or unavailable. This is the gate that keeps remediation
// from overlapping with an in-progress rollout (including hotswap's own).
func deploymentStable(dep *appsv1.Deployment) bool {
	if dep.Generation != dep.Status.ObservedGeneration {
		return false
	}
	if dep.Spec.Replicas == nil {
		return false
	}
	r := *dep.Spec.Replicas
	return dep.Status.Replicas == r &&
		dep.Status.UpdatedReplicas == r &&
		dep.Status.AvailableReplicas == r &&
		dep.Status.UnavailableReplicas == 0
}

// progressDeadlineExceeded reports whether the Deployment's rollout has failed
// its progress deadline (a stuck bad-image rollout).
func progressDeadlineExceeded(dep *appsv1.Deployment) bool {
	for _, c := range dep.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing &&
			c.Status == corev1.ConditionFalse &&
			c.Reason == "ProgressDeadlineExceeded" {
			return true
		}
	}
	return false
}

// effectiveReplicas is the Deployment's current desired replica count (as
// managed by KEDA/HPA), defaulting to 1 when unset.
func effectiveReplicas(dep *appsv1.Deployment) int32 {
	if dep.Spec.Replicas == nil {
		return 1
	}
	return *dep.Spec.Replicas
}

// chooseStrategy resolves the Auto strategy by replica count.
func chooseStrategy(s hotswapv1alpha1.RemediationStrategy, replicas int32) hotswapv1alpha1.RemediationStrategy {
	switch s {
	case hotswapv1alpha1.StrategyRolloutRestart, hotswapv1alpha1.StrategyDeletePod:
		return s
	default:
		if replicas <= 1 {
			return hotswapv1alpha1.StrategyRolloutRestart
		}
		return hotswapv1alpha1.StrategyDeletePod
	}
}

// isSystemic reports whether enough targets are unhealthy at once to treat the
// failure as systemic (dependency outage / lost reachability) and skip
// remediation instead of replacing everything. It is only meaningful with 2+
// targets: for a single replica, 1/1 unhealthy is 100% and is exactly the case
// hotswap must remediate, so systemic detection is disabled below two targets.
func isSystemic(unhealthy, total int, skipPercent int32) bool {
	if total < 2 || unhealthy <= 0 || skipPercent <= 0 {
		return false
	}
	// unhealthy/total >= skipPercent/100, done in integers.
	return unhealthy*100 >= int(skipPercent)*total
}

// breakerOpen reports whether the circuit breaker should suppress remediation:
// too many consecutive attempts, still within the cooldown since the last one.
func breakerOpen(consecutiveAttempts, maxAttempts int32, last *hotswapv1alpha1.RemediationRecord, cooldown time.Duration, now time.Time) bool {
	if consecutiveAttempts < maxAttempts {
		return false
	}
	if last == nil {
		return false
	}
	return now.Sub(last.Time.Time) < cooldown
}

// cooldownActive reports whether we are still inside the minimum spacing since
// the last remediation.
func cooldownActive(last *hotswapv1alpha1.RemediationRecord, cooldown time.Duration, now time.Time) bool {
	if last == nil || cooldown <= 0 {
		return false
	}
	return now.Sub(last.Time.Time) < cooldown
}
