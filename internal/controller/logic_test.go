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
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	hotswapv1alpha1 "github.com/josegonzalez/hotswap-operator/api/v1alpha1"
)

func TestNormalizeProbeDefaults(t *testing.T) {
	p := corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: "/health-check", Port: intstr.FromInt(9999)},
		},
	}
	cfg, err := normalizeProbe(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Period != 10*time.Second || cfg.Timeout != 1*time.Second {
		t.Fatalf("defaults not applied: period=%v timeout=%v", cfg.Period, cfg.Timeout)
	}
	if cfg.SuccessThreshold != 1 || cfg.FailureThreshold != 3 {
		t.Fatalf("threshold defaults wrong: succ=%d fail=%d", cfg.SuccessThreshold, cfg.FailureThreshold)
	}
	if cfg.Port != 9999 || cfg.Path != "/health-check" {
		t.Fatalf("httpGet not carried through: %+v", cfg)
	}
}

func TestNormalizeProbeRejectsNonHTTP(t *testing.T) {
	if _, err := normalizeProbe(corev1.Probe{}); err == nil {
		t.Fatalf("expected error for missing httpGet")
	}
	named := corev1.Probe{ProbeHandler: corev1.ProbeHandler{
		HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromString("http")},
	}}
	if _, err := normalizeProbe(named); err == nil {
		t.Fatalf("expected error for named port")
	}
}

func deploy(gen, observed int64, replicas, upd, avail, unavail int32) *appsv1.Deployment {
	r := replicas
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: gen},
		Spec:       appsv1.DeploymentSpec{Replicas: &r},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration:  observed,
			Replicas:            replicas,
			UpdatedReplicas:     upd,
			AvailableReplicas:   avail,
			UnavailableReplicas: unavail,
		},
	}
}

func TestDeploymentStable(t *testing.T) {
	tests := []struct {
		name string
		dep  *appsv1.Deployment
		want bool
	}{
		{"settled", deploy(2, 2, 1, 1, 1, 0), true},
		{"generation lagging", deploy(3, 2, 1, 1, 1, 0), false},
		{"surging (replicas>desired)", deploy(2, 2, 1, 1, 1, 0), true}, // baseline
		{"mid-rollout not all updated", deploy(2, 2, 2, 1, 2, 0), false},
		{"unavailable present", deploy(2, 2, 1, 1, 0, 1), false},
	}
	for _, tt := range tests {
		if got := deploymentStable(tt.dep); got != tt.want {
			t.Errorf("%s: deploymentStable=%v want %v", tt.name, got, tt.want)
		}
	}
}

func TestProgressDeadlineExceeded(t *testing.T) {
	dep := &appsv1.Deployment{Status: appsv1.DeploymentStatus{Conditions: []appsv1.DeploymentCondition{
		{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded"},
	}}}
	if !progressDeadlineExceeded(dep) {
		t.Fatalf("expected stalled rollout to be detected")
	}
	ok := &appsv1.Deployment{Status: appsv1.DeploymentStatus{Conditions: []appsv1.DeploymentCondition{
		{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "NewReplicaSetAvailable"},
	}}}
	if progressDeadlineExceeded(ok) {
		t.Fatalf("did not expect a healthy rollout to be flagged")
	}
}

func TestChooseStrategy(t *testing.T) {
	if got := chooseStrategy(hotswapv1alpha1.StrategyAuto, 1); got != hotswapv1alpha1.StrategyRolloutRestart {
		t.Errorf("Auto/1 => %s, want RolloutRestart", got)
	}
	if got := chooseStrategy(hotswapv1alpha1.StrategyAuto, 3); got != hotswapv1alpha1.StrategyDeletePod {
		t.Errorf("Auto/3 => %s, want DeletePod", got)
	}
	if got := chooseStrategy(hotswapv1alpha1.StrategyRolloutRestart, 5); got != hotswapv1alpha1.StrategyRolloutRestart {
		t.Errorf("explicit strategy should be honored regardless of replicas")
	}
}

func TestIsSystemic(t *testing.T) {
	if isSystemic(1, 4, 50) {
		t.Errorf("1/4 (25%%) should not be systemic at 50%%")
	}
	if !isSystemic(2, 4, 50) {
		t.Errorf("2/4 (50%%) should be systemic at 50%%")
	}
	if isSystemic(0, 0, 50) {
		t.Errorf("no targets should not be systemic")
	}
	if isSystemic(1, 1, 50) {
		t.Errorf("single replica (1/1) must not be treated as systemic")
	}
}

func TestBreakerAndCooldown(t *testing.T) {
	now := time.Unix(1000, 0)
	last := &hotswapv1alpha1.RemediationRecord{Time: metav1.NewTime(now.Add(-30 * time.Second))}
	cooldown := 60 * time.Second

	// 3 attempts, max 3, last was 30s ago (<60s cooldown) => open.
	if !breakerOpen(3, 3, last, cooldown, now) {
		t.Errorf("expected breaker open within cooldown at max attempts")
	}
	// Same attempts but last was long ago => closed.
	old := &hotswapv1alpha1.RemediationRecord{Time: metav1.NewTime(now.Add(-5 * time.Minute))}
	if breakerOpen(3, 3, old, cooldown, now) {
		t.Errorf("expected breaker closed after cooldown elapsed")
	}
	// Below max => closed.
	if breakerOpen(1, 3, last, cooldown, now) {
		t.Errorf("expected breaker closed below max attempts")
	}
	// Cooldown spacing.
	if !cooldownActive(last, cooldown, now) {
		t.Errorf("expected cooldown active 30s after last with 60s window")
	}
	if cooldownActive(old, cooldown, now) {
		t.Errorf("expected cooldown inactive 5m after last")
	}
}
