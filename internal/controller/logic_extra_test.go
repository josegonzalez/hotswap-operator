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
)

func TestNormalizeProbeExplicitValues(t *testing.T) {
	p := corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: "/hc", Port: intstr.FromInt(8080), Scheme: corev1.URISchemeHTTPS,
			HTTPHeaders: []corev1.HTTPHeader{{Name: "X-Token", Value: "abc"}},
		}},
		PeriodSeconds:       5,
		TimeoutSeconds:      2,
		SuccessThreshold:    2,
		FailureThreshold:    5,
		InitialDelaySeconds: 3,
	}
	cfg, err := normalizeProbe(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Period != 5*time.Second || cfg.Timeout != 2*time.Second || cfg.InitialDelay != 3*time.Second {
		t.Fatalf("timings not carried through: %+v", cfg)
	}
	if cfg.SuccessThreshold != 2 || cfg.FailureThreshold != 5 {
		t.Fatalf("thresholds not carried through: %+v", cfg)
	}
	if cfg.Scheme != "HTTPS" || cfg.Headers["X-Token"] != "abc" {
		t.Fatalf("scheme/headers not carried through: %+v", cfg)
	}
}

func TestDeploymentStableNilReplicas(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 1},
		Status:     appsv1.DeploymentStatus{ObservedGeneration: 1},
	}
	if deploymentStable(dep) {
		t.Fatalf("nil spec.replicas should not be considered stable")
	}
	if effectiveReplicas(dep) != 1 {
		t.Fatalf("nil spec.replicas should default to 1 effective replica")
	}
}

func TestBreakerNilLast(t *testing.T) {
	if breakerOpen(5, 3, nil, time.Minute, time.Unix(0, 0)) {
		t.Fatalf("no last remediation should keep the breaker closed")
	}
	if cooldownActive(nil, time.Minute, time.Unix(0, 0)) {
		t.Fatalf("no last remediation should mean no active cooldown")
	}
}
