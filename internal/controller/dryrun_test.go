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
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hotswapv1alpha1 "github.com/josegonzalez/hotswap-operator/api/v1alpha1"
	"github.com/josegonzalez/hotswap-operator/internal/prober"
)

func TestReconcileDryRunSingleReplicaNoMutation(t *testing.T) {
	dep := newDeployment("app", 1, true)
	pol := newPolicy("app")
	pol.Spec.Remediation.DryRun = true
	probe := &fakeProbe{snapshot: []prober.PodState{{PodName: "p1", Healthy: false, SeenReady: true}}}

	c, _, err := reconcileOnce(t, []client.Object{dep, pol}, probe)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := getDeploy(t, c, "app").Spec.Template.Annotations[restartAnnotation]; ok {
		t.Fatalf("dry-run must not patch the Deployment")
	}
	p := getPolicy(t, c)
	if p.Status.ConsecutiveAttempts != 1 {
		t.Fatalf("dry-run should still record the attempt, got %d", p.Status.ConsecutiveAttempts)
	}
	if p.Status.LastRemediation == nil || !strings.Contains(p.Status.LastRemediation.Reason, "dry-run") {
		t.Fatalf("expected a dry-run last-remediation record, got %+v", p.Status.LastRemediation)
	}
	if !hasCondition(p, hotswapv1alpha1.ConditionRemediating, metav1.ConditionTrue) {
		t.Fatalf("expected Remediating=True in dry-run")
	}
}

func TestReconcileDryRunDeletePodNotDeleted(t *testing.T) {
	dep := newDeployment("app", 3, true)
	pol := newPolicy("app")
	pol.Spec.Remediation.DryRun = true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "ns", Labels: map[string]string{"app": "app"}}}
	probe := &fakeProbe{snapshot: []prober.PodState{
		{PodName: "p1", Healthy: false, SeenReady: true},
		{PodName: "p2", Healthy: true, SeenReady: true},
		{PodName: "p3", Healthy: true, SeenReady: true},
	}}

	c, _, err := reconcileOnce(t, []client.Object{dep, pol, pod}, probe)
	if err != nil {
		t.Fatal(err)
	}
	var got corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "p1"}, &got); err != nil {
		t.Fatalf("dry-run must not delete the pod, but it is gone: %v", err)
	}
	if getPolicy(t, c).Status.ConsecutiveAttempts != 1 {
		t.Fatalf("dry-run should still record the attempt")
	}
}
