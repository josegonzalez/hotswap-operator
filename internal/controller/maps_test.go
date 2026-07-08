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
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	hotswapv1alpha1 "github.com/josegonzalez/hotswap-operator/api/v1alpha1"
	"github.com/josegonzalez/hotswap-operator/internal/prober"
)

func newReconcilerWith(t *testing.T, objs ...client.Object) (*HotSwapPolicyReconciler, client.Client) {
	t.Helper()
	s := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&hotswapv1alpha1.HotSwapPolicy{}).
		Build()
	return &HotSwapPolicyReconciler{
		Client:   c,
		Scheme:   s,
		Prober:   &fakeProbe{},
		Clock:    fixedClock{t: time.Unix(1, 0)},
		Recorder: record.NewFakeRecorder(10),
	}, c
}

func TestPoliciesForPodAndDeployment(t *testing.T) {
	dep := newDeployment("app", 1, true)
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Name: "app-rs", Namespace: "ns",
		OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "app"}},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "app-rs-xyz", Namespace: "ns", Labels: map[string]string{"app": "app"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "app-rs"}},
	}}
	pol := newPolicy("app")
	r, _ := newReconcilerWith(t, dep, rs, pod, pol)
	ctx := context.Background()

	if name := r.deploymentNameForPod(ctx, pod); name != "app" {
		t.Fatalf("deploymentNameForPod = %q, want app", name)
	}
	if reqs := r.policiesForPod(ctx, pod); len(reqs) != 1 || reqs[0].Name != "pol" {
		t.Fatalf("policiesForPod = %+v, want one request for pol", reqs)
	}
	if reqs := r.policiesForDeployment(ctx, dep); len(reqs) != 1 || reqs[0].Name != "pol" {
		t.Fatalf("policiesForDeployment = %+v, want one request for pol", reqs)
	}

	// A pod with no ReplicaSet owner maps to nothing.
	orphan := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: "ns"}}
	if name := r.deploymentNameForPod(ctx, orphan); name != "" {
		t.Fatalf("orphan pod should have no deployment, got %q", name)
	}
	if reqs := r.policiesForPod(ctx, orphan); reqs != nil {
		t.Fatalf("orphan pod should map to no policies, got %+v", reqs)
	}
}

func TestConflictingPolicy(t *testing.T) {
	polA := newPolicy("app")
	polA.Name = "pol-a"
	polB := newPolicy("app")
	polB.Name = "pol-b"
	dep := newDeployment("app", 1, true)
	r, _ := newReconcilerWith(t, dep, polA, polB)
	ctx := context.Background()

	// Tie on creation time -> name order: pol-a wins.
	if w, err := r.conflictingPolicy(ctx, polB); err != nil || w != "pol-a" {
		t.Fatalf("conflictingPolicy(pol-b) = %q,%v; want pol-a,nil", w, err)
	}
	if w, err := r.conflictingPolicy(ctx, polA); err != nil || w != "" {
		t.Fatalf("conflictingPolicy(pol-a) = %q,%v; want \"\",nil", w, err)
	}
}

func TestIsPodReady(t *testing.T) {
	ready := &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	}}}
	notReady := &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	}}}
	none := &corev1.Pod{}
	if !isPodReady(ready) || isPodReady(notReady) || isPodReady(none) {
		t.Fatalf("isPodReady wrong: ready=%v notReady=%v none=%v",
			isPodReady(ready), isPodReady(notReady), isPodReady(none))
	}
}

func TestToTargetHealth(t *testing.T) {
	now := time.Unix(5, 0)
	got := toTargetHealth([]prober.PodState{
		{PodName: "p1", PodIP: "1.1.1.1", Healthy: true, LastProbe: now, LastTransition: now},
		{PodName: "p2", Healthy: false}, // zero times => nil pointers
	})
	if len(got) != 2 {
		t.Fatalf("want 2, got %d", len(got))
	}
	if got[0].LastProbeTime == nil || got[0].LastTransitionTime == nil {
		t.Fatalf("expected non-nil times for p1")
	}
	if got[1].LastProbeTime != nil || got[1].LastTransitionTime != nil {
		t.Fatalf("expected nil times for p2 with zero timestamps")
	}
}
