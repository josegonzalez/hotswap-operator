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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	hotswapv1alpha1 "github.com/josegonzalez/hotswap-operator/api/v1alpha1"
	"github.com/josegonzalez/hotswap-operator/internal/prober"
)

func TestReconcileAddsFinalizer(t *testing.T) {
	dep := newDeployment("app", 1, true)
	pol := newPolicy("app")
	pol.Finalizers = nil // simulate a freshly-created policy

	c, res, err := reconcileOnce(t, []client.Object{dep, pol}, &fakeProbe{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Requeue {
		t.Fatalf("expected requeue after adding finalizer")
	}
	p := getPolicy(t, c)
	found := false
	for _, f := range p.Finalizers {
		if f == finalizerName {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected finalizer to be added")
	}
}

func TestReconcileDeletionRemovesFinalizer(t *testing.T) {
	s := testScheme(t)
	pol := newPolicy("app")
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(pol).
		WithStatusSubresource(&hotswapv1alpha1.HotSwapPolicy{}).
		Build()
	// Deleting while a finalizer is present sets DeletionTimestamp but keeps the object.
	if err := c.Delete(context.Background(), pol); err != nil {
		t.Fatal(err)
	}
	r := &HotSwapPolicyReconciler{Client: c, Scheme: s, Prober: &fakeProbe{}, Clock: fixedClock{}, Recorder: record.NewFakeRecorder(10)}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "pol"}}); err != nil {
		t.Fatal(err)
	}
	var got hotswapv1alpha1.HotSwapPolicy
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "pol"}, &got)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected policy gone after finalizer removal, got %v", err)
	}
}

func TestReconcileInvalidProbe(t *testing.T) {
	dep := newDeployment("app", 1, true)
	pol := newPolicy("app")
	pol.Spec.HotswapProbe = corev1.Probe{} // no httpGet

	c, _, err := reconcileOnce(t, []client.Object{dep, pol}, &fakeProbe{})
	if err != nil {
		t.Fatal(err)
	}
	p := getPolicy(t, c)
	if !hasConditionReason(p, hotswapv1alpha1.ConditionDegraded, hotswapv1alpha1.ReasonInvalidProbe) {
		t.Fatalf("expected Degraded/InvalidProbe, got %+v", p.Status.Conditions)
	}
}

func TestReconcileCircuitBreakerOpen(t *testing.T) {
	dep := newDeployment("app", 1, true)
	pol := newPolicy("app")
	pol.Status.ConsecutiveAttempts = 3
	pol.Status.LastRemediation = &hotswapv1alpha1.RemediationRecord{Time: metav1.NewTime(time.Unix(10000, 0))}
	probe := &fakeProbe{snapshot: []prober.PodState{{PodName: "p1", Healthy: false, SeenReady: true}}}

	c, _, err := reconcileOnce(t, []client.Object{dep, pol}, probe)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := getDeploy(t, c, "app").Spec.Template.Annotations[restartAnnotation]; ok {
		t.Fatalf("breaker open should suppress remediation")
	}
	if !hasConditionReason(getPolicy(t, c), hotswapv1alpha1.ConditionDegraded, hotswapv1alpha1.ReasonCircuitOpen) {
		t.Fatalf("expected Degraded/CircuitOpen")
	}
}

func TestReconcileCooldownActive(t *testing.T) {
	dep := newDeployment("app", 1, true)
	pol := newPolicy("app")
	pol.Status.ConsecutiveAttempts = 1 // below max, but within cooldown
	pol.Status.LastRemediation = &hotswapv1alpha1.RemediationRecord{Time: metav1.NewTime(time.Unix(10000, 0))}
	probe := &fakeProbe{snapshot: []prober.PodState{{PodName: "p1", Healthy: false, SeenReady: true}}}

	c, _, err := reconcileOnce(t, []client.Object{dep, pol}, probe)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := getDeploy(t, c, "app").Spec.Template.Annotations[restartAnnotation]; ok {
		t.Fatalf("cooldown should defer remediation")
	}
	if !hasCondition(getPolicy(t, c), hotswapv1alpha1.ConditionRemediating, metav1.ConditionTrue) {
		t.Fatalf("expected Remediating while cooling down")
	}
}

func TestReconcileConflictingPolicyDegraded(t *testing.T) {
	s := testScheme(t)
	dep := newDeployment("app", 1, true)
	polA := newPolicy("app")
	polA.Name = "pol-a"
	polB := newPolicy("app")
	polB.Name = "pol-b"
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(dep, polA, polB).
		WithStatusSubresource(&hotswapv1alpha1.HotSwapPolicy{}).
		Build()
	r := &HotSwapPolicyReconciler{Client: c, Scheme: s, Prober: &fakeProbe{}, Clock: fixedClock{}, Recorder: record.NewFakeRecorder(10)}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "pol-b"}}); err != nil {
		t.Fatal(err)
	}
	var polBGot hotswapv1alpha1.HotSwapPolicy
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "pol-b"}, &polBGot); err != nil {
		t.Fatal(err)
	}
	if !hasConditionReason(&polBGot, hotswapv1alpha1.ConditionDegraded, hotswapv1alpha1.ReasonConflictingPolic) {
		t.Fatalf("expected loser policy Degraded/ConflictingPolicy, got %+v", polBGot.Status.Conditions)
	}
}

func hasConditionReason(p *hotswapv1alpha1.HotSwapPolicy, condType, reason string) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == condType {
			return c.Reason == reason && c.Status == metav1.ConditionTrue
		}
	}
	return false
}
