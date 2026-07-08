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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	hotswapv1alpha1 "github.com/josegonzalez/hotswap-operator/api/v1alpha1"
	"github.com/josegonzalez/hotswap-operator/internal/prober"
)

// fakeProbe is a scripted ProbeSource.
type fakeProbe struct {
	snapshot []prober.PodState
}

func (f *fakeProbe) Sync(types.NamespacedName, prober.ProbeConfig, []prober.PodTarget) {}
func (f *fakeProbe) SetSeenReady(types.NamespacedName, types.UID)                      {}
func (f *fakeProbe) Snapshot(types.NamespacedName) []prober.PodState                   { return f.snapshot }
func (f *fakeProbe) Forget(types.NamespacedName)                                       {}
func (f *fakeProbe) Events() <-chan event.GenericEvent                                 { return nil }

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := hotswapv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func newPolicy(depName string) *hotswapv1alpha1.HotSwapPolicy {
	requireStable := true
	return &hotswapv1alpha1.HotSwapPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pol",
			Namespace:  "ns",
			Generation: 1,
			Finalizers: []string{finalizerName},
		},
		Spec: hotswapv1alpha1.HotSwapPolicySpec{
			TargetRef: hotswapv1alpha1.TargetReference{APIVersion: "apps/v1", Kind: "Deployment", Name: depName},
			HotswapProbe: corev1.Probe{ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/health-check", Port: intstr.FromInt(9999)},
			}},
			Remediation: hotswapv1alpha1.RemediationSpec{
				Strategy:                   hotswapv1alpha1.StrategyAuto,
				RequireStable:              &requireStable,
				MaxConsecutiveAttempts:     3,
				CooldownSeconds:            600,
				MaxConcurrent:              1,
				SystemicFailureSkipPercent: 50,
			},
		},
	}
}

func newDeployment(name string, replicas int32, stable bool) *appsv1.Deployment {
	r := replicas
	status := appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: replicas}
	if stable {
		status.UpdatedReplicas = replicas
		status.AvailableReplicas = replicas
	} // else mid-rollout: updated/available stay 0
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Generation: 1},
		Spec: appsv1.DeploymentSpec{
			Replicas: &r,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
		},
		Status: status,
	}
}

func reconcileOnce(t *testing.T, objs []client.Object, probe *fakeProbe) (client.Client, ctrl.Result, error) {
	t.Helper()
	s := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&hotswapv1alpha1.HotSwapPolicy{}).
		Build()
	r := &HotSwapPolicyReconciler{
		Client:   c,
		Scheme:   s,
		Prober:   probe,
		Clock:    fixedClock{t: time.Unix(10000, 0)},
		Recorder: record.NewFakeRecorder(100),
	}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "pol"}})
	return c, res, err
}

func getDeploy(t *testing.T, c client.Client, name string) *appsv1.Deployment {
	t.Helper()
	var d appsv1.Deployment
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, &d); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	return &d
}

func getPolicy(t *testing.T, c client.Client) *hotswapv1alpha1.HotSwapPolicy {
	t.Helper()
	var p hotswapv1alpha1.HotSwapPolicy
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "pol"}, &p); err != nil {
		t.Fatalf("get policy: %v", err)
	}
	return &p
}

func hasCondition(p *hotswapv1alpha1.HotSwapPolicy, condType string, status metav1.ConditionStatus) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == condType {
			return c.Status == status
		}
	}
	return false
}

func TestReconcileHealthyNoAction(t *testing.T) {
	dep := newDeployment("app", 1, true)
	pol := newPolicy("app")
	probe := &fakeProbe{snapshot: []prober.PodState{{PodName: "p1", Healthy: true, SeenReady: true}}}

	c, _, err := reconcileOnce(t, []client.Object{dep, pol}, probe)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := getDeploy(t, c, "app").Spec.Template.Annotations[restartAnnotation]; ok {
		t.Fatalf("healthy target should not be restarted")
	}
	if !hasCondition(getPolicy(t, c), hotswapv1alpha1.ConditionHealthy, metav1.ConditionTrue) {
		t.Fatalf("expected Healthy=True")
	}
}

func TestReconcileUnhealthySingleReplicaTriggersRollout(t *testing.T) {
	dep := newDeployment("app", 1, true)
	pol := newPolicy("app")
	probe := &fakeProbe{snapshot: []prober.PodState{{PodName: "p1", Healthy: false, SeenReady: true}}}

	c, _, err := reconcileOnce(t, []client.Object{dep, pol}, probe)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := getDeploy(t, c, "app").Spec.Template.Annotations[restartAnnotation]; !ok {
		t.Fatalf("expected rollout-restart annotation on the Deployment")
	}
	p := getPolicy(t, c)
	if p.Status.ConsecutiveAttempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", p.Status.ConsecutiveAttempts)
	}
	if p.Status.LastRemediation == nil || p.Status.LastRemediation.Strategy != hotswapv1alpha1.StrategyRolloutRestart {
		t.Fatalf("expected RolloutRestart record, got %+v", p.Status.LastRemediation)
	}
}

func TestReconcileGraceSkipsNeverReady(t *testing.T) {
	dep := newDeployment("app", 1, true)
	pol := newPolicy("app")
	// Unhealthy but never became Ready -> excluded by the grace gate.
	probe := &fakeProbe{snapshot: []prober.PodState{{PodName: "p1", Healthy: false, SeenReady: false}}}

	c, _, err := reconcileOnce(t, []client.Object{dep, pol}, probe)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := getDeploy(t, c, "app").Spec.Template.Annotations[restartAnnotation]; ok {
		t.Fatalf("a pod that was never Ready must not be remediated")
	}
}

func TestReconcileNotStableWaits(t *testing.T) {
	dep := newDeployment("app", 1, false) // mid-rollout
	pol := newPolicy("app")
	probe := &fakeProbe{snapshot: []prober.PodState{{PodName: "p1", Healthy: false, SeenReady: true}}}

	c, _, err := reconcileOnce(t, []client.Object{dep, pol}, probe)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := getDeploy(t, c, "app").Spec.Template.Annotations[restartAnnotation]; ok {
		t.Fatalf("must not remediate while the Deployment is mid-rollout")
	}
	if !hasCondition(getPolicy(t, c), hotswapv1alpha1.ConditionRemediating, metav1.ConditionTrue) {
		t.Fatalf("expected Remediating=True while waiting to stabilize")
	}
}

func TestReconcileSystemicFailureSkips(t *testing.T) {
	dep := newDeployment("app", 2, true)
	pol := newPolicy("app")
	// 2/2 unhealthy >= 50% => systemic.
	probe := &fakeProbe{snapshot: []prober.PodState{
		{PodName: "p1", Healthy: false, SeenReady: true},
		{PodName: "p2", Healthy: false, SeenReady: true},
	}}

	c, _, err := reconcileOnce(t, []client.Object{dep, pol}, probe)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := getDeploy(t, c, "app").Spec.Template.Annotations[restartAnnotation]; ok {
		t.Fatalf("systemic failure must skip remediation")
	}
	if !hasCondition(getPolicy(t, c), hotswapv1alpha1.ConditionDegraded, metav1.ConditionTrue) {
		t.Fatalf("expected Degraded=True on systemic failure")
	}
}

func TestReconcileDeletePodMultiReplica(t *testing.T) {
	dep := newDeployment("app", 3, true)
	pol := newPolicy("app")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "ns", Labels: map[string]string{"app": "app"}}}
	// One unhealthy of three => below systemic threshold => DeletePod.
	probe := &fakeProbe{snapshot: []prober.PodState{
		{PodName: "p1", Healthy: false, SeenReady: true},
		{PodName: "p2", Healthy: true, SeenReady: true},
		{PodName: "p3", Healthy: true, SeenReady: true},
	}}

	c, _, err := reconcileOnce(t, []client.Object{dep, pol, pod}, probe)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := getDeploy(t, c, "app").Spec.Template.Annotations[restartAnnotation]; ok {
		t.Fatalf("multi-replica remediation should delete a pod, not roll the Deployment")
	}
	var got corev1.Pod
	err = c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "p1"}, &got)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected pod p1 to be deleted, got err=%v", err)
	}
}

func TestReconcileTargetNotFoundDegraded(t *testing.T) {
	pol := newPolicy("missing")
	probe := &fakeProbe{}

	c, _, err := reconcileOnce(t, []client.Object{pol}, probe)
	if err != nil {
		t.Fatal(err)
	}
	if !hasCondition(getPolicy(t, c), hotswapv1alpha1.ConditionDegraded, metav1.ConditionTrue) {
		t.Fatalf("expected Degraded=True when target Deployment is missing")
	}
}
