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
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	hotswapv1alpha1 "github.com/josegonzalez/hotswap-operator/api/v1alpha1"
	"github.com/josegonzalez/hotswap-operator/internal/metrics"
	"github.com/josegonzalez/hotswap-operator/internal/prober"
)

const (
	finalizerName     = "hotswap.io/finalizer"
	restartAnnotation = "hotswap.io/restarted-at"
)

// ProbeSource is the subset of the prober Manager the reconciler depends on.
// Keeping it an interface lets tests inject scripted health without real HTTP.
type ProbeSource interface {
	Sync(nn types.NamespacedName, cfg prober.ProbeConfig, targets []prober.PodTarget)
	SetSeenReady(nn types.NamespacedName, uid types.UID)
	Snapshot(nn types.NamespacedName) []prober.PodState
	Forget(nn types.NamespacedName)
	Events() <-chan event.GenericEvent
}

// HotSwapPolicyReconciler reconciles a HotSwapPolicy object.
type HotSwapPolicyReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Prober   ProbeSource
	Clock    prober.Clock
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=hotswap.io,resources=hotswappolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hotswap.io,resources=hotswappolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hotswap.io,resources=hotswappolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives a target Deployment toward ECS-style health-based
// replacement: it probes the target's pods, and when one regresses while the
// Deployment is stable, it triggers a rolling restart (single replica) or
// deletes the offending pod (multiple replicas).
func (r *HotSwapPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	nn := req.NamespacedName

	var policy hotswapv1alpha1.HotSwapPolicy
	if err := r.Get(ctx, nn, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion / finalizer.
	if !policy.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&policy, finalizerName) {
			r.Prober.Forget(nn)
			controllerutil.RemoveFinalizer(&policy, finalizerName)
			if err := r.Update(ctx, &policy); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&policy, finalizerName) {
		controllerutil.AddFinalizer(&policy, finalizerName)
		if err := r.Update(ctx, &policy); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Validate the probe.
	cfg, err := normalizeProbe(policy.Spec.HotswapProbe)
	if err != nil {
		r.setDegraded(&policy, hotswapv1alpha1.ReasonInvalidProbe, err.Error())
		return r.finish(ctx, &policy, ctrl.Result{})
	}

	// Resolve the target Deployment.
	var dep appsv1.Deployment
	depName := policy.Spec.TargetRef.Name
	if err := r.Get(ctx, types.NamespacedName{Namespace: nn.Namespace, Name: depName}, &dep); err != nil {
		if apierrors.IsNotFound(err) {
			r.Prober.Forget(nn)
			r.setDegraded(&policy, hotswapv1alpha1.ReasonTargetNotFound, fmt.Sprintf("deployment %q not found", depName))
			return r.finish(ctx, &policy, ctrl.Result{RequeueAfter: 30 * time.Second})
		}
		return ctrl.Result{}, err
	}

	// Reject overlapping policies targeting the same Deployment.
	if winner, err := r.conflictingPolicy(ctx, &policy); err != nil {
		return ctrl.Result{}, err
	} else if winner != "" {
		r.Prober.Forget(nn)
		r.setDegraded(&policy, hotswapv1alpha1.ReasonConflictingPolic,
			fmt.Sprintf("deployment %q already guarded by HotSwapPolicy %q", depName, winner))
		return r.finish(ctx, &policy, ctrl.Result{RequeueAfter: time.Minute})
	}

	// Discover target pods.
	sel, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
	if err != nil {
		r.setDegraded(&policy, hotswapv1alpha1.ReasonTargetNotFound, fmt.Sprintf("invalid selector: %v", err))
		return r.finish(ctx, &policy, ctrl.Result{})
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(nn.Namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return ctrl.Result{}, err
	}

	targets := make([]prober.PodTarget, 0, len(pods.Items))
	for i := range pods.Items {
		p := &pods.Items[i]
		if !p.DeletionTimestamp.IsZero() {
			continue // terminating; draining via preStop, do not probe/remediate
		}
		targets = append(targets, prober.PodTarget{Name: p.Name, UID: p.UID, IP: p.Status.PodIP})
	}
	r.Prober.Sync(nn, cfg, targets)
	for i := range pods.Items {
		p := &pods.Items[i]
		if isPodReady(p) {
			r.Prober.SetSeenReady(nn, p.UID)
		}
	}

	snapshot := r.Prober.Snapshot(nn)
	policy.Status.Targets = toTargetHealth(snapshot)

	healthyCount := 0
	for _, s := range snapshot {
		if s.Healthy {
			healthyCount++
		}
	}
	metrics.TargetsTotal.WithLabelValues(nn.Namespace, nn.Name).Set(float64(len(snapshot)))
	metrics.TargetsHealthy.WithLabelValues(nn.Namespace, nn.Name).Set(float64(healthyCount))

	now := r.Clock.Now()
	unhealthy := make([]prober.PodState, 0, len(snapshot))
	for _, s := range snapshot {
		if !s.Healthy && s.SeenReady { // enforce-after-first-Ready grace
			unhealthy = append(unhealthy, s)
		}
	}

	// A stalled rollout (bad image) is contained, not remediated.
	if progressDeadlineExceeded(&dep) {
		r.setDegraded(&policy, hotswapv1alpha1.ReasonRolloutStalled, "target rollout exceeded its progress deadline")
		return r.finish(ctx, &policy, ctrl.Result{RequeueAfter: cfg.Period})
	}

	if len(unhealthy) == 0 {
		policy.Status.ConsecutiveAttempts = 0
		metrics.CircuitOpen.WithLabelValues(nn.Namespace, nn.Name).Set(0)
		r.setHealthy(&policy)
		return r.finish(ctx, &policy, ctrl.Result{RequeueAfter: cfg.Period})
	}

	spec := policy.Spec.Remediation

	// Safety valve: too many unhealthy at once => systemic, do not replace all.
	if isSystemic(len(unhealthy), len(snapshot), spec.SystemicFailureSkipPercent) {
		msg := fmt.Sprintf("%d/%d targets unhealthy at once; skipping remediation", len(unhealthy), len(snapshot))
		r.Recorder.Event(&policy, corev1.EventTypeWarning, hotswapv1alpha1.ReasonSystemicFailure, msg)
		r.setDegraded(&policy, hotswapv1alpha1.ReasonSystemicFailure, msg)
		return r.finish(ctx, &policy, ctrl.Result{RequeueAfter: cfg.Period})
	}

	requireStable := spec.RequireStable == nil || *spec.RequireStable
	if requireStable && !deploymentStable(&dep) {
		r.setRemediating(&policy, "waiting for target Deployment to stabilize")
		return r.finish(ctx, &policy, ctrl.Result{RequeueAfter: cfg.Period})
	}

	cooldown := time.Duration(spec.CooldownSeconds) * time.Second
	maxAttempts := spec.MaxConsecutiveAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if breakerOpen(policy.Status.ConsecutiveAttempts, maxAttempts, policy.Status.LastRemediation, cooldown, now) {
		msg := fmt.Sprintf("circuit breaker open after %d consecutive remediations", policy.Status.ConsecutiveAttempts)
		metrics.CircuitOpen.WithLabelValues(nn.Namespace, nn.Name).Set(1)
		r.Recorder.Event(&policy, corev1.EventTypeWarning, hotswapv1alpha1.ReasonCircuitOpen, msg)
		r.setDegraded(&policy, hotswapv1alpha1.ReasonCircuitOpen, msg)
		return r.finish(ctx, &policy, ctrl.Result{RequeueAfter: cooldown})
	}
	if cooldownActive(policy.Status.LastRemediation, cooldown, now) {
		r.setRemediating(&policy, "cooling down before next remediation")
		return r.finish(ctx, &policy, ctrl.Result{RequeueAfter: cooldown})
	}

	strategy := chooseStrategy(spec.Strategy, effectiveReplicas(&dep))
	if err := r.remediate(ctx, &policy, &dep, strategy, unhealthy, spec, now); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("remediated target", "strategy", strategy, "unhealthy", len(unhealthy))
	return r.finish(ctx, &policy, ctrl.Result{RequeueAfter: cfg.Period})
}

func (r *HotSwapPolicyReconciler) remediate(
	ctx context.Context,
	policy *hotswapv1alpha1.HotSwapPolicy,
	dep *appsv1.Deployment,
	strategy hotswapv1alpha1.RemediationStrategy,
	unhealthy []prober.PodState,
	spec hotswapv1alpha1.RemediationSpec,
	now time.Time,
) error {
	dryRun := spec.DryRun
	var actedPod string
	switch strategy {
	case hotswapv1alpha1.StrategyRolloutRestart:
		if !dryRun {
			if err := r.rolloutRestart(ctx, dep, now); err != nil {
				return err
			}
		}
		actedPod = unhealthy[0].PodName
	case hotswapv1alpha1.StrategyDeletePod:
		limit := spec.MaxConcurrent
		if limit <= 0 {
			limit = 1
		}
		var acted int32
		for _, s := range unhealthy {
			if acted >= limit {
				break
			}
			if !dryRun {
				if err := r.deletePod(ctx, policy.Namespace, s.PodName); err != nil {
					if apierrors.IsNotFound(err) {
						continue
					}
					return err
				}
			}
			acted++
			actedPod = s.PodName
		}
	}

	policy.Status.ConsecutiveAttempts++
	reason := "hotswapProbe health regression"
	if dryRun {
		reason += " (dry-run)"
	}
	policy.Status.LastRemediation = &hotswapv1alpha1.RemediationRecord{
		PodName:  actedPod,
		Strategy: strategy,
		Time:     metav1.NewTime(now),
		Reason:   reason,
	}

	if dryRun {
		metrics.DryRunTotal.WithLabelValues(policy.Namespace, policy.Name, string(strategy)).Inc()
		msg := fmt.Sprintf("dry-run: would remediate %q via %s (attempt %d)", actedPod, strategy, policy.Status.ConsecutiveAttempts)
		r.setRemediating(policy, msg)
		r.Recorder.Event(policy, corev1.EventTypeNormal, hotswapv1alpha1.ReasonDryRun, msg)
		return nil
	}

	metrics.RemediationsTotal.WithLabelValues(policy.Namespace, policy.Name, string(strategy)).Inc()
	msg := fmt.Sprintf("remediated %q via %s (attempt %d)", actedPod, strategy, policy.Status.ConsecutiveAttempts)
	r.setRemediating(policy, msg)
	r.Recorder.Event(policy, corev1.EventTypeNormal, hotswapv1alpha1.ReasonRemediating, msg)
	return nil
}

// rolloutRestart triggers the Deployment's own rolling update by patching a
// pod-template annotation (the same mechanism as `kubectl rollout restart`). A
// JSON merge patch only adds our annotation, leaving other managed fields (and
// Helm's) untouched.
func (r *HotSwapPolicyReconciler) rolloutRestart(ctx context.Context, dep *appsv1.Deployment, now time.Time) error {
	ts := now.UTC().Format(time.RFC3339)
	patch := []byte(fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{%q:%q}}}}}`, restartAnnotation, ts))
	return r.Patch(ctx, dep, client.RawPatch(types.MergePatchType, patch))
}

func (r *HotSwapPolicyReconciler) deletePod(ctx context.Context, ns, name string) error {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	return r.Delete(ctx, pod)
}

// conflictingPolicy returns the name of another HotSwapPolicy that also targets
// this policy's Deployment and should win (earliest creation, then name),
// or "" if this policy is the sole/winning guardian.
func (r *HotSwapPolicyReconciler) conflictingPolicy(ctx context.Context, policy *hotswapv1alpha1.HotSwapPolicy) (string, error) {
	var list hotswapv1alpha1.HotSwapPolicyList
	if err := r.List(ctx, &list, client.InNamespace(policy.Namespace)); err != nil {
		return "", err
	}
	winner := policy
	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == policy.Name {
			continue
		}
		if other.Spec.TargetRef.Name != policy.Spec.TargetRef.Name {
			continue
		}
		if lessPolicy(other, winner) {
			winner = other
		}
	}
	if winner.Name == policy.Name {
		return "", nil
	}
	return winner.Name, nil
}

// lessPolicy orders policies deterministically: earliest creation wins, ties
// broken by name.
func lessPolicy(a, b *hotswapv1alpha1.HotSwapPolicy) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.Name < b.Name
}

func (r *HotSwapPolicyReconciler) finish(ctx context.Context, policy *hotswapv1alpha1.HotSwapPolicy, res ctrl.Result) (ctrl.Result, error) {
	policy.Status.ObservedGeneration = policy.Generation
	if err := r.Status().Update(ctx, policy); err != nil {
		return ctrl.Result{}, err
	}
	return res, nil
}

func (r *HotSwapPolicyReconciler) setHealthy(p *hotswapv1alpha1.HotSwapPolicy) {
	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{
		Type: hotswapv1alpha1.ConditionHealthy, Status: metav1.ConditionTrue,
		Reason: hotswapv1alpha1.ReasonAllHealthy, Message: "all targets healthy",
		ObservedGeneration: p.Generation,
	})
	meta.RemoveStatusCondition(&p.Status.Conditions, hotswapv1alpha1.ConditionRemediating)
	meta.RemoveStatusCondition(&p.Status.Conditions, hotswapv1alpha1.ConditionDegraded)
}

func (r *HotSwapPolicyReconciler) setRemediating(p *hotswapv1alpha1.HotSwapPolicy, msg string) {
	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{
		Type: hotswapv1alpha1.ConditionRemediating, Status: metav1.ConditionTrue,
		Reason: hotswapv1alpha1.ReasonRemediating, Message: msg,
		ObservedGeneration: p.Generation,
	})
	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{
		Type: hotswapv1alpha1.ConditionHealthy, Status: metav1.ConditionFalse,
		Reason: hotswapv1alpha1.ReasonRemediating, Message: msg,
		ObservedGeneration: p.Generation,
	})
	meta.RemoveStatusCondition(&p.Status.Conditions, hotswapv1alpha1.ConditionDegraded)
}

func (r *HotSwapPolicyReconciler) setDegraded(p *hotswapv1alpha1.HotSwapPolicy, reason, msg string) {
	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{
		Type: hotswapv1alpha1.ConditionDegraded, Status: metav1.ConditionTrue,
		Reason: reason, Message: msg, ObservedGeneration: p.Generation,
	})
	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{
		Type: hotswapv1alpha1.ConditionHealthy, Status: metav1.ConditionFalse,
		Reason: reason, Message: msg, ObservedGeneration: p.Generation,
	})
	meta.RemoveStatusCondition(&p.Status.Conditions, hotswapv1alpha1.ConditionRemediating)
}

func isPodReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func toTargetHealth(snapshot []prober.PodState) []hotswapv1alpha1.TargetHealth {
	out := make([]hotswapv1alpha1.TargetHealth, 0, len(snapshot))
	for _, s := range snapshot {
		th := hotswapv1alpha1.TargetHealth{
			PodName:             s.PodName,
			PodIP:               s.PodIP,
			Healthy:             s.Healthy,
			ConsecutiveFailures: s.ConsecutiveFailures,
		}
		if !s.LastProbe.IsZero() {
			t := metav1.NewTime(s.LastProbe)
			th.LastProbeTime = &t
		}
		if !s.LastTransition.IsZero() {
			t := metav1.NewTime(s.LastTransition)
			th.LastTransitionTime = &t
		}
		out = append(out, th)
	}
	return out
}

// SetupWithManager sets up the controller with the Manager.
func (r *HotSwapPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Clock == nil {
		r.Clock = prober.RealClock{}
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("hotswap")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&hotswapv1alpha1.HotSwapPolicy{}).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(r.policiesForDeployment)).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.policiesForPod)).
		WatchesRawSource(source.Channel(r.Prober.Events(), &handler.EnqueueRequestForObject{})).
		Named("hotswappolicy").
		Complete(r)
}

func (r *HotSwapPolicyReconciler) policiesForDeployment(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.policiesForDeploymentName(ctx, obj.GetNamespace(), obj.GetName())
}

func (r *HotSwapPolicyReconciler) policiesForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	depName := r.deploymentNameForPod(ctx, pod)
	if depName == "" {
		return nil
	}
	return r.policiesForDeploymentName(ctx, pod.Namespace, depName)
}

func (r *HotSwapPolicyReconciler) policiesForDeploymentName(ctx context.Context, ns, name string) []reconcile.Request {
	var list hotswapv1alpha1.HotSwapPolicyList
	if err := r.List(ctx, &list, client.InNamespace(ns)); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range list.Items {
		if list.Items[i].Spec.TargetRef.Name == name {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: list.Items[i].Name}})
		}
	}
	return reqs
}

// deploymentNameForPod walks pod -> ReplicaSet -> Deployment owner references.
func (r *HotSwapPolicyReconciler) deploymentNameForPod(ctx context.Context, pod *corev1.Pod) string {
	rsName := ""
	for _, o := range pod.OwnerReferences {
		if o.Kind == "ReplicaSet" {
			rsName = o.Name
			break
		}
	}
	if rsName == "" {
		return ""
	}
	var rs appsv1.ReplicaSet
	if err := r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: rsName}, &rs); err != nil {
		return ""
	}
	for _, o := range rs.OwnerReferences {
		if o.Kind == "Deployment" {
			return o.Name
		}
	}
	return ""
}
