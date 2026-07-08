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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// RemediationStrategy selects how an unhealthy target is replaced.
// +kubebuilder:validation:Enum=Auto;RolloutRestart;DeletePod
type RemediationStrategy string

const (
	// StrategyAuto picks RolloutRestart when the target runs a single replica
	// and DeletePod when it runs more than one.
	StrategyAuto RemediationStrategy = "Auto"
	// StrategyRolloutRestart triggers the Deployment's own rolling update by
	// patching a pod-template annotation.
	StrategyRolloutRestart RemediationStrategy = "RolloutRestart"
	// StrategyDeletePod deletes the offending pod and lets the ReplicaSet
	// recreate it.
	StrategyDeletePod RemediationStrategy = "DeletePod"
)

// TargetReference identifies the workload a HotSwapPolicy watches. Only
// apps/v1 Deployments are supported today.
type TargetReference struct {
	// +kubebuilder:default="apps/v1"
	// +optional
	APIVersion string `json:"apiVersion,omitempty"`
	// +kubebuilder:default="Deployment"
	// +optional
	Kind string `json:"kind,omitempty"`
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

// RemediationSpec configures how and when hotswap replaces an unhealthy pod.
type RemediationSpec struct {
	// Strategy selects the replacement mechanism.
	// +kubebuilder:default=Auto
	// +optional
	Strategy RemediationStrategy `json:"strategy,omitempty"`

	// RequireStable gates remediation on the target Deployment being settled
	// (not mid-rollout). This is what keeps hotswap from overlapping with
	// normal deploys and from re-firing during its own rollout.
	// +kubebuilder:default=true
	// +optional
	RequireStable *bool `json:"requireStable,omitempty"`

	// MaxConsecutiveAttempts trips the circuit breaker once this many
	// remediations happen inside CooldownSeconds without recovery.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxConsecutiveAttempts int32 `json:"maxConsecutiveAttempts,omitempty"`

	// CooldownSeconds is the minimum spacing between remediations and the
	// window used to reset the circuit breaker after recovery.
	// +kubebuilder:default=600
	// +kubebuilder:validation:Minimum=0
	// +optional
	CooldownSeconds int32 `json:"cooldownSeconds,omitempty"`

	// MaxConcurrent caps how many targets are remediated at once via DeletePod
	// (only meaningful when the Deployment runs more than one replica), so
	// hotswap never deletes every replica simultaneously.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxConcurrent int32 `json:"maxConcurrent,omitempty"`

	// SystemicFailureSkipPercent is a safety valve: if at least this percentage
	// of targets are unhealthy at the same time, hotswap treats it as a
	// systemic failure (dependency outage / lost network reachability), skips
	// remediation, and reports Degraded instead of replacing everything.
	// +kubebuilder:default=50
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +optional
	SystemicFailureSkipPercent int32 `json:"systemicFailureSkipPercent,omitempty"`

	// DryRun, when true, runs the full remediation decision (grace, stability
	// gate, systemic valve, circuit breaker, cooldown) and records/events/
	// metrics what it *would* do, but never patches a Deployment or deletes a
	// pod. Use it to observe hotswap's behavior before enabling real
	// remediation.
	// +kubebuilder:default=false
	// +optional
	DryRun bool `json:"dryRun,omitempty"`
}

// HotSwapPolicySpec defines the desired state of HotSwapPolicy.
type HotSwapPolicySpec struct {
	// TargetRef references the Deployment this policy guards. The policy does
	// not own the Deployment or its pods; it only drives the Deployment's own
	// rolling update.
	// +kubebuilder:validation:Required
	TargetRef TargetReference `json:"targetRef"`

	// HotswapProbe is a core/v1 Probe - the SAME schema as a container's
	// livenessProbe, so a chart's liveness block can be copied verbatim. Only
	// httpGet is supported; exec/tcp/grpc are rejected during reconcile. Unset
	// timing fields are normalized by the controller to the kubelet's probe
	// defaults (periodSeconds=10, timeoutSeconds=1, successThreshold=1,
	// failureThreshold=3, initialDelaySeconds=0).
	// +kubebuilder:validation:Required
	HotswapProbe corev1.Probe `json:"hotswapProbe"`

	// Remediation configures how and when unhealthy pods are replaced.
	// +optional
	Remediation RemediationSpec `json:"remediation,omitempty"`
}

// TargetHealth is the per-pod health snapshot recorded in status.
type TargetHealth struct {
	PodName string `json:"podName"`
	// +optional
	PodIP string `json:"podIP,omitempty"`
	// Healthy is the current probe verdict for this pod.
	Healthy bool `json:"healthy"`
	// ConsecutiveFailures counts probe failures since the last success.
	// +optional
	ConsecutiveFailures int32 `json:"consecutiveFailures,omitempty"`
	// +optional
	LastProbeTime *metav1.Time `json:"lastProbeTime,omitempty"`
	// +optional
	LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`
}

// RemediationRecord captures the most recent remediation action.
type RemediationRecord struct {
	PodName  string              `json:"podName"`
	Strategy RemediationStrategy `json:"strategy"`
	Time     metav1.Time         `json:"time"`
	// +optional
	Reason string `json:"reason,omitempty"`
}

// HotSwapPolicyStatus defines the observed state of HotSwapPolicy.
type HotSwapPolicyStatus struct {
	// ObservedGeneration is the .metadata.generation the status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Targets is the per-pod health snapshot the controller last observed.
	// +listType=map
	// +listMapKey=podName
	// +optional
	Targets []TargetHealth `json:"targets,omitempty"`

	// LastRemediation records the most recent replacement action.
	// +optional
	LastRemediation *RemediationRecord `json:"lastRemediation,omitempty"`

	// ConsecutiveAttempts drives the circuit breaker.
	// +optional
	ConsecutiveAttempts int32 `json:"consecutiveAttempts,omitempty"`

	// Conditions represent the current state of the HotSwapPolicy.
	// Types: Healthy, Remediating, Degraded.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Condition types and reasons used by the controller.
const (
	ConditionHealthy     = "Healthy"
	ConditionRemediating = "Remediating"
	ConditionDegraded    = "Degraded"

	ReasonAllHealthy       = "AllHealthy"
	ReasonRemediating      = "Remediating"
	ReasonDryRun           = "DryRun"
	ReasonTargetNotFound   = "TargetNotFound"
	ReasonCircuitOpen      = "CircuitOpen"
	ReasonRolloutStalled   = "RolloutStalled"
	ReasonSystemicFailure  = "SystemicFailure"
	ReasonConflictingPolic = "ConflictingPolicy"
	ReasonInvalidProbe     = "InvalidProbe"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=hsp
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetRef.name`
// +kubebuilder:printcolumn:name="Healthy",type=string,JSONPath=`.status.conditions[?(@.type=="Healthy")].status`
// +kubebuilder:printcolumn:name="Attempts",type=integer,JSONPath=`.status.consecutiveAttempts`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// HotSwapPolicy is the Schema for the hotswappolicies API.
type HotSwapPolicy struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of HotSwapPolicy
	// +required
	Spec HotSwapPolicySpec `json:"spec"`

	// status defines the observed state of HotSwapPolicy
	// +optional
	Status HotSwapPolicyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// HotSwapPolicyList contains a list of HotSwapPolicy.
type HotSwapPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []HotSwapPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &HotSwapPolicy{}, &HotSwapPolicyList{})
		return nil
	})
}
