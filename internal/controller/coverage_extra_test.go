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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/josegonzalez/hotswap-operator/internal/prober"
)

// Covers the DeletePod path when the offending pod is already gone: the
// NotFound is tolerated and the attempt still counts.
func TestReconcileDeletePodAlreadyGone(t *testing.T) {
	dep := newDeployment("app", 3, true)
	pol := newPolicy("app")
	probe := &fakeProbe{snapshot: []prober.PodState{
		{PodName: "ghost", Healthy: false, SeenReady: true}, // not present in the client
		{PodName: "p2", Healthy: true, SeenReady: true},
		{PodName: "p3", Healthy: true, SeenReady: true},
	}}
	c, _, err := reconcileOnce(t, []client.Object{dep, pol}, probe)
	if err != nil {
		t.Fatalf("NotFound on delete should not error: %v", err)
	}
	if getPolicy(t, c).Status.ConsecutiveAttempts != 1 {
		t.Fatalf("expected the remediation attempt to be recorded")
	}
}

// Covers lessPolicy's creation-time ordering branch (distinct timestamps).
func TestConflictingPolicyByCreationTime(t *testing.T) {
	polOld := newPolicy("app")
	polOld.Name = "zzz-old" // lexically last, but created first
	polOld.CreationTimestamp = metav1.NewTime(time.Unix(1000, 0))
	polNew := newPolicy("app")
	polNew.Name = "aaa-new" // lexically first, but created later
	polNew.CreationTimestamp = metav1.NewTime(time.Unix(2000, 0))
	dep := newDeployment("app", 1, true)

	r, _ := newReconcilerWith(t, dep, polOld, polNew)
	if w, err := r.conflictingPolicy(context.Background(), polNew); err != nil || w != "zzz-old" {
		t.Fatalf("earliest-created policy should win: got %q,%v", w, err)
	}
}
