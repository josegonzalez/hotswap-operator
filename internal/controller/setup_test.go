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

	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/josegonzalez/hotswap-operator/internal/prober"
)

// TestSetupWithManager wires the controller into a real (but unstarted) manager
// to exercise the watch/source registration. It does not contact an API server.
func TestSetupWithManager(t *testing.T) {
	mgr, err := ctrl.NewManager(&rest.Config{Host: "http://127.0.0.1:1"}, ctrl.Options{
		Scheme:                 testScheme(t),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Skipf("manager unavailable in this environment: %v", err)
	}
	pm := prober.NewManager(prober.NewHTTPChecker(), prober.RealClock{}, logr.Discard())
	r := &HotSwapPolicyReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Prober: pm}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}
}
