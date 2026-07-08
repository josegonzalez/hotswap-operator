# hotswap-operator

A Kubernetes operator that recreates ECS-style health-based task replacement for
Deployments: when a running pod fails a health check, hotswap surges a fresh
replacement and retires the old one - **without** interfering with normal
Helm/GitOps deployments.

## Why

Native Kubernetes does not replace a *running* pod that goes unhealthy: a
readiness failure only drains traffic, and a liveness failure restarts the
container in place. ECS, by contrast, treats a task that fails its ALB health
check like a deploy - it surges a healthy replacement, then stops the old task.
hotswap brings that behavior to Deployments.

## How it works

A namespaced `HotSwapPolicy` custom resource *references* an existing Deployment
(the same pattern KEDA's `ScaledObject` uses - it does not own the Deployment or
its pods) and declares an HTTP health probe plus remediation settings.

The controller:

1. **Probes** each target pod itself over HTTP (`hotswapProbe` uses the exact
   schema and defaults of a container `livenessProbe`), running a
   success/failure threshold state machine per pod.
2. Only remediates when the Deployment is **stable** (not mid-rollout), which is
   what keeps it from overlapping with normal deploys.
3. **Remediates** a sustained regression:
   - single replica -> triggers the Deployment's own rolling update by patching
     a `hotswap.io/restarted-at` pod-template annotation (like
     `kubectl rollout restart`), so KEDA and Helm are untouched;
   - multiple replicas -> deletes the offending pod (capped by `maxConcurrent`).
4. Guards against churn with a **circuit breaker** (`maxConsecutiveAttempts` /
   `cooldownSeconds`) and a **systemic safety valve**
   (`systemicFailureSkipPercent`, active only with 2+ targets) that skips
   remediation and alerts when too many pods fail at once (e.g. a dependency
   outage or lost network reachability).

Set `remediation.dryRun: true` to run the full decision pipeline and
record/event/metric (`hotswap_dryrun_total`) what hotswap *would* do - advancing
attempts, cooldown, and the breaker exactly like live mode - **without** patching
a Deployment or deleting a pod. Flip it to `false` to arm remediation.

Health and actions are surfaced in the CR `status` (per-target health,
conditions `Healthy`/`Remediating`/`Degraded`, last remediation) and as
Prometheus metrics (`hotswap_probe_total`, `hotswap_remediations_total`,
`hotswap_circuit_open`, `hotswap_targets_healthy`) plus Kubernetes Events.

See `config/samples/hotswap_v1alpha1_hotswappolicy.yaml` for a complete example.

## Develop

```sh
make test        # unit + reconcile tests
make manifests   # regenerate CRD + RBAC
make build       # build the manager binary
make docker-build IMG=<registry>/hotswap-operator:<tag>
make deploy IMG=<registry>/hotswap-operator:<tag>
```

Only the elected leader probes and remediates (leader election is enabled via
`--leader-elect`).
