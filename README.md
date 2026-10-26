# warmpool

A Kubernetes controller POC that maintains **warm workers**: ready, unclaimed Pods that can be
assigned without waiting for a new Pod to start.

The namespaced resources use API version `linkedin.com/v1alpha1`:

- **WorkerPool** defines a Pod template and the desired number of ready, unclaimed workers.
- **WorkerClaim** requests one ready worker from a pool in the same namespace.

Both reconcilers are implemented. They handle asynchronous readiness, optimistic-concurrency
conflicts, durable bindings, terminal Pod outcomes, claim cleanup, and pool deletion. This is not a
production scheduler; see [limitations](#limitations-and-deferred-work).

## Prerequisites

- Go **1.26.5 or newer**, matching the minimum declared in [go.mod](go.mod).
- `make`, `kubectl`, `kind`, and a running Docker daemon for the local-cluster examples.
- Access to download Go dependencies, envtest binaries, and the sample `nginx:alpine` image.

The Makefile installs `controller-gen` and `setup-envtest` into the ignored `bin/` directory as
needed. The optional admission policies require the
`admissionregistration.k8s.io/v1` `ValidatingAdmissionPolicy` API.

## Quick start

Run from the repository root. These examples assume a fresh demo namespace and no other controller
manager already running against the cluster.

In the controller terminal:

```bash
kind create cluster --name warm-pool   # Skip if this cluster already exists.
kubectl config use-context kind-warm-pool
make install                          # Install the CRDs, not a controller Deployment.
make run                              # Run both controllers locally in one foreground process.
```

**The Makefile's Kubernetes targets use the current kubeconfig context.** Select the local context
before running them; do not run these examples against a production cluster.

In another terminal, create just the pool first:

```bash
kubectl config use-context kind-warm-pool
make namespace
kubectl --context kind-warm-pool apply -f config/samples/workerpool.yaml
kubectl --context kind-warm-pool -n warmpool-demo wait workerpool/default \
  --for=jsonpath='{.status.warmReplicas}'=3 --timeout=120s
kubectl --context kind-warm-pool -n warmpool-demo get workerpools
kubectl --context kind-warm-pool -n warmpool-demo get pods --show-labels
```

After convergence, the pool has **3 warm, 0 warming, 0 claimed, and 3 total counted Pods**.
Readiness depends on image pulls, scheduling, and the worker's readiness probe.

Now request a worker:

```bash
kubectl --context kind-warm-pool apply -f config/samples/workerclaim.yaml
kubectl --context kind-warm-pool -n warmpool-demo wait workerclaim/demo-claim \
  --for=jsonpath='{.status.phase}'=Claimed --timeout=120s
kubectl --context kind-warm-pool -n warmpool-demo wait workerpool/default \
  --for=jsonpath='{.status.claimedReplicas}'=1 --timeout=120s
kubectl --context kind-warm-pool -n warmpool-demo wait workerpool/default \
  --for=jsonpath='{.status.warmReplicas}'=3 --timeout=120s
kubectl --context kind-warm-pool -n warmpool-demo get workerclaim demo-claim -o yaml
kubectl --context kind-warm-pool -n warmpool-demo get workerpools
kubectl --context kind-warm-pool -n warmpool-demo get pods --show-labels
```

The claim reports `Claimed / Assigned` with a `podName`. Once the replacement is ready:

| Desired warm | Warm | Warming | Claimed | Total counted Pods |
|---|---|---|---|---|
| 3 | 3 | 0 | 1 | 4 |

**Shortcut:** `make samples` creates the namespace and applies **both** the pool and claim manifests.
Its expected steady state is the four-Pod state above, not the pool-only three-Pod state.

`make install` uses server-side apply because the generated WorkerPool CRD embeds a large PodSpec
schema. It does not install RBAC bindings, admission policies, or a controller workload. The
repository includes a generated ClusterRole, but not a Dockerfile, Deployment, ServiceAccount, or
ClusterRoleBinding for an in-cluster installation.

## API

### WorkerPool

| Spec field | Default | Meaning |
|---|---|---|
| `warmReplicas` | `1` | Desired ready, unclaimed capacity; a non-negative int32. An explicit `0` is preserved. |
| `template` | Required | `{metadata, spec}`; `spec` is a Kubernetes PodSpec. Template metadata supports labels and annotations. |
| `claimPolicy` | `Delete` | The only supported value. Deleting a claim requests deletion of its bound worker; workers are not recycled. |

The sample explicitly sets `warmReplicas: 3`. The default of `1` applies only when that field is
omitted. The desired count is **not** a guarantee that capacity is ready at every instant.

Pool membership comes from the controlling WorkerPool owner reference and its UID. Merely carrying
the pool label does not make a Pod a member. All status counters below exclude terminating,
`Failed`, and `Succeeded` Pods:

| Status field | Meaning |
|---|---|
| `warmReplicas` | Running, Ready=True, and no non-empty claim label. |
| `claimedReplicas` | Pods with a non-empty claim label, regardless of readiness. This counts Pods, not claim objects. |
| `warmingReplicas` | Unclaimed Pods that are not ready, including Pods that lost readiness. |
| `totalPods` | Sum of warm, claimed, and warming counts; not a count of every existing Pod object. |
| `observedGeneration` | Pool spec generation used by the reconciliation that computed the status. |
| `conditions` | The controller currently reports `WarmCapacityReady`. |

```text
status.totalPods =
    status.warmReplicas + status.claimedReplicas + status.warmingReplicas
```

For example, at steady state, a target of three warm workers plus two active claimed workers
produces five counted Pods. Terminal or terminating Pod objects can still exist outside that count.

`WarmCapacityReady` is true when observed warm capacity meets the target and the current reconcile
has no Pod create/delete error. Reasons include `AtCapacity`, `Warming`, `ScalingUp`,
`PodCreateFailed`, and `PodDeleteFailed`. `TemplateHealthy` is declared in the Go API but is **not
currently reported**.

### WorkerClaim

`spec.poolName` is required, non-empty, and immutable. It names a WorkerPool in the claim's namespace;
cross-namespace claims are not supported.

| Phase | Reason | Meaning |
|---|---|---|
| `Pending` | `PoolNotFound` | No pool of that name exists in this namespace. |
| `Pending` | `PoolTerminating` | The pool is being deleted; no new worker is allocated from that observed state. |
| `Pending` | `WaitingForWarmPod` | No worker was allocated from the available candidates. |
| `Claimed` | `Assigned` | A Pod binding has been published in claim status. |
| `Failed` | `PodLost` | The bound Pod failed or is deleting, or a published binding is missing or mismatched. |
| `Succeeded` | `PodCompleted` | The matching bound Pod was observed in phase Succeeded. |

New claims can have an empty status before reconciliation. They need not pass through a visible
`Pending` state before assignment.

`status.podName` identifies the assigned or last bound Pod. It is retained on terminal claims, even
after the Pod is gone. `status.assignedAt` records when `Claimed` was first published, not the exact
time of the earlier Pod reservation. It is retained if present, but remains absent when recovery
publishes a terminal outcome without first publishing `Claimed`. `reason` and `message` explain the
current state; there is no `podIP` status field.

`Claimed` does not guarantee continuing readiness. Readiness loss alone does not terminate a claim.
`Failed` and `Succeeded` are terminal: the controller does not assign those claims another worker.
Create a new claim, or delete and recreate the old one, to request another assignment.

## How the pool reconciler works

The implementation is in [workerpool_controller.go](internal/controller/workerpool_controller.go)
and [pod.go](internal/controller/pod.go).

1. Ignore a deleting pool; otherwise list owned Pods through the cached owner-UID field index.
2. Classify Pods in priority order: terminating, terminal, claimed, warm, warming.
3. Compute `desiredWarm - (warm + warming)`. A positive result creates that many Pods from the
   template. New Pods use `generateName`, a controlling owner reference, and controller metadata.
4. A negative result deletes surplus unclaimed Pods: currently not-ready Pods first, then newer
   Pods within each readiness class, with name as the final tie-break.
5. Update pool status when its compared counters, observed generation, or condition values change.

**Warming Pods are supply, but not warm capacity.** Including them in creation decisions prevents
repeatedly creating replacements while workers are starting. It also means a stuck, not-ready Pod
can occupy a supply slot indefinitely; there is no warmup timeout.

Each scale-down delete carries Pod UID and resourceVersion preconditions. If a claim binds that Pod
first, the stale delete is rejected. If deletion wins first, a stale allocation update is rejected.
This protects the claimed worker, but does not make the entire scale-down pass transactional.

**Known deferred race:** after a Conflict or NotFound, the deletion loop continues with its original
surplus budget. Concurrent claims can make that budget stale, causing another unclaimed Pod to be
deleted unnecessarily. A later reconcile replenishes the deficit; readiness determines how soon
warm capacity recovers.

Pool and owned-Pod events trigger reconciliation. A default five-minute resync provides a periodic
backup. Template changes affect newly created Pods; existing workers are not automatically rolled.

## Binding, recovery, and cleanup

The implementation is in [workerclaim_controller.go](internal/controller/workerclaim_controller.go).

The controller persists a claim finalizer before reserving a worker. Before new allocation, it
reads the claim and its existing bindings directly from the API server, bypassing the informer
cache. Binding lookup requires both the claim-name label and the exact claim-UID annotation.
Missing or mismatched UID annotations are not adopted or deleted as that claim's bindings.

If no existing binding or published assignment needs handling, the controller reads the pool
directly from the API server. Missing or deleting pools leave the claim Pending. Pod events and
pool lifecycle events wake waiting claims, including when a pool is recreated under the same name.

Allocation selects ready, unclaimed Pods from the cached owner-UID index, oldest by creation time
first. It writes the following metadata in one resourceVersion-checked Pod update:

| Metadata | Value |
|---|---|
| Label `warmpool.linkedin.com/claim` | Claim name |
| Label `warmpool.linkedin.com/state` | `claimed` |
| Annotation `warmpool.linkedin.com/claim-uid` | Claim UID |

Only then does it publish `Claimed`, `podName`, and `assignedAt`. A Conflict or NotFound during the
Pod update causes the allocator to try another candidate.

### Recovery and terminal outcomes

An existing matching binding is handled before any new allocation:

- A non-terminal, non-terminating bound Pod is adopted or retained, even if it is no longer ready.
- A bound Pod observed as `Succeeded` produces `Succeeded / PodCompleted`. This takes precedence
  over its deletion timestamp when both are visible.
- A bound Pod observed as `Failed` or deleting produces `Failed / PodLost`, unless the Succeeded
  case above applies.
- A published `podName` with no matching binding, or a different bound Pod name, produces
  `Failed / PodLost`.

These checks also apply to an **unpublished** binding found after restart. A terminal bound Pod is
reported directly as Failed or Succeeded, not swept and replaced. Once a claim is terminal, later
Pod deletion does not change its recorded outcome.

There is no atomic transaction across the Pod and claim objects. If a reserved Pod disappears
entirely before `podName` was published, neither object retains evidence of that reservation; the
controller can allocate another worker. It cannot infer an unobserved Pod completion after the Pod
has vanished.

If **more than one Pod** matches a claim's name and UID, adoption returns an error regardless of
those Pods' phases. The controller does not choose a winner or delete duplicates during adoption.
The error is logged and reconciliation is retried; existing claim status is left unchanged, so it
does not by itself expose this error.

### Deleting claims and pools

Pods remain owned by their WorkerPool after assignment. A deleting claim's finalizer requests
deletion of **all matching bindings**, with Pod UID and resourceVersion preconditions. Missing or
already-terminating Pods need no further delete request. The finalizer is removed after the
requests succeed; it does not wait for Pod termination or other Pod finalizers to finish.

Claim deletion never returns a used worker to the warm set. Warm capacity is replenished when the
worker is claimed, rather than when the claim is deleted.

Default Kubernetes garbage collection on **pool deletion includes claimed Pods**. Existing bindings
continue to follow their Pod lifecycle while the pool is deleting. Unassigned claims report
`PoolTerminating` while deletion is observed, then `PoolNotFound` once the pool is gone. A recreated
pool has a new UID; it does not inherit the old pool's Pods.

The uncached pool check is not a lock: deletion can begin after that check and before reservation.
The normal binding/Pod-loss handling applies to that race.

## Leader election and runtime

`make run` starts one manager containing both reconcilers. Leader election is **enabled by default**
using the Lease `kube-system/warmpool.linkedin.com`. The identity running the manager needs access
to that Lease; kind's default admin kubeconfig supplies it.

Pod resourceVersion checks arbitrate different claims competing for the same Pod. One elected
manager and per-key reconciliation serialization prevent overlapping allocation attempts for the
same claim during normal operation. Do not disable election when running multiple managers against
the same resources. Election is not a cross-object transaction or a guarantee against arbitrary
split-brain conditions.

Flags can be passed with `go run ./cmd/manager`:

| Flag | Default | Purpose |
|---|---|---|
| `--leader-elect` | `true` | Participate in leader election. |
| `--health-probe-bind-address` | `:8081` | Serve `/healthz` and `/readyz`. |
| `--metrics-bind-address` | `0` | Metrics listener; `0` disables it. |

Use distinct health-probe ports for multiple local manager processes. The health checks are simple
ping checks, not proof of Lease ownership or warm capacity. No custom warm-pool metrics are
implemented; logs, Kubernetes Events, and resource status provide the current observability.

## Protecting reserved metadata

The CRD rejects labels and annotations under `warmpool.linkedin.com/` in
`spec.template.metadata`. Ordinary template labels such as `app: warm-worker` are allowed.
This validation does **not** restrict direct writes to Pods.

The optional policies in [config/policy](config/policy) restrict reserved **Pod labels** on CREATE
and UPDATE:

```bash
make policy-install-dev   # Local kind only: also allows kubernetes-admin.
```

The non-dev variant, installed with `make policy-install`, allows
`system:serviceaccount:warmpool-system:warmpool-controller`. That ServiceAccount is not created by
this repository's Makefile. Do not apply that variant to the local `make run` workflow unless the
allowed identity matches the kubeconfig user.

Cluster-admin privileges do not bypass this validation. The dev exemption also allows humans
using kind's `kubernetes-admin` identity to change reserved labels; it is not suitable for a shared
cluster. The policies share the same resource names, so applying one replaces the other's settings.

The policies do not protect annotations or owner references. Binding metadata must be managed by
trusted actors; these policies are not a complete tenant-isolation mechanism. The controller does
not repair edited routing labels.

## Development and tests

```bash
make generate     # Regenerate DeepCopy methods.
make manifests    # Regenerate CRDs and the ClusterRole.
make build        # Format, vet, and compile packages; does not produce a named manager image/binary.
make test         # Generate, format, vet, and run all Go tests against envtest.
```

`make test` uses a real API server and etcd, with envtest Kubernetes **1.31.0** by default. It does not
require Docker or use the current kind cluster. Envtest has no kubelet, scheduler, or garbage
collector; tests explicitly drive Pod status where needed.

The repository tests cover API defaulting/validation, pool status and capacity accounting, template
copying, scale-up/down, ownership-based membership, claim uniqueness, readiness, terminal Pod
outcomes, claim-name reuse, cleanup conflicts, deleting/recreated pools, and cold reconciliation of
unpublished terminal bindings. Tests are under [api/v1alpha1](api/v1alpha1) and
[internal/controller](internal/controller).

### Six manual acceptance scenarios

Use the fresh-cluster quick start above. Its pool-only step verifies **three ready, unclaimed
workers**. Its claim step verifies **one assigned worker** and **replenishment back to three warm
workers**.

For **several claims with distinct workers**, continue in the resource-inspection terminal:

```bash
for i in 1 2 3; do
  cat <<EOF | kubectl --context kind-warm-pool apply -f -
apiVersion: linkedin.com/v1alpha1
kind: WorkerClaim
metadata:
  name: extra-$i
  namespace: warmpool-demo
spec:
  poolName: default
EOF
done
kubectl --context kind-warm-pool -n warmpool-demo wait \
  workerclaim/extra-1 workerclaim/extra-2 workerclaim/extra-3 \
  --for=jsonpath='{.status.phase}'=Claimed --timeout=120s
kubectl --context kind-warm-pool -n warmpool-demo get workerclaims \
  -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,POD:.status.podName
kubectl --context kind-warm-pool -n warmpool-demo get pods --show-labels
```

All four claims, including `demo-claim`, should name different Pods. Once replenished, the pool has
three warm workers, four claimed workers, and seven counted Pods.

For **assigned-Pod deletion**:

```bash
POD=$(kubectl --context kind-warm-pool -n warmpool-demo get workerclaim demo-claim \
  -o jsonpath='{.status.podName}')
kubectl --context kind-warm-pool -n warmpool-demo delete pod "$POD"
kubectl --context kind-warm-pool -n warmpool-demo wait workerclaim/demo-claim \
  --for=jsonpath='{.status.phase}'=Failed --timeout=120s
kubectl --context kind-warm-pool -n warmpool-demo get workerclaim demo-claim -o yaml
```

Expect `Failed / PodLost` with the original `podName` retained and no replacement assigned to that
claim. Reapplying the same claim manifest does not reset a terminal claim.

For **controller restart**, first record the remaining assignments:

```bash
kubectl --context kind-warm-pool -n warmpool-demo get workerclaims \
  --sort-by=.metadata.name \
  -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,POD:.status.podName
```

Stop `make run` with Ctrl-C in the controller terminal, then start `make run` again. Wait for it to
acquire the Lease and start its controllers, then repeat the query above. The remaining Claimed
claims should retain their Pod names and bindings; `demo-claim` should remain Failed. This procedure
assumes there is no other manager continuing reconciliation while the local process is stopped.

For additional lifecycle checks, deleting a claim should delete its bound Pod. Testing actual
`Succeeded` or `Failed` Pod phases requires an appropriate worker template, such as
`restartPolicy: Never` with a controlled exit code. The nginx sample uses Kubernetes' default
`restartPolicy: Always`; a container exit there can cause a restart instead of a terminal Pod phase.

## Limitations and deferred work

- No warmup timeout or claim wait timeout. Unready supply and pending claims can remain indefinitely.
- No orphan sweeper, duplicate-binding repair, or terminal-Pod sweep outside claim/pool deletion.
  Stale/missing-UID bindings are left untouched. Pool deletion with
  `--cascade=orphan` can leave workers requiring manual cleanup.
- No automatic rollout of existing Pods after a template change.
- Scale-down can temporarily remove too much unclaimed capacity after a conflict; the stale-budget
  optimization is deferred.
- No atomic transaction spanning pool, claim, and Pod writes, and no durable reservation history
  once an unpublished bound Pod has disappeared.
- No per-reconcile create/delete batch cap, custom capacity metrics, or packaged in-cluster
  deployment.

`TODO.md` contains historical planning notes and is not an up-to-date implementation checklist.
The behavior described here is based on the current controllers, API types, and manifests.

## Cleanup

While the controller is running, remove the demo resources with:

```bash
kubectl --context kind-warm-pool delete namespace warmpool-demo
```

This deletes everything in that demo namespace. To remove the **entire local `warm-pool` cluster**,
stop the foreground manager and run:

```bash
make kind-down
```
