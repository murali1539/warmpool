# warmpool

A Kubernetes controller that keeps a pool of **warm** (Ready, unclaimed) Pods and leases them to
claims, so a caller gets a running Pod without paying startup latency.

> **Current state: `WorkerPool` + `WorkerClaim` reconcilers.** The pool creates Pods until the desired
> number of warm Pods exists, deletes surplus unclaimed Pods when that number shrinks, and reports what
> it observes. A `WorkerClaim` binds exactly one warm Pod from a pool, which drops warm capacity and
> makes the pool create a replacement; deleting the claim deletes its Pod. Pods that do not become
> Ready remain warming; the controller does not automatically expire them.

## Prerequisites

`go` 1.23+, `docker`, `kind`, `kubectl`. `controller-gen` and `setup-envtest` are installed
automatically into `./bin` by the Makefile.

## Quick start

```bash
kind create cluster --name warm-pool    # or: make kind-up
make install                            # install CRDs
make run                                # run the controller (foreground)
```

In another terminal:

```bash
make samples                            # namespace + example WorkerPool
kubectl get workerpools -n warmpool-demo
kubectl get pods -n warmpool-demo --show-labels
```

Expected:

```
NAME      DESIRED-WARM   WARM   WARMING   CLAIMED   TOTAL   AGE
default   3              3      0         0         3       20s

NAME            READY   STATUS    RESTARTS   AGE
default-sl6vl   1/1     Running   0          20s
default-spq2q   1/1     Running   0          20s
default-zcx2t   1/1     Running   0          20s
```

Scaling works as expected, and existing Pods are kept rather than replaced:

```bash
kubectl patch workerpool default -n warmpool-demo --type=merge -p '{"spec":{"warmReplicas":5}}'
kubectl get workerpools -n warmpool-demo
# default   5   5   0   0   5
```

Tear down with `make kind-down`.

## Creating a WorkerPool by hand

```bash
kubectl create namespace warmpool-demo

cat <<'EOF' | kubectl apply -f -
apiVersion: linkedin.com/v1alpha1
kind: WorkerPool
metadata:
  name: default
  namespace: warmpool-demo
spec:
  warmReplicas: 3
  template:
    spec:
      containers:
        - name: worker
          image: nginx:alpine
          readinessProbe:
            httpGet: { path: /, port: 80 }
EOF
```

Only `spec.template` is required; everything else is defaulted by the API server:

```bash
kubectl get workerpool default -n warmpool-demo \
  -o jsonpath='{.spec.warmReplicas}{"\t"}{.spec.claimPolicy}{"\n"}'
# 1	Delete
```

Useful commands:

```bash
kubectl get workerpools -n warmpool-demo            # printer columns
kubectl get workerpool default -n warmpool-demo -o yaml
kubectl describe workerpool default -n warmpool-demo
kubectl explain workerpool.spec                     # schema is self-documenting
kubectl delete workerpool default -n warmpool-demo
```

## How the pool reconciler works

One pass:

1. **List owned Pods** through a field index on the controlling ownerReference's **UID**
   (`.metadata.controller.uid`). The query never touches labels, so no label edit can hide a Pod from
   its owner. A hand-written Pod carrying the pool label, or a leftover from a deleted pool of the
   same name, is ignored because its controller UID does not match.
2. **Classify each Pod** into exactly one bucket, as a priority chain:
   `terminating → terminal (Failed/Succeeded) → claimed → warm (Running ∧ Ready) → warming`.
   Order matters — a terminating Pod is terminating even while still Ready, and a dead Pod is dead
   even if it was claimed.
3. **Create the deficit**: `warmReplicas − (warm + warming)`, in one pass.
4. **Or delete the surplus**, when that difference is negative. Only warm and warming Pods are
   eligible — a claimed Pod belongs to its claimant and is never taken. Deletion order gives up the
   cheapest capacity first: warming before warm (a Pod that was never Ready costs nothing to
   discard), newest before oldest within each class, with the Pod name as a final tie-break because
   `creationTimestamp` has one-second granularity. Each delete carries UID and `resourceVersion`
   preconditions, so a Pod claimed between the cache read and the write is rejected rather than
   taken out from under its claimant.
5. **Update status**, but only if something actually changed.

Reconciliation is event-driven — a `spec.warmReplicas` edit bumps the pool's resourceVersion, and a
Pod becoming Ready or disappearing fires the `Owns(Pod)` watch. A 5-minute resync
(`WorkerPoolReconciler.ResyncPeriod`) backs that up for the cases a watch cannot cover, such as a
dropped event.

Two details that are load-bearing:

**Warming Pods count as supply.** Readiness is asynchronous and the reconciler runs repeatedly while
a Pod boots. Counting only *warm* Pods would make every pass see a deficit of 3 and create another
batch — tens of Pods within seconds. `TestPoolDoesNotOvershootWhileWarming` asserts the count stays
put across a window, not merely at one instant.

**Status writes are change-detected.** A status write produces a new `resourceVersion`, which wakes
the pool's own watch, which reconciles, which writes status… An unconditional write is an infinite
loop that looks like it's working. Verified on a live cluster: `resourceVersion` is unchanged after
20 idle seconds, and only two `ScalingUp` Events exist after a create and a scale-up.

Pods use `generateName`, not deterministic names: a pass may create several Pods at once, and a
deterministic name would collide with a Pod of the same name still terminating.

## How the claim reconciler works

A `WorkerClaim` names a pool (`spec.poolName`, immutable) and asks for one warm Pod from it. One pass:

1. **Finalize, if deleting.** Delete the bound Pod, then release the finalizer. A Pod that is already
   gone counts as success, so a claim whose pool was deleted first cannot wedge in `Terminating`.
2. **Add the finalizer**, before any Pod is ever bound. The other order leaves a window where a crash
   loses the only record that this claim owes a Pod deletion, stranding the Pod as claimed forever.
3. **Adopt.** List Pods carrying this claim's label and verify the claim-UID annotation. An existing
   binding is the durable truth and always wins over allocating a new one — that is what makes the
   controller idempotent and what lets a restarted process reconstruct every assignment. Pods that are
   terminating, `Failed` or `Succeeded` can never serve the claim again and are swept. If *two live*
   Pods carry the claim's UID, the pass fails instead: see below.
4. **Decide whether a loss occurred**, if nothing live was adopted (see below).
5. **Allocate.** Pick the oldest warm Pod and write `state=claimed`, the claim label and the claim-UID
   annotation in a single update carrying the Pod's `resourceVersion`. If another claim won the Pod
   first, the API server rejects the write and the next candidate is tried.
6. **Publish status** — `phase: Claimed` and `podName`.

**A lost Pod is not silently replaced.** If the claim already published a `podName`, the client may have
started using that Pod, and the controller cannot know whether it did. Handing over a different Pod would
hide the loss of that work behind a healthy-looking claim, so the claim goes terminally
`phase: Failed`, `reason: PodLost`, keeping `podName` as evidence of which Pod died. Only a claim that
was bound but never published is re-allocated silently — nothing could have been using a Pod whose name
was never visible. A Running Pod that merely goes `Ready=False` is a probe blip, not a loss, and the
claim keeps it.

**A broken invariant stalls the claim; it is not repaired.** Two live Pods carrying one claim's UID
means an assumption elsewhere is already wrong. The controller cannot tell which Pod the client is
using, so deleting either is a guess that may destroy live work, and publishing the survivor would
report a healthy claim built on a broken invariant. `adopt` returns an error, the claim stops making
progress, and the failure shows up in the controller's logs and error metrics. A stalled claim is
recoverable by an operator; a wrong delete is not. Deletion is the one exception — the finalizer
deletes every bound Pod, so there is nothing to guess.

**Binding lookups bypass the informer cache.** Both the adoption list and the published-status read go
straight to the API server. A cached read can miss a Pod this controller bound milliseconds ago, or
still report `Pending` just after `Claimed` was written — no crash required, since each write
immediately re-enqueues the claim. Either stale answer causes a second Pod to be handed out. The
finalizer's list is uncached for the same reason: a miss there would drop the finalizer while the Pod
is still running, and since the pool owns the Pod there would be no GC cascade to collect it.

**The Pod is written before the claim's status.** The binding lives on the Pod; the claim's status is
only a publication of it. A crash between the two leaves a bound Pod whose claim still reads
`Pending`, which the next pass repairs by adoption. The reverse order would produce a claim pointing
at a Pod that never agreed to it.

**One Pod, one claim** rests on the `resourceVersion` precondition, not on a lock. The candidate list
comes from a cache that may be stale, so the write is what arbitrates: whoever gets there first wins,
and the loser moves on to another Pod.

**Claiming triggers replenishment.** Labelling a Pod `claimed` moves it out of `warm`, so the pool
sees a deficit and creates a replacement — `warmReplicas` counts warm Pods, not total Pods. Deleting
a claim does *not* refill anything, because the claimed Pod was never counted as warm; only
`totalPods` drops.

A claim with no capacity stays `Pending` with reason `WaitingForWarmPod` and is woken by the Pod
watch when the pool produces one, so there is no polling interval to tune.

When the controller observes a deleting pool, unassigned claims stay `Pending` with reason
`PoolTerminating`; a missing pool produces `PoolNotFound`. Pool lifecycle events also wake pending
claims, so they can use a pool recreated under the same name. Existing bindings continue to follow
the assigned Pod's lifecycle.

## API

### `spec`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `warmReplicas` | int32, ≥ 0 | `1` | Pods kept **Ready AND unclaimed** |
| `template` | `{metadata, spec}` | *required* | Pod template; `spec` is a standard `PodSpec` |
| `claimPolicy` | `Delete` | `Delete` | Fate of a Pod when its claim is deleted. `Delete` is the only supported value — a claimed Pod is never reused |

**`warmReplicas` is warm headroom, not a total Pod count.** A claimed Pod stops contributing warm
capacity, so the pool creates a replacement:

```
totalPods == warmReplicas + claimedReplicas + warmingReplicas
```

A pool with `warmReplicas: 3` and two claims in flight correctly runs **five** Pods. This is what
makes "warm capacity returns to the desired count after a claim" fall out of the normal control
loop rather than being a special case.

### `status`

There is deliberately **no field named `replicas`**. Since `spec.warmReplicas` redefines "replicas"
to mean warm headroom, a `status.replicas` meaning *total* would silently contradict the spec field
it appears to mirror. Every counter is named for exactly what it counts:

| Field | Counts |
|---|---|
| `warmReplicas` | Ready ∧ unclaimed ∧ not terminating — compare this to `spec.warmReplicas` |
| `claimedReplicas` | assigned to a `WorkerClaim` |
| `warmingReplicas` | created but not Ready yet |
| `totalPods` | real footprint (warm + claimed + warming) |
| `observedGeneration` | spec generation this status reflects |
| `conditions` | `WarmCapacityReady`, `TemplateHealthy` |

`warmingReplicas` is load-bearing, not cosmetic. The create loop computes
`toCreate = warmReplicas(spec) − warmReplicas(status) − warmingReplicas`; subtracting in-flight Pods
is what stops the controller from creating a new Pod on every reconcile while the first is still
booting. It is also the stall signal: `warm: 0, warming: 3` that does not change means Pods cannot
become Ready (bad image, failing probe, unschedulable). Such Pods remain in-flight supply until
they become Ready, terminate, or are deleted; automatic expiration is not supported.

### Reserved prefix

Labels and annotations under `warmpool.linkedin.com/` are **rejected** in `spec.template.metadata`.
The controller stores pool membership and claim assignment there, so a user-settable value would let
a hand-written template forge warm or claimed state:

```bash
$ kubectl apply -f forged-pool.yaml
The WorkerPool "forged" is invalid: spec.template.metadata.labels: Invalid value:
labels under the warmpool.linkedin.com/ prefix are reserved by the controller
```

Ordinary labels (`app: warm-worker`) work normally.

## Development

```bash
make generate     # deepcopy
make manifests    # CRDs + RBAC
make build        # fmt, vet, build
make test         # runs against a real kube-apiserver via envtest (no Docker needed)
make install      # apply CRDs to the current cluster
make uninstall
```

`make install` uses **server-side apply** deliberately: the CRD embeds a full `PodSpec`, and
client-side apply's `last-applied-configuration` annotation exceeds the 256KB limit.

### Tests

`make test` starts a real `kube-apiserver` + `etcd` via envtest and asserts:

**API (`api/v1alpha1`)**

- API-server defaulting (`1` / `Delete`)
- an explicit `warmReplicas: 0` is preserved, not re-defaulted to `1` (the field is a pointer so
  "keep nothing warm" is distinguishable from "unset")
- rejection of negative `warmReplicas`, unknown `claimPolicy`, missing `template`
- rejection of reserved label/annotation prefixes, while ordinary labels are accepted
- `status` is a true subresource: a main-resource write cannot modify status, and a status write
  cannot clobber spec
- the shipped sample manifest parses strictly and is accepted by the API server, so docs cannot
  drift from the types

**Controller (`internal/controller`)**

- `classifyPod` priority chain, including terminating-outranks-ready, terminating-outranks-claimed,
  failed-outranks-claimed, and a stale `Ready=True` condition on a non-Running Pod
- deficit arithmetic across warm / warming / claimed / terminating / terminal mixes
- `buildPod` preserves user labels, stamps controller labels last, and deep-copies the template so a
  built Pod can never alias the cached `WorkerPool`
- *Scenario 1*: `warmReplicas: 3` → 3 Pods, reported `warming=3` before readiness and `warm=3` after
- no overshoot while Pods are warming (asserted over a window)
- scale-up adds Pods instead of replacing existing warm ones
- `warmReplicas: 0` creates nothing
- created Pods carry the controller labels and a controlling ownerReference whose UID matches
- a foreign Pod carrying the pool label but no ownerReference is ignored
- stripping the pool label off an owned Pod does **not** hide it: no replacement is created and the
  counts stay accurate (the controller deliberately does not rewrite the label)
- scaling down deletes exactly the surplus and then stops churning
- scale-down never deletes a claimed Pod, even when the pool is scaled to zero
- deletion order is warming-first, newest-first, and independent of lister order

Note that envtest has no kubelet, so Pods never become Ready on their own — the tests set Pod status
explicitly, which is also what makes the warm/warming distinction testable at all.

## Protecting the reserved labels

`config/policy/` holds a `ValidatingAdmissionPolicy` that restricts the `warmpool.linkedin.com/`
label prefix on Pods to the controller's identity. It is the replacement for the in-controller label
repair loop: repairing drift after the fact re-derived a value the controller never reads, whereas
this stops the write happening at all.

```sh
make policy-install-dev   # kind: also exempts kubernetes-admin, so `make run` works
make policy-install       # real clusters: controller ServiceAccount only
```

Two things worth knowing before applying it:

- **A cluster-admin does not bypass a `ValidatingAdmissionPolicy`.** Only the identities in the
  policy's `allowedUsers` variable may write these keys, so that list has to contain whatever
  identity the controller actually runs as. `make run` runs out-of-cluster under your kubeconfig, so
  on kind that identity is `kubernetes-admin` rather than a ServiceAccount — which is the only
  reason the `-dev` variant exists, and the reason it must not be applied anywhere shared.
- **`CREATE` is covered as well as `UPDATE`.** Guarding updates alone would still let someone create
  a Pod that already carries a claim label and — with an `ownerReference` pointing at the pool — have
  it adopted in a forged state.

Verified on kind: a non-exempt user is denied when setting, changing or removing a reserved label,
and when creating a Pod that carries one; non-reserved labels and ordinary Pods are unaffected; and
the controller keeps creating and deleting Pods normally with the policy active.

## Design notes

**Pod membership is decided by the controlling `ownerReference` alone** — there is no user-supplied
`spec.selector`, and no adoption or release path. Core workload controllers derive membership from a
mutable label selector, which allows overlapping selectors and lets a label edit orphan a Pod into
another controller's scope. For a warm pool that would be a correctness bug rather than a cosmetic
one: a Pod released from one pool and re-adopted by another could be counted warm while still
assigned to a live claim, and handed out twice. Labels here are a human-facing index over that
membership, not an input to it.

**Ownership never transfers to the claim.** A Pod's controlling ownerReference is written once at
creation and never changes; assignment is recorded in labels instead. Moving the ownerRef to the
WorkerClaim would make `kubectl delete workerclaim --cascade=orphan` strand a running, unowned Pod,
would cost the pool its owner-based view of claimed Pods (`status.claimedReplicas`), and would make
membership mutable again. Claim cleanup is therefore a finalizer plus an orphan sweeper rather than a
GC cascade. See `TODO.md` D7 for the experiments behind this.

**Labels are never read to establish membership.** The first version of the reconciler listed Pods
with `MatchingLabels{pool: <name>}` and *then* verified the ownerReference. That is still
selector-based membership wearing a disguise: `kubectl label pod <p> warmpool.linkedin.com/pool-`
made a running, owned Pod invisible, so the pool reported `TOTAL=3` while 4 Pods ran and created a
replacement for a Pod that had never left. The listing path is now a field index on the controller
UID, which no user-writable field can perturb. The reconciler deliberately does **not** write the
label back: nothing in the controller reads it (`classifyPod` never looks at it), so a repair loop
would be re-deriving a value it does not consume. Keeping the label honest is admission's job — the
reserved prefix is rejected in `spec.template.metadata`, and a cluster `ValidatingAdmissionPolicy`
restricts mutation of `warmpool.linkedin.com/` keys to the controller's ServiceAccount.
`TestStrippingPoolLabelDoesNotHidePod` pins this.
PoolLabelDoesNotHidePod` pins this.

## Status / roadmap

Done: `WorkerPool` API + validation, label scheme, create-only pool reconciler, status and
conditions, envtest + unit suites.
Next: `WorkerClaim` API, claim assignment with optimistic concurrency, Pod failure recovery, claim
finalizer. See `TODO.md`, including §7 for deliberately deferred work.
