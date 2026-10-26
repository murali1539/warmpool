# Warm Pool Controller — Implementation TODO

Tracking doc for the `WorkerPool` / `WorkerClaim` controller POC.
Status legend: `[ ]` pending · `[~]` in progress · `[x]` done · `[!]` blocked

> Keep this file in sync as work progresses. Each item lists the requirement(s) from the
> spec that it satisfies and its prerequisites.

---

## 0. Design decisions (resolve before coding)

| ID | Question | Default proposal | Decided? |
|----|----------|------------------|----------|
| D1 | Assigned Pod lost | Depends on whether the assignment was **published**. Before publication: silently re-allocate. After publication: `phase: Failed`, `reason: PodLost` — terminal, never reassign | ✅ decided |
| D2 | `WorkerClaim` deleted while holding a Pod | **Delete** the Pod via a claim finalizer; pool refills warm capacity. `Recycle` is removed from the API — a claimed Pod is never reused | ✅ decided |
| D3 | Scope | Namespaced CRDs, single cluster, leader election ON | ❓ open |
| D4 | Claim timeout | — | ⏸ deferred, see §7 |
| D5 | Template mutation | — | ⏸ deferred, see §7 |
| D6 | `WorkerPool` deletion | Default ownerRef cascade (claimed Pods die with the pool); documented, not engineered | ⏸ deferred, see §7 |
| D7 | Should the Pod's ownerReference transfer from pool to claim on assignment? | **No.** The pool is the controller owner for the Pod's whole life; assignment lives in labels | ✅ decided |

### D7 in detail — why ownership never transfers to the claim

Assignment could have been expressed by moving the Pod's controlling ownerReference from the
WorkerPool to the WorkerClaim. Five experiments against a live cluster:

| | Experiment | Result |
|---|---|---|
| A | add the claim as a **second** `controller: true` owner | ❌ rejected: `Only one reference can have Controller set to true` — so transfer means *release* |
| B | replace the pool's ownerRef with the claim's | ✅ succeeds, atomically, in one write |
| C | then `delete claim --cascade=orphan` | ☠️ Pod `Running`, `ownerReferences: []` — unreachable garbage |
| D | pool (controller) + claim (non-controller), delete claim, background cascade | Pod survives; ref merely stripped — **no cascade** |
| E | same as D but `--cascade=foreground` with `blockOwnerDeletion: true` | Pod **still** survives |

Test E's reason matters: `blockOwnerDeletion` blocks the *owner's* removal until the dependent is
gone; it never forces the dependent to die. The GC deletes a dependent only when **no solid owner
remains**, so a live WorkerPool ref keeps the Pod alive under either propagation policy. A second
ownerRef therefore cannot produce a cascade — only full transfer can.

**The honest argument for transfer:** with `Recycle` removed, the only release behavior is `Delete`,
which is exactly what a GC cascade does for free. Transfer would delete the claim finalizer entirely,
and GC is server-side — it works even if the controller is permanently down, where a finalizer would
wedge claims in `Terminating`.

**Why we decline it anyway:**

1. **The orphan sweeper neutralizes the benefit.** A sweeper is required regardless, because
   `kubectl delete workerpool --cascade=orphan` leaks every Pod (**reproduced**: 2 Pods orphaned and
   Running; the recreated pool has a new UID, so the owner index can never match them — pool reported
   `TOTAL=3` while 5 Pods ran). Once that sweeper exists it also covers "Pod whose claim is gone", so
   the finalizer is only *promptness*, not correctness. Transfer saves ~20 lines, not a subsystem.
2. **The pool loses its owner-based view of claimed Pods.** *(Design preference, not a requirement —
   Req 9 asks for "enough information", it names no fields.)* After transfer the pool cannot count
   claimed Pods by owner; it would fall back to the pool label (proven tamperable) or to listing
   claims (misses Window-A claims whose status was never published). This matters only for as long as
   we choose to report a claimed count; drop that field and this argument disappears.
3. **Immutable ownership is what makes the owner-UID index authoritative** — the invariant behind the
   label-tampering fix. Transfer makes membership mutable again.
4. **It moves orphan exposure onto the user-facing path.** Both designs leak under `--cascade=orphan`,
   so this is not a clean win — but transfer *adds* a second leak path on the routine operation,
   available to everyone who can delete a claim, while the pool path is rare and admin-privileged.
5. **Cross-namespace claims stay possible.** ownerReferences cannot cross namespaces; transfer would
   foreclose that permanently.

**Conceded in transfer's favour:** it would shrink the pool-orphan blast radius, since claimed Pods
would not be owned by the pool and so would survive `delete workerpool --cascade=orphan`.

**Consequence:** assignment is expressed in Pod *labels* (cheap, reversible, non-destructive to
membership), cleanup is a claim finalizer, and the sweeper is the crash-proof backstop.

### D1 in detail — the publication boundary

Requirement 10 says "before the claim is ready". A claim is never assigned a not-yet-ready Pod, so
there is no warm-up window. The only window the controller can observe is the gap between its two
writes, which have no transaction between them:

1. **write the Pod** — `state: claimed`, `claim: <name>`, claim-UID annotation. The durable record.
2. **write the claim status** — `phase: Claimed`, `podName`, `podIP`. The *publication*.

| | Pod labelled | status published | Pod lost ⇒ |
|---|---|---|---|
| Window A | ✅ | ❌ | **re-allocate silently** — the client never learned the Pod's name or IP, so it cannot have started using it |
| Window B | ✅ | ✅ | **`phase: Failed`, `reason: PodLost`** — terminal |

**Why Window B fails instead of reassigning:** the controller cannot know whether the client began
using the Pod. If it had, and the Pod died, a silent reassignment doesn't just hand over a stale IP —
it *masks data loss*. The client's work is gone, and a fresh Pod makes it look like nothing happened.
Failing loudly surfaces the loss at the one moment the client can still react. A new Pod is the
client's decision, expressed by creating a new `WorkerClaim`.

`podName`/`podIP` are **retained** on a failed claim: the phase already says "do not use this", so
they are forensic evidence identifying *which* Pod died, not a pointer to act on.

**Not a loss:** a Pod that is still `Running` and not terminating but has flipped `Ready=False`
(probe blip, container restart). Failing a claim there would be more disruptive than the blip, and
nothing was swapped. Only deletion, `Failed`, `Succeeded`, or eviction count.

Window A also covers **controller restart**: a Pod labelled for a claim whose status is still
`Pending` is indistinguishable from a crash between write 1 and write 2. The sweeper *republishes*
(completes write 2) rather than re-allocating, since the Pod is already durably bound.

### Core semantics (non-negotiable, from the spec)

- `WorkerPool.spec.replicas` = number of **ready, unclaimed (warm)** Pods, *not* total Pods.
  Therefore `totalPods ≈ replicas + claimedCount`, and claiming a Pod must trigger a replacement. (Req 2, 3)
- A Pod counts as **warm** only if **all** hold: `deletionTimestamp == nil`, `phase == Running`,
  `PodReady == True`, `state == warm`, no claim label, owned by this pool. (Req 4)
- **Durability model (Req 8):** the Pod object is the source of truth. Assignment is written to the
  Pod's labels *before* the claim's status. Any in-memory state is a cache only; a cold start can
  reconstruct every assignment by listing Pods and Claims.

### Label / annotation scheme

```
labels:
  warmpool.linkedin.com/pool:     <poolName>
  warmpool.linkedin.com/state:    warm | claimed
  warmpool.linkedin.com/claim:    <claimName>        # present only when claimed
  warmpool.linkedin.com/template: <templateHash>
annotations:
  warmpool.linkedin.com/claim-uid: <claimUID>        # guards against name reuse
ownerReferences: [ WorkerPool (controller=true) ]
```

### Status field naming (decided)

`status` deliberately contains **no field named `replicas`**. Because `spec.warmReplicas` redefines
"replicas" to mean warm headroom, a `status.replicas` meaning *total* would break the
`status.X` mirrors `spec.X` convention, and the inherited Deployment triple
(`replicas ⊇ readyReplicas ⊇ availableReplicas`) cannot hold once claimed Pods are Ready but excluded.
Every counter is therefore named for exactly what it counts:

| Field | Counts |
|---|---|
| `status.warmReplicas` | Ready ∧ unclaimed ∧ not terminating — compares directly to `spec.warmReplicas` |
| `status.claimedReplicas` | assigned to a `WorkerClaim` |
| `status.warmingReplicas` | created, not Ready yet (in-flight supply; the overshoot guard and the stall signal) |
| `status.totalPods` | real footprint = warm + claimed + warming |

---

## 1. Foundation

- [x] **scaffold-project** — hand-rolled layout (no kubebuilder binary needed): `go.mod`, `api/v1alpha1`,
      `internal/controller`, `config/{crd,rbac,samples}`, `Makefile` with `generate`/`manifests`/`build`/
      `test`/`install`/`run`/`kind-up`/`samples`. `controller-gen` and `setup-envtest` are pinned and
      installed into `./bin` on demand. *(Req 1)*
- [x] **api-types** — **WorkerPool done**: `spec.{warmReplicas,template,claimPolicy}`,
      `status.{warmReplicas,claimedReplicas,warmingReplicas,totalPods,observedGeneration,conditions}`,
      printer columns, status subresource, deepcopy + CRD generated and verified against a live API
      server. **WorkerClaim done**: `spec.poolName` (immutable via CEL),
      `status.{phase,podName,reason,message,assignedAt}`, printer columns, `wc` short name.
      *(Req 9)* — deps: scaffold-project
- [x] **api-validation** — `warmReplicas` minimum=0 + default 1 (pointer, so an explicit `0` is
      distinguishable from unset), `claimPolicy` enum default `Delete`, `template` required.
      All enforced by the API server, proven by envtest tests. — deps: api-types
- [x] **label-scheme** ⭐ — `api/v1alpha1/labels.go`: pool/state/claim/template-hash labels, claim-UID
      annotation, reserved prefix (enforced by the API server via CEL). Membership is the controlling
      ownerReference; labels are a read-only index as far as the reconciler is concerned. *(Req 8)*
- [~] **field-indexes** — Pod-by-controller-UID index (`.metadata.controller.uid`) is **done** and is
      the pool's only membership lookup. Still to do: index Pods by claim label and Claims by
      `spec.poolName` for the claim controller. — deps: label-scheme
- [ ] **manifests-rbac** — `config/crd`, `config/rbac` (pods create/delete/patch/list/watch, CR status),
      `config/samples/workerpool.yaml` + `workerclaim.yaml`, kustomize wiring. *(Req 1, 12)* — deps: api-types

## 2. WorkerPool controller

- [x] **warm-counting** ⭐ — `classifyPod` priority chain (terminating → terminal → claimed → warm →
      warming) and `poolCounts{warm,warming,claimed,terminating,terminal}` with `supply()`/`total()`.
      Foreign Pods carrying the label but not the ownerRef UID are excluded. *(Req 4)* — deps: label-scheme
- [x] **label-tamper-repair** ⭐ — listing is by controller-UID field index, never by label, so no label
      edit can hide an owned Pod. Fixes a reproduced bug where stripping the pool label made a running
      Pod invisible and the pool created a phantom replacement. *(Req 4, 8)* — deps: label-scheme

      The original `repairLabels` patch loop has been **removed**. It was re-deriving a value the
      controller never reads — `classifyPod` buckets on `deletionTimestamp`, `status.phase`, the claim
      label and readiness, never on `pool`/`state` — so repairing it protected nothing the index was
      not already protecting. Prevention moved to admission: the reserved prefix is rejected in
      `spec.template.metadata`, and a cluster `ValidatingAdmissionPolicy` restricts CREATE/UPDATE of
      `warmpool.linkedin.com/` keys on Pods to the controller's ServiceAccount. Residual gap, accepted:
      if that policy is absent or misscoped, the labels can drift and nothing reports it. Worth a
      drift metric if the policy is ever not a hard cluster prerequisite.
- [x] **pod-scale-down** — promoted out of §7 and implemented. `surplusCandidates` + `deleteSurplusPods`
      shed `(warm + warming) - desiredWarm` Pods in one pass. Claimed Pods are never eligible; terminating and terminal Pods are skipped because they
      are already not supply. Order is warming-before-warm, then newest-first, then by name — the name
      tie-break matters because `creationTimestamp` has one-second granularity and a pool creates
      several Pods inside one second. Deletes carry UID + `resourceVersion` preconditions so a Pod
      claimed between the cache read and the write is rejected rather than taken from its claimant; a
      rejected delete needs no explicit retry because the write that caused the conflict fires the
      `Owns(Pod)` watch. — deps: warm-counting, pod-creation
- [x] **pool-resync** — `WorkerPoolReconciler.ResyncPeriod` (default 5m, negative disables) returns a
      `RequeueAfter` so a pool is re-reconciled without a watch event. A safety net only: spec edits and
      Pod transitions are already events.
- [x] **pod-creation** — `buildPod` + create loop. Creates `desiredWarm - (warm + warming)` in one
      pass. Uses `generateName` (a deterministic name would
      collide with a Pod still terminating) and deep-copies the template so a built Pod can never alias
      the cached WorkerPool. *(Req 3)* — deps: label-scheme, api-types
- [ ] **creation-backoff** — cap Pods created per reconcile, workqueue rate limiting, Events on
      repeated create failures (quota, bad image, invalid template) instead of hot-looping the API server. — deps: pod-creation
- [x] **pool-status** — warm/warming/claimed/totalPods + `WarmCapacityReady` condition +
      `observedGeneration`, written via the status subresource with `RetryOnConflict` and a
      change-detecting guard (an unconditional write would wake the pool's own watch and spin).
      *(Req 9)* — deps: warm-counting
- [x] **replicas-zero** — `warmReplicas: 0` creates nothing and reports AtCapacity; covered by test. — deps: pod-creation

## 3. WorkerClaim controller

- [x] **claim-adopt** ⭐ — before allocating, list Pods labeled with this claim; adopt and repair status
      if one exists. If `status.podName` is set, verify the Pod still exists and still carries the claim
      label. Makes correctness reconstructable after a restart. *(Req 8)* — deps: label-scheme, field-indexes
      Deviation: adopt does **not** repair a duplicate binding by keeping the oldest and deleting the
      rest. Two live Pods carrying one claim UID means the invariant is already broken; the controller
      cannot know which Pod the client is using, so it returns an error and stalls the claim rather than
      guessing. A stall is operator-recoverable, a wrong delete is not. `finalize` is exempt — it
      deletes every bound Pod, so no choice is involved.
- [x] **claim-allocate** ⭐ — pick one warm Pod and claim it with an optimistic-concurrency update
      (ResourceVersion precondition) setting `state=claimed` + claim label. On `Conflict`/`NotFound`,
      retry with a different Pod. **Pod write happens before claim status write** so a crash never
      produces a phantom claim. *(Req 5)* — deps: warm-counting, claim-adopt
- [x] **claim-uniqueness** ⭐ — all three invariants hold: adoption runs before allocation so a claim
      that already holds a Pod never allocates; candidates are `classifyPod == podWarm` (a claim label
      makes a Pod `podClaimed`) plus a re-check on the object being written; extras are swept in
      `adopt`, keeping the oldest. Extras are **deleted, not released** — a Pod that was ever bound may
      have been touched by the client, so it cannot go back in the warm pool (D2). *(Req 6, 7)* — deps: claim-allocate
- [~] **claim-status** — phases `Pending → Claimed → Failed`; populate `podName`, `reason`, `message`,
      `assignedAt`; Events on transition. **Deviations, both deliberate:** `podIP` is omitted (so
      `claim-pod-ip` below is moot for now), and the claim uses `reason`/`message` rather than
      conditions — it has a single linear lifecycle with mutually exclusive states, which is what a
      phase expresses; `Released` is unreachable because a claimed Pod is deleted, never recycled
      (D2). *(Req 9)* — deps: claim-allocate
- [ ] **claim-pod-ip** — a ready Pod may still have an empty `status.podIP`; requeue until it's populated
      rather than publishing an unusable claim. — deps: claim-allocate
- [x] **claim-no-capacity** — no warm Pod available → stay `Pending` with reason `WaitingForWarmPod`,
      emit Event, requeue with backoff, and be woken by the Pod-ready watch (not just polling). — deps: claim-allocate
- [~] **claim-bad-pool** — **missing pool done** (`Pending`/`PoolNotFound`, re-checked on every pass so
      it self-heals when the pool appears). Still TODO: pool terminating, and cross-namespace →
      re-reconcile if the pool later appears. *(Req 9, 10 — this is the required half of pool-deletion
      handling: a claim must never hang silently when its pool is gone.)* — deps: claim-status
- [x] **claim-pod-failure** ⭐ — watch assigned Pods; on delete/evict/`Failed`/NotReady either re-allocate
      per **D1**: re-allocate if unpublished, else `Failed`/`PodLost` (terminal). A Running Pod that
      merely goes NotReady is **not** a loss — do not fail on a probe blip. *(Req 10)* — deps: claim-status
      Done. `classifyPod` supplies the loss test (`podTerminating`/`podTerminal`); a Running-but-NotReady
      Pod stays held. The Window A/B decision reads the claim **uncached**: the cached copy can still
      say `Pending` milliseconds after `publishClaimed` wrote `Claimed`, and trusting it misclassifies a
      real Window B loss as Window A — i.e. silently rebinds. This was observed, not theorised: with the
      cached read, `TestLosingAPublishedPodFailsTheClaim` reproducibly ended `Claimed` on a *different*
      live Pod once enough earlier tests had loaded the process.
- [x] **claim-finalizer** ⭐ — finalizer on the claim; on delete, **delete** the assigned Pod per **D2**
      (there is no release-to-warm path). A finalizer is required because the pool — not the claim —
      owns the Pod, so there is no GC cascade to inherit. Idempotent when the Pod is already gone;
      must not deadlock if the pool was deleted first. Needs RBAC on `workerclaims/finalizers`.
      *(Req 11)* — deps: claim-allocate
- [ ] **orphan-sweeper** ⭐ — two jobs:
      (a) a Pod labeled for a claim that no longer exists (or whose UID differs) must be
      deleted/released — covers the crash window between Pod labeling and claim-status write, plus
      missed delete events;
      (b) **ownerless Pods**: a Pod carrying `warmpool.linkedin.com/pool` with **no controller
      ownerReference** is unreachable garbage and must be deleted. Produced by
      `kubectl delete workerpool --cascade=orphan` (**reproduced**: 2 Pods orphaned, still Running;
      the recreated pool has a new UID so the owner index can never match them — pool reported
      `TOTAL=3` while 5 Pods ran). Sweep on the *label*, which is the only remaining trace; it is
      safe here precisely because the Pod has no owner to misattribute it to.
      *(Req 7, 8)* — deps: claim-uniqueness

## 4. Watches, concurrency, runtime

- [~] **watches-mapping** — `Owns(&corev1.Pod{})` wired, so readiness transitions re-enqueue the pool
      with no polling. Claim-side mappings still TODO. — deps: claim-allocate, pod-creation
- [ ] **concurrency-safety** ⭐ — `MaxConcurrentReconciles > 1` must be safe: ResourceVersion
      preconditions, `IsConflict` treated as retry-not-error, tolerate a stale informer cache showing an
      already-claimed Pod as warm. *(Req 7)* — deps: claim-allocate
- [x] **leader-election** — `--leader-elect` flag in `cmd/manager`. — deps: scaffold-project

## 5. Tests *(Req 12)*

- [x] **test-unit-warm** — table-driven `classifyPod`/`countPods`/deficit tests, including priority-chain
      ordering (terminating outranks ready and claimed; failed outranks claimed) and a stale
      `Ready=True` condition on a non-Running Pod. — deps: warm-counting
- [x] **test-pool-replicas** — *Scenario 1* passing in envtest and verified on kind: 3 Pods created,
      reported `warming=3` before readiness and `warm=3` after, `WarmCapacityReady=True/AtCapacity`.
      Plus: no-overshoot-while-warming, scale-up keeps existing Pods, zero-replicas, label/ownerRef
      stamping, and foreign-Pod-ignored. — deps: pool-status
- [x] **test-claim-assign** — *Scenario 2*: one claim goes `Pending → Claimed` with exactly one labeled
      Pod and populated `podName`/`podIP`. — deps: claim-status
- [x] **test-refill** — *Scenario 3*: after one Pod is claimed, warm count returns to `replicas` and total
      Pods `= replicas + claimed`. — deps: test-claim-assign, pod-creation
- [ ] **test-no-double-assign** — *Scenario 4*: N concurrent claims yield N distinct Pod names; no Pod
      carries two claim labels. — deps: claim-uniqueness
- [ ] **test-pod-deleted** — *Scenario 5*: delete an assigned Pod → claim re-assigns or fails with the
      documented reason. — deps: claim-pod-failure
- [ ] **test-restart** — *Scenario 6*: stop and restart the manager → no claim loses its Pod, no Pod is
      re-assigned, no duplicates. Also simulate a crash **between** the Pod-label write and the
      claim-status write. — deps: claim-adopt, orphan-sweeper
- [ ] **test-finalizer** — delete a `Claimed` claim → Pod deleted (or released) and finalizer removed even
      if the Pod is already gone. — deps: claim-finalizer
- [ ] **test-starvation** — more claims than replicas → extras stay `Pending` with reason, then get
      satisfied as the pool refills. — deps: claim-no-capacity

## 6. Docs & ops

- [ ] **readme** — API reference, design decisions (warm-capacity semantics, ownership/release policy,
      crash-safety write ordering), `kind create cluster` → `make install` → `make run` flow, and the exact
      `kubectl` commands for all six scenarios. *(Req 1, 9)* — deps: claim-finalizer, pool-status
- [ ] **metrics-events** — gauges/counters for warm Pods, claimed Pods, pending claims, assignment
      conflicts, failed assignments; Events on every state transition. — deps: claim-status
- [ ] **e2e-script** — scripted version of the six manual scenarios against a real `kind` cluster. — deps: manifests-rbac, readme

---

## 7. Below the scope — deferred, revisit after the core implementation

These are **my inferences, not requirements you gave**. They are defensible for production but are
not needed to satisfy Reqs 1–12, and each adds real complexity or new failure modes. Parked here
deliberately; revisit once the core controller and its tests are green.

- [ ] **pod-batch-bounding** — cap Pods created or deleted per reconcile, with an immediate requeue
      for the remainder. *Existed briefly as `maxPodsPerReconcile=10` and was removed: at POC scale a
      pool never jumps by enough Pods for one unbounded pass to matter, and the cap added a second
      control path (clamp + requeue) that had to be kept consistent across both the create and delete
      sides. Worth restoring before any pool gets large enough that a single spec edit turns into a
      burst of hundreds of writes against the API server.*
- [ ] **pool-template-change** *(was D5)* — template-hash label, recycle stale unclaimed Pods, never
      disrupt claimed ones. *Deferred: no requirement covers mutating `spec.template`. Until then,
      behavior is "new Pods use the new template, existing warm Pods are left alone" — which is simply
      what falls out of doing nothing, and will be documented as such.*
- [ ] **pool-deletion** *(was D6)* — explicit handling of `WorkerPool` deletion while claims are
      outstanding. *Deferred because cascade deletion is not code I write — it is what Kubernetes GC
      does for free once Pods carry the pool's `ownerReference`. Suppressing it would require a pool
      finalizer (new deadlock risk: pool stuck terminating forever if a claim is never deleted) or
      stripping ownerRefs (the two-write race that permanently leaks Pods). POC behavior: default
      cascade, documented as destructive. User escape hatch today:*
      `kubectl delete workerpool <name> --cascade=orphan`.
      *The genuinely required sliver — a claim whose pool disappeared must land in a clear state — stays
      in scope as `claim-bad-pool` (§3).*
- [ ] **claim-timeout** *(was D4)* — optional `spec.maxClaimWait`; on expiry mark `Failed`/`Timeout`.
      *Deferred: Req 10 only demands recovery-or-failure when an **assigned** Pod dies, which
      `claim-pod-failure` covers. A claim waiting on capacity staying `Pending` with a clear reason is
      correct, observable behavior — not a hang.*
- [ ] **metrics-events (metrics half)** — Prometheus gauges/counters. *Events are in scope (Req 9
      observability); metrics are an inference.*

### API fields removed from the POC `spec` as a result

`templatePolicy` (D5) and `maxClaimWait` (D4) are **out** of `WorkerPool.spec` for now. Adding optional
fields later is backward-compatible, so nothing is foreclosed.

Surviving spec fields and their justification:

| Field | Required by | Notes |
|---|---|---|
| `replicas` | Req 2, 3 | Warm headroom |
| `template` | Req 3 | Pod source |
| `claimPolicy` | Req 11 | Makes release behavior explicit, as Req 11 asks |

---
 (the reason most of the above exists)

1. **Crash between Pod-label write and claim-status write** → orphaned claimed Pod.
   Mitigated by write ordering + `claim-adopt` + `orphan-sweeper`.
2. **Stale informer cache** hands the same "warm" Pod to two concurrent reconciles.
   Mitigated by ResourceVersion-precondition writes + conflict retry (`claim-allocate`, `concurrency-safety`).
3. **Finalizer deadlock** when the `WorkerPool` (and its Pods) is deleted before the claim.
   Mitigated by treating a missing Pod as success in `claim-finalizer`.
4. **Overshooting Pod creation** while Pods are still becoming ready (readiness is asynchronous) →
   runaway Pod count. Mitigated by counting not-yet-ready pool Pods as in-flight supply in `pod-creation`.
5. **Claim name reuse** (delete + recreate the same name) binding to a stale Pod.
   Mitigated by the claim-UID annotation.
6. **Pod ready but `podIP` empty** → claim published with an unusable address (`claim-pod-ip`).
