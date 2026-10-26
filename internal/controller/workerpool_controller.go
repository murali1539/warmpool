package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	warmpoolv1alpha1 "github.com/linkedin/warmpool/api/v1alpha1"
)

// defaultResyncPeriod is how often a pool is re-reconciled in the absence of
// any watch event. See WorkerPoolReconciler.ResyncPeriod.
const defaultResyncPeriod = 5 * time.Minute

// podOwnerUIDIndex indexes Pods by the UID of their controlling WorkerPool.
//
// Indexing by owner UID keeps label edits from hiding owned Pods from capacity
// accounting or making foreign Pods count toward a pool.
const podOwnerUIDIndex = ".metadata.controller.uid"

// indexPodByOwnerUID extracts the controlling WorkerPool UID from a Pod.
func indexPodByOwnerUID(obj client.Object) []string {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	ref := metav1.GetControllerOf(pod)
	if ref == nil || ref.Kind != "WorkerPool" || ref.APIVersion != warmpoolv1alpha1.GroupVersion.String() {
		return nil
	}
	return []string{string(ref.UID)}
}

// WorkerPoolReconciler reconciles toward spec.warmReplicas ready, unclaimed Pods.
//
// Scope: it creates Pods to reach the desired warm count, deletes surplus
// unclaimed Pods when the count shrinks, and reports what it observes. Claim
// assignment and claim lifecycle management are handled by WorkerClaimReconciler.
type WorkerPoolReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// ControllerName distinguishes this controller's metrics. Defaults to
	// "workerpool"; tests override it because controller-runtime requires names
	// to be unique within a process.
	ControllerName string

	// ResyncPeriod re-reconciles each pool on an interval even when no watch
	// event arrives.
	//
	// This is a safety net, not the primary trigger: a spec.warmReplicas edit
	// bumps the pool's resourceVersion and a Pod becoming Ready or disappearing
	// fires the Owns(Pod) watch, so the steady-state path is entirely
	// event-driven. The resync exists for the cases a watch cannot cover -- a
	// dropped event, an informer re-list that races a write -- where the
	// alternative is a pool sitting at the wrong capacity indefinitely.
	//
	// Zero selects defaultResyncPeriod. A negative value disables the resync,
	// which is useful in tests that assert purely event-driven behavior.
	ResyncPeriod time.Duration
}

// resyncPeriod returns the configured resync interval, or 0 when disabled.
func (r *WorkerPoolReconciler) resyncPeriod() time.Duration {
	switch {
	case r.ResyncPeriod < 0:
		return 0
	case r.ResyncPeriod == 0:
		return defaultResyncPeriod
	default:
		return r.ResyncPeriod
	}
}

// opFailure describes a failed create or delete pass so that status can name
// what went wrong rather than reporting a generic failure.
type opFailure struct {
	// reason is the condition reason, e.g. PodCreateFailed.
	reason string
	// summary reads as a clause in the condition message, e.g. "pod creation
	// failing".
	summary string
	err     error
}

// +kubebuilder:rbac:groups=linkedin.com,resources=workerpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=linkedin.com,resources=workerpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=linkedin.com,resources=workerpools/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;create;update

func (r *WorkerPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pool := &warmpoolv1alpha1.WorkerPool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		// Kubernetes garbage-collects owned Pods, including claimed ones, when
		// the pool is deleted.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// A pool being deleted must not have Pods created for it. Garbage
	// collection removes the existing ones.
	if pool.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	pods, err := r.ownedPods(ctx, pool)
	if err != nil {
		// Returning the error is the whole of the retry policy, and it is
		// sufficient: controller-runtime re-enqueues the request through the
		// workqueue's rate limiter, so a transient cache or API failure is
		// retried with exponential backoff rather than hot-looping. Nothing has
		// been written yet, so there is no partial progress to record in status
		// and no reason to distinguish transient from permanent here -- a
		// permanent failure simply keeps failing, with backoff.
		return ctrl.Result{}, fmt.Errorf("listing pods: %w", err)
	}

	counts := countPods(pods)

	desired := pool.DesiredWarmReplicas()

	// Pods that are still warming already count as supply. Subtracting them is
	// what stops the controller from creating a new Pod on every pass while the
	// first is still booting.
	deficit := desired - counts.supply()

	var failure *opFailure
	switch {
	case deficit > 0:
		logger.Info("deficit detected, creating new pods",
			"pool", pool.Name, "deficit", deficit,
			"warm", counts.warm, "warming", counts.warming)
		created, createErr := r.createPods(ctx, pool, deficit)
		if created > 0 {
			logger.Info("created warm pods",
				"pool", pool.Name, "created", created, "deficit", deficit,
				"warm", counts.warm, "warming", counts.warming, "claimed", counts.claimed)
			// Reflect the new Pods immediately so status does not report a stale
			// deficit until the Pod watch fires.
			counts.warming += created
		}
		if createErr != nil {
			failure = &opFailure{
				reason:  warmpoolv1alpha1.PoolReasonPodCreateFailed,
				summary: "pod creation failing",
				err:     createErr,
			}
		}

	case deficit < 0:
		logger.Info("surplus detected, deleting excess pods",
			"pool", pool.Name, "surplus", -deficit,
			"warm", counts.warm, "warming", counts.warming)
		surplus := -deficit
		deleted, deleteErr := r.deleteSurplusPods(ctx, pool, surplus, pods)
		if n := deleted.warm + deleted.warming; n > 0 {
			logger.Info("deleted surplus pods",
				"pool", pool.Name, "deleted", n, "surplus", surplus,
				"warm", counts.warm, "warming", counts.warming, "claimed", counts.claimed)
			// Deleted Pods leave capacity immediately, even if graceful
			// termination or finalizers keep their API objects around.
			counts.warm -= deleted.warm
			counts.warming -= deleted.warming
		}
		if deleteErr != nil {
			failure = &opFailure{
				reason:  warmpoolv1alpha1.PoolReasonPodDeleteFailed,
				summary: "pod deletion failing",
				err:     deleteErr,
			}
		}
	}

	statusErr := r.updateStatus(ctx, pool, counts, failure)
	switch {
	case apierrors.IsConflict(statusErr):
		// Another writer updated the pool. Requeue with backoff rather than
		// retrying immediately against a potentially stale cache.
		return ctrl.Result{Requeue: true}, nil

	case statusErr != nil:
		return ctrl.Result{}, fmt.Errorf("updating status: %w", statusErr)

	case failure != nil:
		// Returning the error lets the workqueue rate limiter back off instead
		// of hot-looping against a template the API server keeps rejecting.
		return ctrl.Result{}, failure.err
	}

	return ctrl.Result{RequeueAfter: r.resyncPeriod()}, nil
}

// ownedPods returns the Pods this pool owns.
//
// Membership uses the controlling WorkerPool UID, not labels. Recreating a pool
// under the same name gives it a new UID, so it does not inherit the old Pods.
//
// The pool label supports human-facing selectors and claim watch routing;
// changing it cannot hide an owned Pod from capacity accounting.
func (r *WorkerPoolReconciler) ownedPods(ctx context.Context, pool *warmpoolv1alpha1.WorkerPool) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list,
		client.InNamespace(pool.Namespace),
		client.MatchingFields{podOwnerUIDIndex: string(pool.UID)},
	); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// createPods creates `deficit` warm Pods. It returns how many were actually
// created, even on error, so status can reflect the partial progress.
func (r *WorkerPoolReconciler) createPods(ctx context.Context, pool *warmpoolv1alpha1.WorkerPool, deficit int32) (int32, error) {
	var created int32
	for i := int32(0); i < deficit; i++ {
		pod := buildPod(pool)
		// The ownerReference is what makes this Pod a member of the pool and
		// what garbage collects it when the pool goes away.
		if err := ctrl.SetControllerReference(pool, pod, r.Scheme); err != nil {
			return created, fmt.Errorf("setting owner reference: %w", err)
		}
		if err := r.Create(ctx, pod); err != nil {
			r.eventf(pool, corev1.EventTypeWarning, warmpoolv1alpha1.PoolReasonPodCreateFailed,
				"failed to create warm pod: %v", err)
			return created, fmt.Errorf("creating pod: %w", err)
		}
		created++
	}

	r.eventf(pool, corev1.EventTypeNormal, warmpoolv1alpha1.PoolReasonScalingUp,
		"created %d warm pod(s) to reach %d warm replicas", created, pool.DesiredWarmReplicas())
	return created, nil
}

// surplusCandidates returns the Pods that may be deleted to shed capacity, in
// the order they should be deleted.
//
// Only warm and warming Pods are eligible. Claimed Pods are excluded outright:
// the pool still owns them, but they belong to a claimant and deleting one
// would kill a live workload to satisfy a number. Terminating and terminal Pods
// are excluded because they are already not supply -- deleting them again
// sheds nothing while consuming the per-reconcile budget.
//
// The ordering preserves currently ready capacity where possible:
//
//   - warming before warm, because a not-ready Pod contributes no warm capacity;
//   - newest before oldest within each class, to discard recent additions first.
//
// Name is the final tie-break. creationTimestamp has one-second granularity and
// a pool routinely creates several Pods inside one second, so without it the
// deletion order would be whatever order the lister happened to return.
func surplusCandidates(pods []corev1.Pod) []*corev1.Pod {
	candidates := make([]*corev1.Pod, 0, len(pods))
	for i := range pods {
		switch classifyPod(&pods[i]) {
		case podWarm, podWarming:
			candidates = append(candidates, &pods[i])
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]

		// podWarming sorts before podWarm by deletion preference, which is the
		// reverse of the podClass declaration order.
		aWarming := classifyPod(a) == podWarming
		bWarming := classifyPod(b) == podWarming
		if aWarming != bWarming {
			return aWarming
		}

		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return b.CreationTimestamp.Before(&a.CreationTimestamp)
		}
		return a.Name > b.Name
	})

	return candidates
}

// deleteSurplusPods requests deletion of up to surplus unclaimed Pods. It
// returns accepted deletions by class so the caller can adjust its counts.
//
// Deletes carry UID and resourceVersion preconditions. The Pod comes from a
// watch cache that may be behind the API server, and the dangerous staleness is
// specific: a Pod that was warm when it was cached may have been claimed since.
// The resourceVersion precondition turns that race into a rejected delete
// rather than a claimant losing its worker. Pod events enqueue the pool to
// observe changed capacity in a later reconciliation.
func (r *WorkerPoolReconciler) deleteSurplusPods(
	ctx context.Context,
	pool *warmpoolv1alpha1.WorkerPool,
	surplus int32,
	pods []corev1.Pod,
) (poolCounts, error) {
	var deleted poolCounts
	if surplus <= 0 {
		return deleted, nil
	}

	for _, pod := range surplusCandidates(pods) {
		if deleted.warm+deleted.warming >= surplus {
			break
		}

		class := classifyPod(pod)
		err := r.Delete(ctx, pod, client.Preconditions{
			UID:             &pod.UID,
			ResourceVersion: &pod.ResourceVersion,
		})
		switch {
		case err == nil:
		case apierrors.IsNotFound(err), apierrors.IsConflict(err):
			// Skip without counting a deletion. The original surplus is not
			// recomputed here, so concurrent claims can cause a temporary
			// capacity dip until a later reconcile restores it.
			continue
		default:
			r.eventf(pool, corev1.EventTypeWarning, warmpoolv1alpha1.PoolReasonPodDeleteFailed,
				"failed to delete surplus pod %s: %v", pod.Name, err)
			return deleted, fmt.Errorf("deleting pod %s: %w", pod.Name, err)
		}

		switch class {
		case podWarm:
			deleted.warm++
		case podWarming:
			deleted.warming++
		}
	}

	if n := deleted.warm + deleted.warming; n > 0 {
		r.eventf(pool, corev1.EventTypeNormal, warmpoolv1alpha1.PoolReasonScalingDown,
			"deleted %d surplus pod(s) to reach %d warm replicas", n, pool.DesiredWarmReplicas())
	}
	return deleted, nil
}

// updateStatus writes observed counts and conditions only when they change.
//
// The Get below uses the informer cache. Conflicts are returned for a later
// reconciliation rather than retried against potentially stale state.
func (r *WorkerPoolReconciler) updateStatus(
	ctx context.Context,
	pool *warmpoolv1alpha1.WorkerPool,
	counts poolCounts,
	failure *opFailure,
) error {
	desired := pool.DesiredWarmReplicas()

	// Merge onto the latest cached pool, which may have advanced during this
	// pass. The API server still arbitrates writes using resourceVersion.
	latest := &warmpoolv1alpha1.WorkerPool{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pool), latest); err != nil {
		return client.IgnoreNotFound(err)
	}

	updated := latest.DeepCopy()
	updated.Status.WarmReplicas = counts.warm
	updated.Status.WarmingReplicas = counts.warming
	updated.Status.ClaimedReplicas = counts.claimed
	updated.Status.TotalPods = counts.total()

	// The generation comes from pool, not latest. observedGeneration means "the
	// spec generation this status was computed from", and every count and
	// message below derives from pool. If the spec was edited mid-pass, latest
	// carries a newer generation this pass never read; publishing it would
	// claim the status is current for a spec the controller has not acted on,
	// which is the exact staleness observedGeneration exists to expose. The
	// spec edit raises its own watch event, so the next pass reports it.
	updated.Status.ObservedGeneration = pool.Generation

	cond := metav1.Condition{
		Type:               warmpoolv1alpha1.PoolConditionWarmCapacityReady,
		ObservedGeneration: pool.Generation,
	}
	switch {
	case failure != nil:
		cond.Status = metav1.ConditionFalse
		cond.Reason = failure.reason
		cond.Message = fmt.Sprintf("%d/%d warm pods; %s: %v",
			counts.warm, desired, failure.summary, failure.err)
	case counts.warm >= desired:
		cond.Status = metav1.ConditionTrue
		cond.Reason = warmpoolv1alpha1.PoolReasonAtCapacity
		cond.Message = fmt.Sprintf("%d/%d warm pods available", counts.warm, desired)
	case counts.warming > 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = warmpoolv1alpha1.PoolReasonWarming
		cond.Message = fmt.Sprintf("%d/%d warm pods; %d warming",
			counts.warm, desired, counts.warming)
	default:
		cond.Status = metav1.ConditionFalse
		cond.Reason = warmpoolv1alpha1.PoolReasonScalingUp
		cond.Message = fmt.Sprintf("%d/%d warm pods", counts.warm, desired)
	}
	meta.SetStatusCondition(&updated.Status.Conditions, cond)

	if equalStatus(latest.Status, updated.Status) {
		return nil
	}
	return r.Status().Update(ctx, updated)
}

// equalStatus compares counters, observed generation, and condition values.
// LastTransitionTime is maintained by meta.SetStatusCondition and is not an
// independent reason to write status.
func equalStatus(a, b warmpoolv1alpha1.WorkerPoolStatus) bool {
	if a.WarmReplicas != b.WarmReplicas ||
		a.WarmingReplicas != b.WarmingReplicas ||
		a.ClaimedReplicas != b.ClaimedReplicas ||
		a.TotalPods != b.TotalPods ||
		a.ObservedGeneration != b.ObservedGeneration ||
		len(a.Conditions) != len(b.Conditions) {
		return false
	}
	for i := range a.Conditions {
		x, y := a.Conditions[i], b.Conditions[i]
		if x.Type != y.Type || x.Status != y.Status || x.Reason != y.Reason || x.Message != y.Message {
			return false
		}
	}
	return true
}

func (r *WorkerPoolReconciler) eventf(pool *warmpoolv1alpha1.WorkerPool, eventType, reason, format string, args ...any) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(pool, eventType, reason, format, args...)
}

// SetupWithManager wires the controller.
//
// Owns(Pod) is what makes the pool react to readiness: a Pod moving from
// warming to warm, or disappearing, enqueues its owning pool without polling.
func (r *WorkerPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Membership lookups go through the ownerReference UID, so the index must
	// exist before the controller starts reconciling.
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(), &corev1.Pod{}, podOwnerUIDIndex, indexPodByOwnerUID,
	); err != nil {
		return fmt.Errorf("indexing pods by owner uid: %w", err)
	}

	name := r.ControllerName
	if name == "" {
		name = "workerpool"
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&warmpoolv1alpha1.WorkerPool{}).
		Owns(&corev1.Pod{}).
		Named(name).
		Complete(r)
}

// IgnoreAlreadyExists is a small helper for callers that tolerate races with
// another writer creating the same object.
func IgnoreAlreadyExists(err error) error {
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}
