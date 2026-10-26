package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	warmpoolv1alpha1 "github.com/linkedin/warmpool/api/v1alpha1"
)

// claimPoolIndex routes Pod and WorkerPool events to pending claims for that pool.
const claimPoolIndex = ".spec.poolName"

func indexClaimByPool(obj client.Object) []string {
	claim, ok := obj.(*warmpoolv1alpha1.WorkerClaim)
	if !ok || claim.Spec.PoolName == "" {
		return nil
	}
	return []string{claim.Spec.PoolName}
}

// WorkerClaimReconciler binds one warm Pod to one WorkerClaim.
//
// Pod labels and UID annotations persist bindings across restarts. Pod
// resourceVersions arbitrate competing claims; leader election and per-key
// reconciliation serialize allocations for the same claim.
//
// Failed and Succeeded claims are terminal and are never reassigned. Pending
// claims have no timeout.
type WorkerClaimReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// APIReader bypasses the informer cache for claim status, binding lookups,
	// and pool lifecycle checks. Stale bindings could cause duplicate allocation
	// or skipped cleanup. SetupWithManager supplies this reader.
	APIReader client.Reader

	// ControllerName disambiguates controllers when several managers run in one
	// process, as they do in tests.
	ControllerName string
}

// +kubebuilder:rbac:groups=linkedin.com,resources=workerclaims,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=linkedin.com,resources=workerclaims/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=linkedin.com,resources=workerclaims/finalizers,verbs=update

func (r *WorkerClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	claim := &warmpoolv1alpha1.WorkerClaim{}
	if err := r.Get(ctx, req.NamespacedName, claim); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if claim.DeletionTimestamp != nil {
		return ctrl.Result{}, r.finalize(ctx, claim)
	}

	// Persist cleanup responsibility before binding a Pod. Updates go to the API
	// server; resourceVersion rejects writes based on stale cached metadata.
	if !controllerutil.ContainsFinalizer(claim, warmpoolv1alpha1.ClaimFinalizer) {
		controllerutil.AddFinalizer(claim, warmpoolv1alpha1.ClaimFinalizer)
		if err := r.Update(ctx, claim); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
		// Reconcile again to observe the finalizer; cache propagation is asynchronous.
		return ctrl.Result{Requeue: true}, nil
	}

	if claim.Status.Phase == warmpoolv1alpha1.ClaimPhaseFailed ||
		claim.Status.Phase == warmpoolv1alpha1.ClaimPhaseSucceeded {
		return ctrl.Result{}, nil
	}

	// A stale Pending read must not allow a published assignment to be replaced.
	published := &warmpoolv1alpha1.WorkerClaim{}
	if err := r.APIReader.Get(ctx, req.NamespacedName, published); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if published.Status.Phase == warmpoolv1alpha1.ClaimPhaseFailed ||
		published.Status.Phase == warmpoolv1alpha1.ClaimPhaseSucceeded {
		return ctrl.Result{}, nil
	}

	// Adopt before allocating. A Pod already bound to this claim is the durable
	// truth, whether it was bound a second ago or by a previous process that
	// died before publishing status.
	bound, err := r.adopt(ctx, published)
	if err != nil {
		return ctrl.Result{}, err
	}
	if published.Status.PodName != "" && (bound == nil || bound.Name != published.Status.PodName) {
		return ctrl.Result{}, r.publishLost(ctx, published, published.Status.PodName)
	}
	if bound != nil {
		switch {
		case bound.Status.Phase == corev1.PodSucceeded:
			return ctrl.Result{}, r.publishSucceeded(ctx, published, bound.Name)
		case bound.DeletionTimestamp != nil || bound.Status.Phase == corev1.PodFailed:
			return ctrl.Result{}, r.publishLost(ctx, published, bound.Name)
		default:
			return ctrl.Result{}, r.publishClaimed(ctx, published, bound)
		}
	}

	// Existing bindings are handled above, even when their pool is deleting.
	// This uncached check cannot prevent deletion starting after the read.
	pool := &warmpoolv1alpha1.WorkerPool{}
	poolKey := types.NamespacedName{Namespace: published.Namespace, Name: published.Spec.PoolName}
	if err := r.APIReader.Get(ctx, poolKey, pool); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("getting pool: %w", err)
		}
		// Not terminal: the pool may simply not exist yet. The claim says
		// plainly why it is waiting instead of hanging with an empty status.
		return ctrl.Result{}, r.publishPending(ctx, published,
			warmpoolv1alpha1.ClaimReasonPoolNotFound,
			fmt.Sprintf("WorkerPool %q not found in namespace %q", published.Spec.PoolName, published.Namespace))
	}
	if pool.DeletionTimestamp != nil {
		return ctrl.Result{}, r.publishPending(ctx, published,
			warmpoolv1alpha1.ClaimReasonPoolTerminating,
			fmt.Sprintf("WorkerPool %q is terminating; waiting for a usable pool", pool.Name))
	}

	pod, err := r.allocate(ctx, published, pool)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pod == nil {
		// No warm Pod right now. The Pod watch wakes this claim when the pool
		// produces one, so there is no polling interval to tune.
		return ctrl.Result{}, r.publishPending(ctx, published,
			warmpoolv1alpha1.ClaimReasonWaitingForWarmPod,
			fmt.Sprintf("no warm Pod available in pool %q", pool.Name))
	}

	logger.Info("bound pod to claim", "claim", published.Name, "pod", pod.Name, "pool", pool.Name)
	return ctrl.Result{}, r.publishClaimed(ctx, published, pod)
}

// binding groups bound Pods by lifecycle state.
type binding struct {
	// live includes non-terminal, non-terminating Pods regardless of readiness.
	live []corev1.Pod

	// lost are Pods bound to this claim that are terminating, Failed or
	// Succeeded. They can never serve the claim again.
	lost []corev1.Pod
}

// all returns every Pod bound to the claim, live or not.
func (b binding) all() []corev1.Pod {
	out := make([]corev1.Pod, 0, len(b.live)+len(b.lost))
	out = append(out, b.live...)
	return append(out, b.lost...)
}

func podNames(pods []corev1.Pod) string {
	names := make([]string, 0, len(pods))
	for i := range pods {
		names = append(names, pods[i].Name)
	}
	return strings.Join(names, ", ")
}

// boundPods finds Pods carrying both this claim's name label and UID annotation.
//
// An uncached read avoids missing a binding written before its watch event
// reaches the cache. UID matching excludes bindings for a previous same-name
// claim. The label selector works on the API server, unlike our cache indexes.
func (r *WorkerClaimReconciler) boundPods(
	ctx context.Context, claim *warmpoolv1alpha1.WorkerClaim,
) (corev1.PodList, error) {
	var list corev1.PodList
	if err := r.APIReader.List(ctx, &list,
		client.InNamespace(claim.Namespace),
		client.MatchingLabels{warmpoolv1alpha1.LabelClaim: claim.Name},
	); err != nil {
		return corev1.PodList{}, fmt.Errorf("listing bound pods: %w", err)
	}
	bound := list.Items[:0]
	for _, pod := range list.Items {
		if pod.Annotations[warmpoolv1alpha1.AnnotationClaimUID] == string(claim.UID) {
			bound = append(bound, pod)
		}
	}
	list.Items = bound
	return list, nil
}

// adopt returns the single bound Pod, including a terminal or terminating one,
// or nil if no binding exists. Reconcile determines the claim's resulting phase.
//
// Multiple matching bindings are rejected regardless of Pod phase. Adoption
// never deletes Pods or guesses which worker the client is using.
func (r *WorkerClaimReconciler) adopt(
	ctx context.Context, claim *warmpoolv1alpha1.WorkerClaim,
) (*corev1.Pod, error) {
	b, err := r.boundPods(ctx, claim)
	if err != nil {
		return nil, err
	}

	if len(b.Items) > 1 {
		return nil, fmt.Errorf(
			"claim %s/%s is bound to %d Pods (%s): the one-Pod-per-claim invariant is broken "+
				"and this controller will not guess which Pod the client is using; resolve manually",
			claim.Namespace, claim.Name, len(b.Items), podNames(b.Items))
	}

	if len(b.Items) == 1 {
		return &b.Items[0], nil
	}
	return nil, nil
}

// sortPodsOldestFirst orders Pods deterministically. The name is the tie-break
// because creationTimestamp has only one-second granularity, so Pods created
// in the same pass routinely share one.
func sortPodsOldestFirst(pods []corev1.Pod) {
	sort.Slice(pods, func(i, j int) bool {
		if !pods[i].CreationTimestamp.Equal(&pods[j].CreationTimestamp) {
			return pods[i].CreationTimestamp.Before(&pods[j].CreationTimestamp)
		}
		return pods[i].Name < pods[j].Name
	})
}

// allocate binds one warm Pod from the pool to the claim, or returns nil if
// the pool has none.
func (r *WorkerClaimReconciler) allocate(
	ctx context.Context,
	claim *warmpoolv1alpha1.WorkerClaim,
	pool *warmpoolv1alpha1.WorkerPool,
) (*corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list,
		client.InNamespace(pool.Namespace),
		client.MatchingFields{podOwnerUIDIndex: string(pool.UID)},
	); err != nil {
		return nil, fmt.Errorf("listing pool pods: %w", err)
	}

	var warm []corev1.Pod
	for i := range list.Items {
		if classifyPod(&list.Items[i]) == podWarm {
			warm = append(warm, list.Items[i])
		}
	}
	// Prefer older Pods deterministically; creation time is not readiness duration.
	sort.Slice(warm, func(i, j int) bool {
		if !warm[i].CreationTimestamp.Equal(&warm[j].CreationTimestamp) {
			return warm[i].CreationTimestamp.Before(&warm[j].CreationTimestamp)
		}
		return warm[i].Name < warm[j].Name
	})

	for i := range warm {
		pod := warm[i].DeepCopy()

		if pod.Labels[warmpoolv1alpha1.LabelClaim] != "" {
			continue
		}

		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Labels[warmpoolv1alpha1.LabelClaim] = claim.Name
		pod.Labels[warmpoolv1alpha1.LabelState] = warmpoolv1alpha1.StateClaimed
		pod.Annotations[warmpoolv1alpha1.AnnotationClaimUID] = string(claim.UID)

		// The API server rejects a stale resourceVersion if another writer changed
		// this Pod, including its readiness, deletion state, or claim binding.
		err := r.Update(ctx, pod)
		switch {
		case err == nil:
			return pod, nil
		case apierrors.IsConflict(err), apierrors.IsNotFound(err):
			// Lost the race or the Pod vanished. Try the next candidate.
			continue
		default:
			return nil, fmt.Errorf("binding pod %s: %w", pod.Name, err)
		}
	}
	return nil, nil
}

// finalize requests deletion of all matching bindings, then releases the finalizer.
//
// Used workers are never returned to the warm pool. Already-missing or
// terminating Pods need no further delete request; cleanup does not wait for
// their termination to finish.
func (r *WorkerClaimReconciler) finalize(ctx context.Context, claim *warmpoolv1alpha1.WorkerClaim) error {
	if !controllerutil.ContainsFinalizer(claim, warmpoolv1alpha1.ClaimFinalizer) {
		return nil
	}

	// A stale list could skip cleanup: claim deletion cannot garbage-collect a
	// Pod owned by the pool.
	b, err := r.boundPods(ctx, claim)
	if err != nil {
		return err
	}
	// Unlike adopt, finalize does not refuse when several Pods are bound.
	// Adopt refuses because choosing between them is a guess; here every bound
	// Pod is being deleted regardless, so there is nothing to guess.
	for i := range b.Items {
		pod := &b.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		if err := r.Delete(ctx, pod, client.Preconditions{
			UID:             &pod.UID,
			ResourceVersion: &pod.ResourceVersion,
		}); err != nil &&
			!apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting pod %s: %w", pod.Name, err)
		}
		r.eventf(claim, corev1.EventTypeNormal, "PodDeleted",
			"deleted Pod %s on claim deletion", pod.Name)
	}

	controllerutil.RemoveFinalizer(claim, warmpoolv1alpha1.ClaimFinalizer)
	if err := r.Update(ctx, claim); err != nil {
		return fmt.Errorf("removing finalizer: %w", err)
	}
	return nil
}

func (r *WorkerClaimReconciler) publishClaimed(
	ctx context.Context,
	claim *warmpoolv1alpha1.WorkerClaim,
	pod *corev1.Pod,
) error {
	phase := claim.Status.Phase
	if phase != "" && phase != warmpoolv1alpha1.ClaimPhasePending {
		return nil
	}

	now := metav1.Now()
	return r.writeStatus(ctx, claim, warmpoolv1alpha1.WorkerClaimStatus{
		Phase:      warmpoolv1alpha1.ClaimPhaseClaimed,
		PodName:    pod.Name,
		Reason:     warmpoolv1alpha1.ClaimReasonAssigned,
		Message:    fmt.Sprintf("bound to Pod %s", pod.Name),
		AssignedAt: &now,
	})
}

// publishLost marks a claim Failed when its Pod fails, is deleting, or its
// published binding is missing or mismatched.
//
// The Pod name is explicit because the binding may not have been published.
// Any existing assignedAt is retained as evidence.
func (r *WorkerClaimReconciler) publishLost(
	ctx context.Context,
	claim *warmpoolv1alpha1.WorkerClaim,
	podName string,
) error {
	r.eventf(claim, corev1.EventTypeWarning, warmpoolv1alpha1.ClaimReasonPodLost,
		"Pod %s was lost; this claim will not be reassigned", podName)

	return r.writeStatus(ctx, claim, warmpoolv1alpha1.WorkerClaimStatus{
		Phase:   warmpoolv1alpha1.ClaimPhaseFailed,
		PodName: podName,
		Reason:  warmpoolv1alpha1.ClaimReasonPodLost,
		Message: fmt.Sprintf(
			"Pod %s was deleted or failed; create a new WorkerClaim to obtain another worker",
			podName),
		AssignedAt: claim.Status.AssignedAt,
	})
}

func (r *WorkerClaimReconciler) publishSucceeded(
	ctx context.Context,
	claim *warmpoolv1alpha1.WorkerClaim,
	podName string,
) error {
	r.eventf(claim, corev1.EventTypeNormal, warmpoolv1alpha1.ClaimReasonPodCompleted,
		"Pod %s completed successfully", podName)

	return r.writeStatus(ctx, claim, warmpoolv1alpha1.WorkerClaimStatus{
		Phase:      warmpoolv1alpha1.ClaimPhaseSucceeded,
		PodName:    podName,
		Reason:     warmpoolv1alpha1.ClaimReasonPodCompleted,
		Message:    fmt.Sprintf("Pod %s completed successfully", podName),
		AssignedAt: claim.Status.AssignedAt,
	})
}

func (r *WorkerClaimReconciler) publishPending(
	ctx context.Context,
	claim *warmpoolv1alpha1.WorkerClaim,
	reason, message string,
) error {
	if claim.Status.Reason != reason {
		r.eventf(claim, corev1.EventTypeNormal, reason, "%s", message)
	}
	return r.writeStatus(ctx, claim, warmpoolv1alpha1.WorkerClaimStatus{
		Phase:   warmpoolv1alpha1.ClaimPhasePending,
		Reason:  reason,
		Message: message,
	})
}

// writeStatus submits the observed claim status to the status subresource.
func (r *WorkerClaimReconciler) writeStatus(
	ctx context.Context,
	claim *warmpoolv1alpha1.WorkerClaim,
	status warmpoolv1alpha1.WorkerClaimStatus,
) error {
	updated := claim.DeepCopy()
	updated.Status = status
	// Return conflicts so the workqueue retries the whole reconciliation with
	// newly observed state, rather than retrying this stale status calculation.
	return r.Status().Update(ctx, updated)
}

func (r *WorkerClaimReconciler) eventf(
	claim *warmpoolv1alpha1.WorkerClaim, eventType, reason, format string, args ...any,
) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(claim, eventType, reason, format, args...)
}

// SetupWithManager wires the controller.
//
// Pod events wake bound claims or claims waiting for capacity. Pool lifecycle
// events wake pending claims even when no Pod event occurs.
func (r *WorkerClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}

	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(), &warmpoolv1alpha1.WorkerClaim{}, claimPoolIndex, indexClaimByPool,
	); err != nil {
		return fmt.Errorf("indexing claims by pool: %w", err)
	}

	name := r.ControllerName
	if name == "" {
		name = "workerclaim"
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&warmpoolv1alpha1.WorkerClaim{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.claimsForPod)).
		Watches(&warmpoolv1alpha1.WorkerPool{}, handler.EnqueueRequestsFromMapFunc(r.claimsForPool)).
		Named(name).
		Complete(r)
}

// claimsForPod maps a Pod event to the claims that might care about it.
//
// The pool label is only a routing hint. Allocation uses the controlling owner
// UID for membership and resourceVersion to reject writes from stale Pod reads.
func (r *WorkerClaimReconciler) claimsForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	// A bound Pod concerns exactly one claim.
	if name := pod.Labels[warmpoolv1alpha1.LabelClaim]; name != "" {
		return []reconcile.Request{{
			NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: name},
		}}
	}

	poolName := pod.Labels[warmpoolv1alpha1.LabelPool]
	if poolName == "" {
		return nil
	}
	return r.pendingClaimsForPool(ctx, pod.Namespace, poolName)
}

func (r *WorkerClaimReconciler) claimsForPool(ctx context.Context, obj client.Object) []reconcile.Request {
	pool, ok := obj.(*warmpoolv1alpha1.WorkerPool)
	if !ok {
		return nil
	}
	return r.pendingClaimsForPool(ctx, pool.Namespace, pool.Name)
}

func (r *WorkerClaimReconciler) pendingClaimsForPool(ctx context.Context, namespace, poolName string) []reconcile.Request {
	var claims warmpoolv1alpha1.WorkerClaimList
	if err := r.List(ctx, &claims,
		client.InNamespace(namespace),
		client.MatchingFields{claimPoolIndex: poolName},
	); err != nil {
		log.FromContext(ctx).Error(err, "listing pending claims for pool", "namespace", namespace, "pool", poolName)
		return nil
	}

	var reqs []reconcile.Request
	for i := range claims.Items {
		c := &claims.Items[i]
		if c.Status.Phase == warmpoolv1alpha1.ClaimPhaseClaimed ||
			c.Status.Phase == warmpoolv1alpha1.ClaimPhaseFailed ||
			c.Status.Phase == warmpoolv1alpha1.ClaimPhaseSucceeded {
			continue
		}
		reqs = append(reqs, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: c.Namespace, Name: c.Name},
		})
	}
	return reqs
}
