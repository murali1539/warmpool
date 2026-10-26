package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	warmpoolv1alpha1 "github.com/linkedin/warmpool/api/v1alpha1"
)

// startBothManagers runs both reconcilers in one manager for allocation and
// replenishment tests.
func startBothManagers(t *testing.T) {
	t.Helper()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		Metrics:        server.Options{BindAddress: "0"},
		LeaderElection: false,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	suffix := strings.ToLower(t.Name())
	if err := (&WorkerPoolReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		Recorder:       mgr.GetEventRecorderFor("workerpool-controller"),
		ControllerName: "workerpool-" + suffix,
	}).SetupWithManager(mgr); err != nil {
		t.Fatalf("setup pool reconciler: %v", err)
	}
	if err := (&WorkerClaimReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		Recorder:       mgr.GetEventRecorderFor("workerclaim-controller"),
		ControllerName: "workerclaim-" + suffix,
	}).SetupWithManager(mgr); err != nil {
		t.Fatalf("setup claim reconciler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = mgr.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache did not sync")
	}
}

func newClaim(t *testing.T, ns, name, pool string) *warmpoolv1alpha1.WorkerClaim {
	t.Helper()
	claim := &warmpoolv1alpha1.WorkerClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       warmpoolv1alpha1.WorkerClaimSpec{PoolName: pool},
	}
	if err := k8sClient.Create(context.Background(), claim); err != nil {
		t.Fatalf("create claim %s: %v", name, err)
	}
	return claim
}

func getClaim(t *testing.T, ns, name string) *warmpoolv1alpha1.WorkerClaim {
	t.Helper()
	claim := &warmpoolv1alpha1.WorkerClaim{}
	if err := k8sClient.Get(context.Background(),
		client.ObjectKey{Namespace: ns, Name: name}, claim); err != nil {
		t.Fatalf("get claim %s: %v", name, err)
	}
	return claim
}

func getPodByName(t *testing.T, ns, name string) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(),
		client.ObjectKey{Namespace: ns, Name: name}, pod); err != nil {
		t.Fatalf("get pod %s: %v", name, err)
	}
	return pod
}

func newBoundPod(t *testing.T, pool *warmpoolv1alpha1.WorkerPool, claim *warmpoolv1alpha1.WorkerClaim) *corev1.Pod {
	t.Helper()
	pod := buildPod(pool)
	pod.Labels[warmpoolv1alpha1.LabelClaim] = claim.Name
	pod.Labels[warmpoolv1alpha1.LabelState] = warmpoolv1alpha1.StateClaimed
	pod.Annotations = map[string]string{warmpoolv1alpha1.AnnotationClaimUID: string(claim.UID)}
	if err := ctrl.SetControllerReference(pool, pod, scheme); err != nil {
		t.Fatalf("set pod owner: %v", err)
	}
	if err := k8sClient.Create(context.Background(), pod); err != nil {
		t.Fatalf("create bound pod: %v", err)
	}
	markReady(t, pod)
	return getPodByName(t, pool.Namespace, pod.Name)
}

func holdPoolDeletion(t *testing.T, pool *warmpoolv1alpha1.WorkerPool) func() {
	t.Helper()
	ctx := context.Background()
	const finalizer = "tests.warmpool.linkedin.com/hold-pool"
	controllerutil.AddFinalizer(pool, finalizer)
	if err := k8sClient.Update(ctx, pool); err != nil {
		t.Fatalf("hold pool deletion: %v", err)
	}
	release := func() {
		t.Helper()
		current := &warmpoolv1alpha1.WorkerPool{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), current); err != nil {
			if !apierrors.IsNotFound(err) {
				t.Errorf("get pool for cleanup: %v", err)
			}
			return
		}
		if current.UID != pool.UID {
			return
		}
		controllerutil.RemoveFinalizer(current, finalizer)
		if err := k8sClient.Update(ctx, current); err != nil {
			t.Errorf("release pool deletion: %v", err)
		}
	}
	t.Cleanup(release)
	return release
}

// readyAllPods marks non-terminating Pods in the default pool Ready. It returns
// the number visited, including Pods that were already ready.
func readyAllPods(t *testing.T, ns string) int {
	t.Helper()
	pods := livePoolPods(t, ns, "default")
	for i := range pods {
		if !isPodReady(&pods[i]) {
			markReady(t, &pods[i])
		}
	}
	return len(pods)
}

func claimPhase(t *testing.T, ns, name string) warmpoolv1alpha1.ClaimPhase {
	t.Helper()
	return getClaim(t, ns, name).Status.Phase
}

// A claim binds exactly one warm Pod, and the binding is written on the Pod.
func TestClaimBindsWarmPod(t *testing.T) {
	ns := newNamespace(t)
	startBothManagers(t)

	newPool(t, ns, 1)
	eventually(t, "pool creates one pod", func() bool {
		return len(livePoolPods(t, ns, "default")) == 1
	})
	readyAllPods(t, ns)

	claim := newClaim(t, ns, "claim-a", "default")

	eventually(t, "claim becomes Claimed", func() bool {
		return claimPhase(t, ns, "claim-a") == warmpoolv1alpha1.ClaimPhaseClaimed
	})

	got := getClaim(t, ns, "claim-a")
	if got.Status.PodName == "" {
		t.Fatal("status.podName is empty on a Claimed claim")
	}
	if got.Status.AssignedAt == nil {
		t.Error("status.assignedAt was not set")
	}

	// The Pod, not the claim status, is the source of truth for the binding.
	pod := getPodByName(t, ns, got.Status.PodName)
	if pod.Labels[warmpoolv1alpha1.LabelClaim] != "claim-a" {
		t.Errorf("pod claim label = %q, want claim-a", pod.Labels[warmpoolv1alpha1.LabelClaim])
	}
	if pod.Labels[warmpoolv1alpha1.LabelState] != warmpoolv1alpha1.StateClaimed {
		t.Errorf("pod state label = %q, want claimed", pod.Labels[warmpoolv1alpha1.LabelState])
	}
	if pod.Annotations[warmpoolv1alpha1.AnnotationClaimUID] != string(claim.UID) {
		t.Errorf("pod claim-uid = %q, want %q",
			pod.Annotations[warmpoolv1alpha1.AnnotationClaimUID], claim.UID)
	}
	if classifyPod(pod) != podClaimed {
		t.Errorf("classifyPod = %v, want claimed", classifyPod(pod))
	}
}

// Claiming a Pod removes warm capacity, so the pool must create a replacement:
// warmReplicas counts warm Pods, not total Pods.
func TestPoolReplenishesAfterClaim(t *testing.T) {
	ns := newNamespace(t)
	startBothManagers(t)

	newPool(t, ns, 2)
	eventually(t, "pool creates two pods", func() bool {
		return len(livePoolPods(t, ns, "default")) == 2
	})
	readyAllPods(t, ns)
	eventually(t, "pool reports two warm", func() bool {
		return getPool(t, ns).Status.WarmReplicas == 2
	})

	newClaim(t, ns, "claim-a", "default")
	eventually(t, "claim becomes Claimed", func() bool {
		return claimPhase(t, ns, "claim-a") == warmpoolv1alpha1.ClaimPhaseClaimed
	})

	// The replacement is created because warm dropped to 1, not because a Pod
	// disappeared: the claimed Pod is still running.
	eventually(t, "pool creates a replacement", func() bool {
		return len(livePoolPods(t, ns, "default")) == 3
	})
	readyAllPods(t, ns)

	eventually(t, "pool returns to two warm plus one claimed", func() bool {
		s := getPool(t, ns).Status
		return s.WarmReplicas == 2 && s.ClaimedReplicas == 1 && s.TotalPods == 3
	})

	claimed := getClaim(t, ns, "claim-a").Status.PodName
	pod := getPodByName(t, ns, claimed)
	if pod.DeletionTimestamp != nil {
		t.Error("the claimed pod was deleted; replenishment must add a Pod, not move one")
	}
}

// Two claims against one warm Pod: one wins, the other waits rather than
// sharing or stealing.
func TestPodIsNeverBoundToTwoClaims(t *testing.T) {
	ns := newNamespace(t)
	startBothManagers(t)

	newPool(t, ns, 1)
	eventually(t, "pool creates one pod", func() bool {
		return len(livePoolPods(t, ns, "default")) == 1
	})
	readyAllPods(t, ns)

	newClaim(t, ns, "claim-a", "default")
	newClaim(t, ns, "claim-b", "default")

	eventually(t, "exactly one claim is bound", func() bool {
		a := claimPhase(t, ns, "claim-a")
		b := claimPhase(t, ns, "claim-b")
		bound := 0
		if a == warmpoolv1alpha1.ClaimPhaseClaimed {
			bound++
		}
		if b == warmpoolv1alpha1.ClaimPhaseClaimed {
			bound++
		}
		return bound == 1
	})

	waiting := getClaim(t, ns, "claim-a")
	if waiting.Status.Phase == warmpoolv1alpha1.ClaimPhaseClaimed {
		waiting = getClaim(t, ns, "claim-b")
	}
	eventually(t, "the loser explains why it is waiting", func() bool {
		return getClaim(t, ns, waiting.Name).Status.Reason ==
			warmpoolv1alpha1.ClaimReasonWaitingForWarmPod
	})
	if p := getClaim(t, ns, waiting.Name).Status.PodName; p != "" {
		t.Errorf("pending claim published podName %q", p)
	}

	// No Pod carries two claim labels: a label holds one value, so the real
	// assertion is that each live Pod is claimed by at most one claim and the
	// two claims never name the same Pod.
	readyAllPods(t, ns)
	eventually(t, "the waiting claim is satisfied by the replacement", func() bool {
		return claimPhase(t, ns, waiting.Name) == warmpoolv1alpha1.ClaimPhaseClaimed
	})

	a := getClaim(t, ns, "claim-a").Status.PodName
	b := getClaim(t, ns, "claim-b").Status.PodName
	if a == "" || b == "" {
		t.Fatalf("expected both claims bound, got %q and %q", a, b)
	}
	if a == b {
		t.Fatalf("both claims bound to the same Pod %q", a)
	}
}

// Reconciling a bound claim repeatedly must not bind a second Pod: adoption
// has to win over allocation.
func TestClaimDoesNotAccumulatePods(t *testing.T) {
	ns := newNamespace(t)
	startBothManagers(t)

	newPool(t, ns, 3)
	eventually(t, "pool creates three pods", func() bool {
		return len(livePoolPods(t, ns, "default")) == 3
	})
	readyAllPods(t, ns)

	newClaim(t, ns, "claim-a", "default")
	eventually(t, "claim becomes Claimed", func() bool {
		return claimPhase(t, ns, "claim-a") == warmpoolv1alpha1.ClaimPhaseClaimed
	})
	first := getClaim(t, ns, "claim-a").Status.PodName

	// Poke the claim repeatedly; each update re-enqueues it.
	for i := 0; i < 3; i++ {
		c := getClaim(t, ns, "claim-a")
		if c.Annotations == nil {
			c.Annotations = map[string]string{}
		}
		c.Annotations["poke"] = string(rune('a' + i))
		if err := k8sClient.Update(context.Background(), c); err != nil {
			t.Fatalf("poke claim: %v", err)
		}
		readyAllPods(t, ns)
	}

	eventually(t, "pool settles at three warm", func() bool {
		return getPool(t, ns).Status.WarmReplicas == 3
	})

	bound := 0
	for _, p := range livePoolPods(t, ns, "default") {
		if p.Labels[warmpoolv1alpha1.LabelClaim] == "claim-a" {
			bound++
		}
	}
	if bound != 1 {
		t.Errorf("%d pods bound to claim-a, want 1", bound)
	}
	if got := getClaim(t, ns, "claim-a").Status.PodName; got != first {
		t.Errorf("claim moved from pod %q to %q", first, got)
	}
}

// Claim deletion must request Pod cleanup explicitly because the pool, not
// the claim, is the Pod's garbage-collection owner.
func TestDeletingClaimDeletesItsPod(t *testing.T) {
	ns := newNamespace(t)
	startBothManagers(t)

	newPool(t, ns, 1)
	eventually(t, "pool creates one pod", func() bool {
		return len(livePoolPods(t, ns, "default")) == 1
	})
	readyAllPods(t, ns)

	newClaim(t, ns, "claim-a", "default")
	eventually(t, "claim becomes Claimed", func() bool {
		return claimPhase(t, ns, "claim-a") == warmpoolv1alpha1.ClaimPhaseClaimed
	})
	podName := getClaim(t, ns, "claim-a").Status.PodName

	if err := k8sClient.Delete(context.Background(), getClaim(t, ns, "claim-a")); err != nil {
		t.Fatalf("delete claim: %v", err)
	}

	eventually(t, "claim object is gone", func() bool {
		err := k8sClient.Get(context.Background(),
			client.ObjectKey{Namespace: ns, Name: "claim-a"}, &warmpoolv1alpha1.WorkerClaim{})
		return apierrors.IsNotFound(err)
	})

	// The Pod must stop being live. Whether it lingers with a deletionTimestamp
	// or disappears outright depends on the environment: graceful deletion only
	// applies to a Pod bound to a node, and envtest has no scheduler, so these
	// Pods are removed immediately.
	eventually(t, "the claimed pod is no longer live", func() bool {
		pod := &corev1.Pod{}
		err := k8sClient.Get(context.Background(),
			client.ObjectKey{Namespace: ns, Name: podName}, pod)
		if apierrors.IsNotFound(err) {
			return true
		}
		return err == nil && pod.DeletionTimestamp != nil
	})
}

func TestClaimNameReuseIgnoresOldBindings(t *testing.T) {
	ctx := context.Background()
	ns := newNamespace(t)
	pool := newPool(t, ns, 0)
	oldClaim := newClaim(t, ns, "claim-a", pool.Name)
	oldPod := newBoundPod(t, pool, oldClaim)
	if err := k8sClient.Delete(ctx, oldClaim); err != nil {
		t.Fatalf("delete previous claim: %v", err)
	}

	claim := newClaim(t, ns, oldClaim.Name, pool.Name)
	if claim.UID == oldClaim.UID {
		t.Fatal("recreated claim must have a different UID")
	}
	controllerutil.AddFinalizer(claim, warmpoolv1alpha1.ClaimFinalizer)
	if err := k8sClient.Update(ctx, claim); err != nil {
		t.Fatalf("add finalizer: %v", err)
	}
	missingUID := newBoundPod(t, pool, claim)
	delete(missingUID.Annotations, warmpoolv1alpha1.AnnotationClaimUID)
	if err := k8sClient.Update(ctx, missingUID); err != nil {
		t.Fatalf("remove claim UID: %v", err)
	}

	r := &WorkerClaimReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme}
	adopted, err := r.adopt(ctx, claim)
	if err != nil || adopted != nil {
		t.Fatalf("adopted a stale or missing-UID binding: pod=%v err=%v", adopted, err)
	}

	owned := newBoundPod(t, pool, claim)
	adopted, err = r.adopt(ctx, claim)
	if err != nil {
		t.Fatalf("adopt current binding among stale bindings: %v", err)
	}
	if adopted == nil || adopted.Name != owned.Name {
		t.Fatalf("adopted pod = %v, want %s", adopted, owned.Name)
	}
	duplicate := newBoundPod(t, pool, claim)
	if _, err := r.adopt(ctx, claim); err == nil {
		t.Fatal("two Pods with the current claim UID must still be rejected")
	}

	if err := k8sClient.Delete(ctx, claim); err != nil {
		t.Fatalf("delete current claim: %v", err)
	}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(claim)}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("finalize current claim: %v", err)
	}
	if err := k8sClient.Get(ctx, req.NamespacedName, &warmpoolv1alpha1.WorkerClaim{}); !apierrors.IsNotFound(err) {
		t.Fatalf("claim finalizer was not released: %v", err)
	}
	for _, pod := range []*corev1.Pod{owned, duplicate} {
		remaining := &corev1.Pod{}
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), remaining)
		if !apierrors.IsNotFound(err) && (err != nil || remaining.DeletionTimestamp == nil) {
			t.Errorf("current claim's Pod %s was not deleted: %v", pod.Name, err)
		}
	}
	for _, pod := range []*corev1.Pod{oldPod, missingUID} {
		remaining := getPodByName(t, ns, pod.Name)
		if remaining.DeletionTimestamp != nil || remaining.ResourceVersion != pod.ResourceVersion {
			t.Errorf("cleanup modified a Pod not owned by the current claim: %s", pod.Name)
		}
	}
}

type rebindBeforeDeleteClient struct {
	client.Client
	newClaim *warmpoolv1alpha1.WorkerClaim
}

func (c *rebindBeforeDeleteClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	pod := &corev1.Pod{}
	if err := c.Client.Get(ctx, client.ObjectKeyFromObject(obj), pod); err != nil {
		return err
	}
	pod.Labels[warmpoolv1alpha1.LabelClaim] = c.newClaim.Name
	pod.Annotations[warmpoolv1alpha1.AnnotationClaimUID] = string(c.newClaim.UID)
	if err := c.Client.Update(ctx, pod); err != nil {
		return err
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func TestClaimFinalizerRejectsChangedBinding(t *testing.T) {
	ctx := context.Background()
	ns := newNamespace(t)
	pool := newPool(t, ns, 0)
	claim := newClaim(t, ns, "claim-a", pool.Name)
	controllerutil.AddFinalizer(claim, warmpoolv1alpha1.ClaimFinalizer)
	if err := k8sClient.Update(ctx, claim); err != nil {
		t.Fatalf("add finalizer: %v", err)
	}
	pod := newBoundPod(t, pool, claim)
	otherClaim := newClaim(t, ns, "claim-b", pool.Name)
	if err := k8sClient.Delete(ctx, claim); err != nil {
		t.Fatalf("delete claim: %v", err)
	}
	r := &WorkerClaimReconciler{
		Client:    &rebindBeforeDeleteClient{Client: k8sClient, newClaim: otherClaim},
		APIReader: k8sClient,
		Scheme:    scheme,
	}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(claim)}
	if _, err := r.Reconcile(ctx, req); !apierrors.IsConflict(err) {
		t.Fatalf("expected deletion conflict after binding changed, got %v", err)
	}
	if !controllerutil.ContainsFinalizer(getClaim(t, ns, claim.Name), warmpoolv1alpha1.ClaimFinalizer) {
		t.Fatal("claim finalizer was removed despite the deletion conflict")
	}
	remaining := getPodByName(t, ns, pod.Name)
	if remaining.DeletionTimestamp != nil ||
		remaining.Annotations[warmpoolv1alpha1.AnnotationClaimUID] != string(otherClaim.UID) {
		t.Fatal("cleanup deleted or overwrote the new binding")
	}

	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("retry finalization with the current binding: %v", err)
	}
	if err := k8sClient.Get(ctx, req.NamespacedName, &warmpoolv1alpha1.WorkerClaim{}); !apierrors.IsNotFound(err) {
		t.Fatalf("claim finalizer was not released after re-observing: %v", err)
	}
	if getPodByName(t, ns, pod.Name).DeletionTimestamp != nil {
		t.Fatal("retry deleted the other claim's Pod")
	}
}

type stalePoolClient struct {
	client.Client
	pool *warmpoolv1alpha1.WorkerPool
}

func (c *stalePoolClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if pool, ok := obj.(*warmpoolv1alpha1.WorkerPool); ok && key == client.ObjectKeyFromObject(c.pool) {
		c.pool.DeepCopyInto(pool)
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func TestClaimHandlesDeletingPoolWithStaleCache(t *testing.T) {
	for _, tc := range []struct {
		name     string
		assigned bool
	}{
		{name: "unassigned"},
		{name: "already-assigned", assigned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ns := newNamespace(t)
			pool := newPool(t, ns, 1)
			holdPoolDeletion(t, pool)
			stalePool := pool.DeepCopy()
			claim := newClaim(t, ns, "claim-a", pool.Name)
			controllerutil.AddFinalizer(claim, warmpoolv1alpha1.ClaimFinalizer)
			if err := k8sClient.Update(ctx, claim); err != nil {
				t.Fatalf("add claim finalizer: %v", err)
			}
			var pod *corev1.Pod
			if tc.assigned {
				pod = newBoundPod(t, pool, claim)
				assignedAt := metav1.Now()
				claim.Status = warmpoolv1alpha1.WorkerClaimStatus{
					Phase: warmpoolv1alpha1.ClaimPhaseClaimed, PodName: pod.Name,
					Reason: warmpoolv1alpha1.ClaimReasonAssigned, AssignedAt: &assignedAt,
				}
				if err := k8sClient.Status().Update(ctx, claim); err != nil {
					t.Fatalf("publish assignment: %v", err)
				}
			} else {
				pod = buildPod(pool)
				if err := ctrl.SetControllerReference(pool, pod, scheme); err != nil {
					t.Fatalf("set pod owner: %v", err)
				}
				if err := k8sClient.Create(ctx, pod); err != nil {
					t.Fatalf("create warm pod: %v", err)
				}
				markReady(t, pod)
			}
			pod = getPodByName(t, ns, pod.Name)
			if err := k8sClient.Delete(ctx, pool); err != nil {
				t.Fatalf("delete pool: %v", err)
			}
			if getPool(t, ns).DeletionTimestamp == nil {
				t.Fatal("pool must be terminating in the API server")
			}

			r := &WorkerClaimReconciler{
				Client:    &stalePoolClient{Client: k8sClient, pool: stalePool},
				APIReader: k8sClient,
				Scheme:    scheme,
			}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(claim)}
			if _, err := r.Reconcile(ctx, req); err != nil {
				t.Fatalf("reconcile claim against deleting pool: %v", err)
			}
			got := getClaim(t, ns, claim.Name)
			if tc.assigned {
				if got.Status.Phase != warmpoolv1alpha1.ClaimPhaseClaimed ||
					got.Status.PodName != pod.Name || got.ResourceVersion != claim.ResourceVersion {
					t.Errorf("pool deletion changed an existing live assignment: %+v", got.Status)
				}
			} else if got.Status.Phase != warmpoolv1alpha1.ClaimPhasePending ||
				got.Status.Reason != warmpoolv1alpha1.ClaimReasonPoolTerminating ||
				got.Status.PodName != "" || got.Status.AssignedAt != nil {
				t.Errorf("expected unassigned Pending/PoolTerminating, got %+v", got.Status)
			}
			if current := getPodByName(t, ns, pod.Name); current.ResourceVersion != pod.ResourceVersion {
				t.Errorf("reconcile modified pod from deleting pool: %+v", current.Labels)
			}
		})
	}
}

func TestPendingClaimTracksPoolDeletionAndRecreation(t *testing.T) {
	ctx := context.Background()
	ns := newNamespace(t)
	pool := newPool(t, ns, 0)
	releasePool := holdPoolDeletion(t, pool)
	startBothManagers(t)
	newClaim(t, ns, "claim-a", pool.Name)
	pendingWithReason := func(reason string) bool {
		c := getClaim(t, ns, "claim-a")
		return c.Status.Phase == warmpoolv1alpha1.ClaimPhasePending &&
			c.Status.Reason == reason && c.Status.PodName == ""
	}
	eventually(t, "claim waits for capacity", func() bool {
		return pendingWithReason(warmpoolv1alpha1.ClaimReasonWaitingForWarmPod)
	})
	consistently(t, "claim remains idle without pod events", func() bool {
		return pendingWithReason(warmpoolv1alpha1.ClaimReasonWaitingForWarmPod)
	})

	if err := k8sClient.Delete(ctx, pool); err != nil {
		t.Fatalf("delete empty pool: %v", err)
	}
	eventually(t, "pool update wakes pending claim", func() bool {
		return pendingWithReason(warmpoolv1alpha1.ClaimReasonPoolTerminating)
	})
	releasePool()
	eventually(t, "pool removal wakes pending claim", func() bool {
		return pendingWithReason(warmpoolv1alpha1.ClaimReasonPoolNotFound)
	})

	newPool(t, ns, 0)
	eventually(t, "pool recreation wakes pending claim without pods", func() bool {
		return pendingWithReason(warmpoolv1alpha1.ClaimReasonWaitingForWarmPod)
	})
	scalePool(t, ns, 1)
	eventually(t, "recreated pool produces a worker", func() bool {
		return len(livePoolPods(t, ns, "default")) == 1
	})
	readyAllPods(t, ns)
	eventually(t, "claim receives a worker from the recreated pool", func() bool {
		return claimPhase(t, ns, "claim-a") == warmpoolv1alpha1.ClaimPhaseClaimed
	})
}

// A claim whose pool does not exist must say so rather than hang silently.
func TestClaimWithMissingPoolReportsWhy(t *testing.T) {
	ns := newNamespace(t)
	startBothManagers(t)

	newClaim(t, ns, "claim-a", "no-such-pool")

	eventually(t, "claim reports PoolNotFound", func() bool {
		c := getClaim(t, ns, "claim-a")
		return c.Status.Phase == warmpoolv1alpha1.ClaimPhasePending &&
			c.Status.Reason == warmpoolv1alpha1.ClaimReasonPoolNotFound
	})
}

// A claim must never be handed a Pod that is not yet Ready.
func TestClaimWaitsForReadiness(t *testing.T) {
	ns := newNamespace(t)
	startBothManagers(t)

	newPool(t, ns, 1)
	eventually(t, "pool creates one pod", func() bool {
		return len(livePoolPods(t, ns, "default")) == 1
	})

	newClaim(t, ns, "claim-a", "default")
	eventually(t, "claim reports it is waiting", func() bool {
		return getClaim(t, ns, "claim-a").Status.Reason ==
			warmpoolv1alpha1.ClaimReasonWaitingForWarmPod
	})

	readyAllPods(t, ns)
	eventually(t, "claim binds once the pod is Ready", func() bool {
		return claimPhase(t, ns, "claim-a") == warmpoolv1alpha1.ClaimPhaseClaimed
	})
}

// spec.poolName is immutable: a bound claim must not be able to repoint at a
// different pool.
func TestClaimPoolNameIsImmutable(t *testing.T) {
	ns := newNamespace(t)

	newClaim(t, ns, "claim-a", "pool-one")

	claim := getClaim(t, ns, "claim-a")
	claim.Spec.PoolName = "pool-two"
	err := k8sClient.Update(context.Background(), claim)
	if err == nil {
		t.Fatal("spec.poolName was mutable")
	}
	if !strings.Contains(err.Error(), "immutable") {
		t.Errorf("unexpected error: %v", err)
	}
}

// The claim published a Pod name, so the client may have begun
// using it. Losing that Pod is terminal -- a silent reassignment would hide the
// loss behind a healthy-looking claim.
func TestLosingAPublishedPodFailsTheClaim(t *testing.T) {
	ns := newNamespace(t)
	startBothManagers(t)

	newPool(t, ns, 2)
	eventually(t, "pool creates two pods", func() bool {
		return len(livePoolPods(t, ns, "default")) == 2
	})
	readyAllPods(t, ns)

	newClaim(t, ns, "claim-a", "default")
	eventually(t, "claim becomes Claimed", func() bool {
		return claimPhase(t, ns, "claim-a") == warmpoolv1alpha1.ClaimPhaseClaimed
	})
	assigned := getClaim(t, ns, "claim-a").Status.PodName

	if err := k8sClient.Delete(context.Background(), getPodByName(t, ns, assigned)); err != nil {
		t.Fatalf("delete assigned pod: %v", err)
	}

	eventually(t, "claim fails with PodLost", func() bool {
		c := getClaim(t, ns, "claim-a")
		return c.Status.Phase == warmpoolv1alpha1.ClaimPhaseFailed &&
			c.Status.Reason == warmpoolv1alpha1.ClaimReasonPodLost
	})

	// Keep the pool supplied so a reassignment would certainly succeed if the
	// controller were willing to make one.
	for i := 0; i < 3; i++ {
		readyAllPods(t, ns)
		time.Sleep(300 * time.Millisecond)
	}

	c := getClaim(t, ns, "claim-a")
	if c.Status.Phase != warmpoolv1alpha1.ClaimPhaseFailed {
		t.Fatalf("claim left Failed: phase=%s podName=%s", c.Status.Phase, c.Status.PodName)
	}
	if c.Status.PodName != assigned {
		t.Errorf("podName = %q, want %q retained as forensic evidence", c.Status.PodName, assigned)
	}

	// No other Pod may be assigned as a replacement for the failed claim.
	for _, p := range livePoolPods(t, ns, "default") {
		if p.Labels[warmpoolv1alpha1.LabelClaim] == "claim-a" && p.Name != assigned {
			t.Errorf("claim was silently reassigned to pod %s", p.Name)
		}
	}
}

// A bound Pod that crashes to Failed is just as lost as a deleted one, even
// though it has no deletionTimestamp.
func TestBoundPodInTerminalPhaseFailsTheClaim(t *testing.T) {
	ns := newNamespace(t)
	startBothManagers(t)

	newPool(t, ns, 2)
	eventually(t, "pool creates two pods", func() bool {
		return len(livePoolPods(t, ns, "default")) == 2
	})
	readyAllPods(t, ns)

	newClaim(t, ns, "claim-a", "default")
	eventually(t, "claim becomes Claimed", func() bool {
		return claimPhase(t, ns, "claim-a") == warmpoolv1alpha1.ClaimPhaseClaimed
	})
	assigned := getClaim(t, ns, "claim-a").Status.PodName

	pod := getPodByName(t, ns, assigned)
	pod.Status.Phase = corev1.PodFailed
	if err := k8sClient.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("fail the pod: %v", err)
	}

	eventually(t, "claim fails with PodLost", func() bool {
		c := getClaim(t, ns, "claim-a")
		return c.Status.Phase == warmpoolv1alpha1.ClaimPhaseFailed &&
			c.Status.Reason == warmpoolv1alpha1.ClaimReasonPodLost
	})
}

func TestUnpublishedTerminalBindingDoesNotPublishClaimed(t *testing.T) {
	tests := []struct {
		name        string
		podPhase    corev1.PodPhase
		terminating bool
		wantPhase   warmpoolv1alpha1.ClaimPhase
		wantReason  string
	}{
		{
			name: "failed", podPhase: corev1.PodFailed,
			wantPhase: warmpoolv1alpha1.ClaimPhaseFailed, wantReason: warmpoolv1alpha1.ClaimReasonPodLost,
		},
		{
			name: "succeeded", podPhase: corev1.PodSucceeded,
			wantPhase: warmpoolv1alpha1.ClaimPhaseSucceeded, wantReason: warmpoolv1alpha1.ClaimReasonPodCompleted,
		},
		{
			name: "terminating", podPhase: corev1.PodRunning, terminating: true,
			wantPhase: warmpoolv1alpha1.ClaimPhaseFailed, wantReason: warmpoolv1alpha1.ClaimReasonPodLost,
		},
		{
			name: "succeeded-while-terminating", podPhase: corev1.PodSucceeded, terminating: true,
			wantPhase: warmpoolv1alpha1.ClaimPhaseSucceeded, wantReason: warmpoolv1alpha1.ClaimReasonPodCompleted,
		},
	}
	for _, initial := range []struct {
		name  string
		phase warmpoolv1alpha1.ClaimPhase
	}{
		{name: "uninitialized"},
		{name: "pending", phase: warmpoolv1alpha1.ClaimPhasePending},
	} {
		t.Run(initial.name, func(t *testing.T) {
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					ctx := context.Background()
					ns := newNamespace(t)
					pool := newPool(t, ns, 0)
					claim := newClaim(t, ns, "claim-a", pool.Name)
					controllerutil.AddFinalizer(claim, warmpoolv1alpha1.ClaimFinalizer)
					if err := k8sClient.Update(ctx, claim); err != nil {
						t.Fatalf("add claim finalizer: %v", err)
					}
					if initial.phase != "" {
						claim.Status.Phase = initial.phase
						if err := k8sClient.Status().Update(ctx, claim); err != nil {
							t.Fatalf("initialize claim phase: %v", err)
						}
					}

					pod := buildPod(pool)
					pod.Labels[warmpoolv1alpha1.LabelClaim] = claim.Name
					pod.Labels[warmpoolv1alpha1.LabelState] = warmpoolv1alpha1.StateClaimed
					pod.Annotations = map[string]string{warmpoolv1alpha1.AnnotationClaimUID: string(claim.UID)}
					pod.Spec.RestartPolicy = corev1.RestartPolicyNever
					const holdFinalizer = "tests.warmpool.linkedin.com/hold"
					if tc.terminating {
						controllerutil.AddFinalizer(pod, holdFinalizer)
					}
					if err := ctrl.SetControllerReference(pool, pod, scheme); err != nil {
						t.Fatalf("set pod owner: %v", err)
					}
					if err := k8sClient.Create(ctx, pod); err != nil {
						t.Fatalf("create bound pod: %v", err)
					}
					if tc.terminating {
						t.Cleanup(func() {
							remaining := getPodByName(t, ns, pod.Name)
							controllerutil.RemoveFinalizer(remaining, holdFinalizer)
							if err := k8sClient.Update(ctx, remaining); err != nil {
								t.Errorf("release pod deletion: %v", err)
							}
						})
					}
					markReady(t, pod)
					pod = getPodByName(t, ns, pod.Name)
					pod.Status.Phase = tc.podPhase
					if err := k8sClient.Status().Update(ctx, pod); err != nil {
						t.Fatalf("set pod phase: %v", err)
					}
					if tc.terminating {
						if err := k8sClient.Delete(ctx, pod); err != nil {
							t.Fatalf("terminate bound pod: %v", err)
						}
					}

					// One cold reconcile must publish the final outcome, not an intermediate Claimed state.
					r := &WorkerClaimReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme}
					req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(claim)}
					if _, err := r.Reconcile(ctx, req); err != nil {
						t.Fatalf("reconcile unpublished binding: %v", err)
					}
					got := getClaim(t, ns, claim.Name)
					if got.Status.Phase != tc.wantPhase || got.Status.Reason != tc.wantReason {
						t.Fatalf("status = %+v, want phase=%s reason=%s", got.Status, tc.wantPhase, tc.wantReason)
					}
					if got.Status.PodName != pod.Name || !strings.Contains(got.Status.Message, pod.Name) {
						t.Errorf("terminal status does not identify bound pod %s: %+v", pod.Name, got.Status)
					}
					if got.Status.AssignedAt != nil {
						t.Error("terminal publication manufactured an assignment timestamp")
					}
					if _, err := r.Reconcile(ctx, req); err != nil {
						t.Fatalf("reconcile terminal claim: %v", err)
					}
					if getClaim(t, ns, claim.Name).ResourceVersion != got.ResourceVersion {
						t.Error("reconciling a terminal claim changed its status")
					}
					if len(allPods(t, ns)) != 1 {
						t.Error("reconciling a terminal binding created another pod")
					}
				})
			}
		})
	}
}

func TestSucceededPodCompletesClaim(t *testing.T) {
	ctx := context.Background()
	ns := newNamespace(t)
	startBothManagers(t)

	newPool(t, ns, 2)
	eventually(t, "pool creates two pods", func() bool {
		return len(livePoolPods(t, ns, "default")) == 2
	})
	readyAllPods(t, ns)
	newClaim(t, ns, "claim-a", "default")
	eventually(t, "claim becomes Claimed", func() bool {
		return claimPhase(t, ns, "claim-a") == warmpoolv1alpha1.ClaimPhaseClaimed
	})
	assigned := getClaim(t, ns, "claim-a")

	pod := getPodByName(t, ns, assigned.Status.PodName)
	pod.Status.Phase = corev1.PodSucceeded
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		t.Fatalf("complete pod: %v", err)
	}
	eventually(t, "claim succeeds with PodCompleted", func() bool {
		c := getClaim(t, ns, "claim-a")
		return c.Status.Phase == warmpoolv1alpha1.ClaimPhaseSucceeded &&
			c.Status.Reason == warmpoolv1alpha1.ClaimReasonPodCompleted
	})

	if err := k8sClient.Delete(ctx, getPodByName(t, ns, assigned.Status.PodName)); err != nil {
		t.Fatalf("delete completed pod: %v", err)
	}
	r := &WorkerClaimReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(assigned)}); err != nil {
		t.Fatalf("reconcile terminal claim with a fresh reconciler: %v", err)
	}
	consistently(t, "successful claim stays terminal after pod deletion", func() bool {
		c := getClaim(t, ns, "claim-a")
		if c.Status.Phase != warmpoolv1alpha1.ClaimPhaseSucceeded ||
			c.Status.Reason != warmpoolv1alpha1.ClaimReasonPodCompleted ||
			c.Status.PodName != assigned.Status.PodName ||
			c.Status.AssignedAt == nil || !c.Status.AssignedAt.Equal(assigned.Status.AssignedAt) {
			return false
		}
		for _, p := range livePoolPods(t, ns, "default") {
			if p.Labels[warmpoolv1alpha1.LabelClaim] == "claim-a" {
				return false
			}
		}
		return true
	})

	if err := k8sClient.Delete(ctx, getClaim(t, ns, "claim-a")); err != nil {
		t.Fatalf("delete successful claim: %v", err)
	}
	eventually(t, "successful claim finalizer is released", func() bool {
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(assigned), &warmpoolv1alpha1.WorkerClaim{})
		return apierrors.IsNotFound(err)
	})
}

func TestTerminatingPodFailsClaim(t *testing.T) {
	ctx := context.Background()
	ns := newNamespace(t)
	startBothManagers(t)

	newPool(t, ns, 1)
	eventually(t, "pool creates one pod", func() bool {
		return len(livePoolPods(t, ns, "default")) == 1
	})
	readyAllPods(t, ns)
	newClaim(t, ns, "claim-a", "default")
	eventually(t, "claim becomes Claimed", func() bool {
		return claimPhase(t, ns, "claim-a") == warmpoolv1alpha1.ClaimPhaseClaimed
	})
	assigned := getClaim(t, ns, "claim-a")
	pod := getPodByName(t, ns, assigned.Status.PodName)
	const holdFinalizer = "tests.warmpool.linkedin.com/hold"
	controllerutil.AddFinalizer(pod, holdFinalizer)
	if err := k8sClient.Update(ctx, pod); err != nil {
		t.Fatalf("hold pod deletion: %v", err)
	}
	t.Cleanup(func() {
		remaining := &corev1.Pod{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), remaining); err != nil {
			if !apierrors.IsNotFound(err) {
				t.Errorf("get terminating pod for cleanup: %v", err)
			}
			return
		}
		controllerutil.RemoveFinalizer(remaining, holdFinalizer)
		if err := k8sClient.Update(ctx, remaining); err != nil {
			t.Errorf("release pod deletion: %v", err)
		}
	})
	if err := k8sClient.Delete(ctx, pod); err != nil {
		t.Fatalf("delete assigned pod: %v", err)
	}
	eventually(t, "claim fails before pod disappears", func() bool {
		c := getClaim(t, ns, "claim-a")
		return c.Status.Phase == warmpoolv1alpha1.ClaimPhaseFailed &&
			c.Status.Reason == warmpoolv1alpha1.ClaimReasonPodLost &&
			c.Status.PodName == assigned.Status.PodName
	})
	if getPodByName(t, ns, pod.Name).DeletionTimestamp == nil {
		t.Fatal("expected pod to remain terminating behind its finalizer")
	}
}

// Readiness loss alone does not make an assigned Pod terminal.
func TestNotReadyBoundPodDoesNotFailTheClaim(t *testing.T) {
	ns := newNamespace(t)
	startBothManagers(t)

	newPool(t, ns, 2)
	eventually(t, "pool creates two pods", func() bool {
		return len(livePoolPods(t, ns, "default")) == 2
	})
	readyAllPods(t, ns)

	newClaim(t, ns, "claim-a", "default")
	eventually(t, "claim becomes Claimed", func() bool {
		return claimPhase(t, ns, "claim-a") == warmpoolv1alpha1.ClaimPhaseClaimed
	})
	assigned := getClaim(t, ns, "claim-a").Status.PodName

	pod := getPodByName(t, ns, assigned)
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			pod.Status.Conditions[i].Status = corev1.ConditionFalse
		}
	}
	if err := k8sClient.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("flip readiness: %v", err)
	}

	consistently(t, "NotReady pod keeps its claim", func() bool {
		c := getClaim(t, ns, "claim-a")
		return c.Status.Phase == warmpoolv1alpha1.ClaimPhaseClaimed && c.Status.PodName == assigned
	})
}

// Two live Pods carrying one claim's UID means the invariant the design rests
// on is already broken. The controller must refuse rather than pick a winner:
// it cannot tell which Pod the client is using, so deleting either is a guess
// that can destroy live work.
func TestDuplicateBindingIsRefusedNotRepaired(t *testing.T) {
	ns := newNamespace(t)
	startBothManagers(t)

	newPool(t, ns, 3)
	eventually(t, "pool creates three pods", func() bool {
		return len(livePoolPods(t, ns, "default")) == 3
	})
	readyAllPods(t, ns)

	newClaim(t, ns, "claim-a", "default")
	eventually(t, "claim becomes Claimed", func() bool {
		return claimPhase(t, ns, "claim-a") == warmpoolv1alpha1.ClaimPhaseClaimed
	})
	claim := getClaim(t, ns, "claim-a")
	assigned := claim.Status.PodName

	// Forge a second binding, which is the state a split-brain writer would
	// leave behind.
	var impostor *corev1.Pod
	for _, p := range livePoolPods(t, ns, "default") {
		if p.Name != assigned && p.Labels[warmpoolv1alpha1.LabelClaim] == "" {
			impostor = p.DeepCopy()
			break
		}
	}
	if impostor == nil {
		t.Fatal("no spare warm pod to forge a duplicate binding with")
	}
	impostor.Labels[warmpoolv1alpha1.LabelClaim] = "claim-a"
	impostor.Labels[warmpoolv1alpha1.LabelState] = warmpoolv1alpha1.StateClaimed
	if impostor.Annotations == nil {
		impostor.Annotations = map[string]string{}
	}
	impostor.Annotations[warmpoolv1alpha1.AnnotationClaimUID] = string(claim.UID)
	if err := k8sClient.Update(context.Background(), impostor); err != nil {
		t.Fatalf("forge duplicate binding: %v", err)
	}

	// Poke the claim so the reconciler is guaranteed to observe both Pods.
	for i := 0; i < 3; i++ {
		c := getClaim(t, ns, "claim-a")
		if c.Annotations == nil {
			c.Annotations = map[string]string{}
		}
		c.Annotations["poke"] = string(rune('a' + i))
		if err := k8sClient.Update(context.Background(), c); err != nil {
			t.Fatalf("poke claim: %v", err)
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Neither Pod may be deleted: the controller stalls rather than guessing.
	for _, name := range []string{assigned, impostor.Name} {
		pod := &corev1.Pod{}
		err := k8sClient.Get(context.Background(),
			client.ObjectKey{Namespace: ns, Name: name}, pod)
		if err != nil {
			t.Fatalf("pod %s should have been left alone, got: %v", name, err)
		}
		if pod.DeletionTimestamp != nil {
			t.Errorf("pod %s was deleted; the controller guessed instead of refusing", name)
		}
	}

	// The previously published Pod name must remain unchanged.
	if got := getClaim(t, ns, "claim-a").Status.PodName; got != assigned {
		t.Errorf("claim podName moved to %q; it must not be republished while ambiguous", got)
	}
}
