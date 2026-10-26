package controller

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	warmpoolv1alpha1 "github.com/linkedin/warmpool/api/v1alpha1"
)

var (
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
	scheme    = runtime.NewScheme()
)

func TestMain(m *testing.M) {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(warmpoolv1alpha1.AddToScheme(scheme))

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd")},
		ErrorIfCRDPathMissing: true,
	}

	var err error
	cfg, err = testEnv.Start()
	if err != nil {
		panic(err)
	}

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		panic(err)
	}

	code := m.Run()
	_ = testEnv.Stop()
	os.Exit(code)
}

// startManager runs the reconciler for the duration of a test and returns when
// its cache is ready.
func startManager(t *testing.T) {
	t.Helper()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		Metrics:        server.Options{BindAddress: "0"},
		LeaderElection: false,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	if err := (&WorkerPoolReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("workerpool-controller"),
		// controller-runtime requires controller names to be unique within a
		// process, and each test starts its own manager.
		ControllerName: "workerpool-" + strings.ToLower(t.Name()),
	}).SetupWithManager(mgr); err != nil {
		t.Fatalf("setup reconciler: %v", err)
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

// newNamespace isolates each test; envtest does not reclaim deleted namespaces.
func newNamespace(t *testing.T) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "wp-test-"}}
	if err := k8sClient.Create(context.Background(), ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	return ns.Name
}

func newPool(t *testing.T, ns string, warm int32) *warmpoolv1alpha1.WorkerPool {
	t.Helper()
	pool := &warmpoolv1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: ns},
		Spec: warmpoolv1alpha1.WorkerPoolSpec{
			WarmReplicas: &warm,
			Template: warmpoolv1alpha1.WorkerPodTemplate{
				Metadata: warmpoolv1alpha1.WorkerPodTemplateMetadata{
					Labels: map[string]string{"app": "warm-worker"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "worker", Image: "nginx:alpine"}},
				},
			},
		},
	}
	if err := k8sClient.Create(context.Background(), pool); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	return pool
}

// allPods ignores labels entirely: label-based assertions cannot detect a Pod
// that was hidden by label tampering.
func allPods(t *testing.T, ns string) []corev1.Pod {
	t.Helper()
	var list corev1.PodList
	if err := k8sClient.List(context.Background(), &list, client.InNamespace(ns)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return list.Items
}

func listPoolPods(t *testing.T, ns, pool string) []corev1.Pod {
	t.Helper()
	var list corev1.PodList
	if err := k8sClient.List(context.Background(), &list,
		client.InNamespace(ns),
		client.MatchingLabels{warmpoolv1alpha1.LabelPool: pool},
	); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return list.Items
}

// livePoolPods excludes deleting Pods whether they disappear immediately or
// remain visible during termination. It does not filter terminal Pod phases.
func livePoolPods(t *testing.T, ns, pool string) []corev1.Pod {
	t.Helper()
	var live []corev1.Pod
	for _, p := range listPoolPods(t, ns, pool) {
		if p.DeletionTimestamp == nil {
			live = append(live, p)
		}
	}
	return live
}

// scalePool updates spec.warmReplicas on the pool.
func scalePool(t *testing.T, ns string, warm int32) {
	t.Helper()
	pool := getPool(t, ns)
	pool.Spec.WarmReplicas = &warm
	if err := k8sClient.Update(context.Background(), pool); err != nil {
		t.Fatalf("scale pool to %d: %v", warm, err)
	}
}

// claimPod sets claim labels for pool-only tests without running the claim controller.
func claimPod(t *testing.T, pod *corev1.Pod, claim string) {
	t.Helper()
	fresh := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	patched := fresh.DeepCopy()
	patched.Labels[warmpoolv1alpha1.LabelClaim] = claim
	patched.Labels[warmpoolv1alpha1.LabelState] = warmpoolv1alpha1.StateClaimed
	if err := k8sClient.Patch(context.Background(), patched, client.MergeFrom(fresh)); err != nil {
		t.Fatalf("claim pod: %v", err)
	}
}

// markReady simulates the kubelet's readiness update, which envtest does not provide.
func markReady(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	fresh := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	fresh.Status.Phase = corev1.PodRunning
	fresh.Status.PodIP = "10.244.0.1"
	fresh.Status.Conditions = []corev1.PodCondition{{
		Type:               corev1.PodReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	}}
	if err := k8sClient.Status().Update(context.Background(), fresh); err != nil {
		t.Fatalf("mark pod ready: %v", err)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// consistently guards against the overshoot bug: asserting a condition held
// true for a window, not just at one instant.
func consistently(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("condition stopped holding: %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func getPool(t *testing.T, ns string) *warmpoolv1alpha1.WorkerPool {
	t.Helper()
	pool := &warmpoolv1alpha1.WorkerPool{}
	if err := k8sClient.Get(context.Background(),
		client.ObjectKey{Namespace: ns, Name: "default"}, pool); err != nil {
		t.Fatalf("get pool: %v", err)
	}
	return pool
}

// Scenario 1: a WorkerPool with warmReplicas 3 results in three Pods, which
// become three warm Pods once they are Ready.
func TestPoolCreatesDesiredPods(t *testing.T) {
	startManager(t)
	ns := newNamespace(t)
	newPool(t, ns, 3)

	eventually(t, "3 pods created", func() bool {
		return len(listPoolPods(t, ns, "default")) == 3
	})

	// Before readiness they are warming, not warm. Reporting them as warm would
	// let a claim be handed a Pod that cannot serve traffic.
	eventually(t, "status reports 3 warming", func() bool {
		s := getPool(t, ns).Status
		return s.WarmingReplicas == 3 && s.WarmReplicas == 0 && s.TotalPods == 3
	})

	pods := listPoolPods(t, ns, "default")
	for i := range pods {
		markReady(t, &pods[i])
	}

	eventually(t, "status reports 3 warm", func() bool {
		s := getPool(t, ns).Status
		return s.WarmReplicas == 3 && s.WarmingReplicas == 0 && s.TotalPods == 3
	})

	// Still exactly three Pods: readiness transitions must not trigger creation.
	if got := len(listPoolPods(t, ns, "default")); got != 3 {
		t.Fatalf("pods = %d, want 3", got)
	}

	pool := getPool(t, ns)
	cond := findCondition(pool.Status.Conditions, warmpoolv1alpha1.PoolConditionWarmCapacityReady)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("WarmCapacityReady = %v, want True", cond)
	}
	if cond.Reason != warmpoolv1alpha1.PoolReasonAtCapacity {
		t.Errorf("reason = %q, want %q", cond.Reason, warmpoolv1alpha1.PoolReasonAtCapacity)
	}
	if pool.Status.ObservedGeneration != pool.Generation {
		t.Errorf("observedGeneration = %d, want %d", pool.Status.ObservedGeneration, pool.Generation)
	}
}

// The overshoot guard, end to end. Pods take time to become Ready, and the
// reconciler runs repeatedly in that window. Counting only warm Pods would
// create a new batch on every pass.
func TestPoolDoesNotOvershootWhileWarming(t *testing.T) {
	startManager(t)
	ns := newNamespace(t)
	newPool(t, ns, 3)

	eventually(t, "3 pods created", func() bool {
		return len(listPoolPods(t, ns, "default")) == 3
	})

	// No Pod is ever marked Ready, so warm stays 0 for the whole window. The
	// count must still not grow.
	consistently(t, "pod count stays at 3 while warming", func() bool {
		return len(listPoolPods(t, ns, "default")) == 3
	})
}

// Scaling up must create only the difference, not a fresh set.
func TestPoolScalesUp(t *testing.T) {
	startManager(t)
	ns := newNamespace(t)
	newPool(t, ns, 1)

	eventually(t, "1 pod created", func() bool {
		return len(listPoolPods(t, ns, "default")) == 1
	})
	pods := listPoolPods(t, ns, "default")
	markReady(t, &pods[0])
	eventually(t, "1 warm", func() bool {
		return getPool(t, ns).Status.WarmReplicas == 1
	})
	firstPodName := pods[0].Name

	pool := getPool(t, ns)
	three := int32(3)
	pool.Spec.WarmReplicas = &three
	if err := k8sClient.Update(context.Background(), pool); err != nil {
		t.Fatalf("scale up: %v", err)
	}

	eventually(t, "3 pods after scale up", func() bool {
		return len(listPoolPods(t, ns, "default")) == 3
	})

	// The existing warm Pod must be kept, not replaced.
	var found bool
	for _, p := range listPoolPods(t, ns, "default") {
		if p.Name == firstPodName {
			found = true
		}
	}
	if !found {
		t.Error("scaling up replaced the existing warm pod instead of adding to it")
	}
}

// Scaling down must delete the surplus and stop there.
func TestPoolScalesDown(t *testing.T) {
	startManager(t)
	ns := newNamespace(t)
	newPool(t, ns, 3)

	eventually(t, "3 pods created", func() bool {
		return len(listPoolPods(t, ns, "default")) == 3
	})
	for _, p := range listPoolPods(t, ns, "default") {
		markReady(t, &p)
	}
	eventually(t, "3 warm", func() bool {
		return getPool(t, ns).Status.WarmReplicas == 3
	})

	scalePool(t, ns, 1)

	eventually(t, "1 live pod after scale down", func() bool {
		return len(livePoolPods(t, ns, "default")) == 1
	})
	eventually(t, "status reports 1 warm", func() bool {
		s := getPool(t, ns).Status
		return s.WarmReplicas == 1 && s.TotalPods == 1
	})

	// The surviving Pod must be left alone: a pool that deletes down to one and
	// then keeps churning is worse than one that never scaled down.
	consistently(t, "pod count stays at 1", func() bool {
		return len(livePoolPods(t, ns, "default")) == 1
	})
}

// A claimed Pod belongs to its claimant. Scale-down may never take it, even
// when it is the only Pod left and the pool is scaled to zero.
func TestScaleDownNeverDeletesClaimedPods(t *testing.T) {
	startManager(t)
	ns := newNamespace(t)
	newPool(t, ns, 2)

	eventually(t, "2 pods created", func() bool {
		return len(listPoolPods(t, ns, "default")) == 2
	})
	for _, p := range listPoolPods(t, ns, "default") {
		markReady(t, &p)
	}
	eventually(t, "2 warm", func() bool {
		return getPool(t, ns).Status.WarmReplicas == 2
	})

	claimed := listPoolPods(t, ns, "default")[0]
	claimPod(t, &claimed, "claim-a")

	// Claiming removes warm capacity, so the pool backfills: 1 warm + 1 claimed
	// + 1 warming.
	eventually(t, "pool backfills the claimed pod", func() bool {
		s := getPool(t, ns).Status
		return s.ClaimedReplicas == 1 && s.TotalPods == 3
	})

	scalePool(t, ns, 0)

	// Both unclaimed Pods go; the claimed one stays.
	eventually(t, "only the claimed pod remains", func() bool {
		live := livePoolPods(t, ns, "default")
		return len(live) == 1 && live[0].Name == claimed.Name
	})

	consistently(t, "claimed pod is never deleted", func() bool {
		pod := &corev1.Pod{}
		err := k8sClient.Get(context.Background(),
			client.ObjectKey{Namespace: ns, Name: claimed.Name}, pod)
		return err == nil && pod.DeletionTimestamp == nil
	})

	eventually(t, "status reports 0 warm, 1 claimed", func() bool {
		s := getPool(t, ns).Status
		return s.WarmReplicas == 0 && s.ClaimedReplicas == 1 && s.TotalPods == 1
	})
}

// warmReplicas 0 is a legitimate request and must not create anything.
func TestPoolWithZeroWarmReplicasCreatesNothing(t *testing.T) {
	startManager(t)
	ns := newNamespace(t)
	newPool(t, ns, 0)

	consistently(t, "no pods created", func() bool {
		return len(listPoolPods(t, ns, "default")) == 0
	})

	eventually(t, "status reports at capacity", func() bool {
		s := getPool(t, ns).Status
		cond := findCondition(s.Conditions, warmpoolv1alpha1.PoolConditionWarmCapacityReady)
		return s.TotalPods == 0 && cond != nil && cond.Status == metav1.ConditionTrue
	})
}

// Pods must carry the controller's labels and a controlling ownerReference:
// this is the durable state the controller rebuilds from after a restart.
func TestCreatedPodsAreLabelledAndOwned(t *testing.T) {
	startManager(t)
	ns := newNamespace(t)
	pool := newPool(t, ns, 1)

	eventually(t, "pod created", func() bool {
		return len(listPoolPods(t, ns, "default")) == 1
	})

	pod := listPoolPods(t, ns, "default")[0]

	if pod.Labels[warmpoolv1alpha1.LabelPool] != "default" {
		t.Errorf("pool label = %q", pod.Labels[warmpoolv1alpha1.LabelPool])
	}
	if pod.Labels[warmpoolv1alpha1.LabelState] != warmpoolv1alpha1.StateWarm {
		t.Errorf("state label = %q, want warm", pod.Labels[warmpoolv1alpha1.LabelState])
	}
	if pod.Labels["app"] != "warm-worker" {
		t.Error("template labels must be preserved on created pods")
	}
	if pod.Labels[warmpoolv1alpha1.LabelTemplateHash] == "" {
		t.Error("template hash label missing")
	}

	ref := metav1.GetControllerOf(&pod)
	if ref == nil {
		t.Fatal("pod has no controlling owner reference")
	}
	if ref.Kind != "WorkerPool" || ref.Name != "default" {
		t.Errorf("owner = %s/%s, want WorkerPool/default", ref.Kind, ref.Name)
	}
	// The owner UID ties membership to this pool instance, not just its name.
	if ref.UID != getPool(t, ns).UID {
		t.Error("owner reference UID does not match the pool")
	}
	_ = pool
}

// Membership is decided by the ownerReference. A Pod that merely carries the
// pool label -- hand-written, or left over from a deleted pool of the same
// name -- must not be counted, or the pool would under-provision.
func TestForeignPodWithPoolLabelIsIgnored(t *testing.T) {
	startManager(t)
	ns := newNamespace(t)

	impostor := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "impostor",
			Namespace: ns,
			Labels: map[string]string{
				warmpoolv1alpha1.LabelPool:  "default",
				warmpoolv1alpha1.LabelState: warmpoolv1alpha1.StateWarm,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "w", Image: "nginx:alpine"}},
		},
	}
	if err := k8sClient.Create(context.Background(), impostor); err != nil {
		t.Fatalf("create impostor: %v", err)
	}
	markReady(t, impostor)

	newPool(t, ns, 1)

	// The pool must create its own Pod and ignore the impostor entirely.
	eventually(t, "pool creates its own pod", func() bool {
		for _, p := range listPoolPods(t, ns, "default") {
			if metav1.GetControllerOf(&p) != nil {
				return true
			}
		}
		return false
	})

	eventually(t, "status counts only owned pods", func() bool {
		return getPool(t, ns).Status.TotalPods == 1
	})
}

// Membership is decided by the ownerReference, so stripping the pool label must
// not hide a running, owned Pod. If it did, the pool would under-count warm
// capacity, create an unnecessary replacement, and leak the hidden Pod until
// the pool was deleted.
func TestStrippingPoolLabelDoesNotHidePod(t *testing.T) {
	startManager(t)
	ns := newNamespace(t)
	newPool(t, ns, 3)

	eventually(t, "3 pods created", func() bool {
		return len(listPoolPods(t, ns, "default")) == 3
	})
	for _, p := range listPoolPods(t, ns, "default") {
		markReady(t, &p)
	}
	eventually(t, "3 warm", func() bool {
		return getPool(t, ns).Status.WarmReplicas == 3
	})

	victim := listPoolPods(t, ns, "default")[0]
	stripped := victim.DeepCopy()
	delete(stripped.Labels, warmpoolv1alpha1.LabelPool)
	if err := k8sClient.Patch(context.Background(), stripped, client.MergeFrom(&victim)); err != nil {
		t.Fatalf("strip label: %v", err)
	}

	// Total Pods in the namespace must stay at 3: no replacement for a Pod that
	// never actually left.
	consistently(t, "no replacement pod is created", func() bool {
		return len(allPods(t, ns)) == 3
	})

	// The pool controller does not repair routing labels. Owner references
	// still drive accounting; the separately installed admission policy can
	// prevent this edit, but is not enabled in this test.
	pod := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(),
		client.ObjectKey{Namespace: ns, Name: victim.Name}, pod); err != nil {
		t.Fatalf("get victim: %v", err)
	}
	if _, ok := pod.Labels[warmpoolv1alpha1.LabelPool]; ok {
		t.Error("controller rewrote the pool label; membership must come from the ownerReference alone")
	}

	if s := getPool(t, ns).Status; s.TotalPods != 3 {
		t.Errorf("totalPods = %d, want 3 (status must not drift from reality)", s.TotalPods)
	}
}

func findCondition(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}

// TestObservedGenerationReflectsTheSpecActedOn pins observedGeneration to the
// generation the reconcile pass actually read, not whatever the object happens
// to carry when status is written.
//
// The pass reads the spec once, decides from it, then re-reads the pool to get
// a current resourceVersion to write against. A spec edit landing in between
// makes that re-read carry a newer generation. Publishing it would claim the
// status is current for a spec the controller never acted on -- the precise
// staleness observedGeneration exists to expose.
//
// No manager runs here: updateStatus is called directly so the interleaving is
// deterministic rather than raced against a live reconciler.
func TestObservedGenerationReflectsTheSpecActedOn(t *testing.T) {
	ctx := context.Background()
	ns := newNamespace(t)

	pool := newPool(t, ns, 2)
	actedOn := pool.DeepCopy()
	if actedOn.Generation == 0 {
		t.Fatalf("expected a generation on the created pool")
	}

	// The spec edit a reconcile pass would race with.
	scalePool(t, ns, 10)

	latest := getPool(t, ns)
	if latest.Generation == actedOn.Generation {
		t.Fatalf("spec edit did not bump generation (still %d)", latest.Generation)
	}

	r := &WorkerPoolReconciler{Client: k8sClient, Scheme: scheme}
	if err := r.updateStatus(ctx, actedOn, poolCounts{warm: 2}, nil); err != nil {
		t.Fatalf("update status: %v", err)
	}

	got := getPool(t, ns)
	if got.Status.ObservedGeneration != actedOn.Generation {
		t.Errorf("observedGeneration = %d, want %d (the generation acted on, not %d)",
			got.Status.ObservedGeneration, actedOn.Generation, latest.Generation)
	}

	cond := findCondition(got.Status.Conditions, warmpoolv1alpha1.PoolConditionWarmCapacityReady)
	if cond == nil {
		t.Fatalf("condition %q not set", warmpoolv1alpha1.PoolConditionWarmCapacityReady)
	}
	if cond.ObservedGeneration != actedOn.Generation {
		t.Errorf("condition observedGeneration = %d, want %d",
			cond.ObservedGeneration, actedOn.Generation)
	}

	// The whole point of the field: a reader can tell this status is stale.
	if got.Status.ObservedGeneration == got.Generation {
		t.Error("status claims to be current for a spec the pass never read")
	}
}
