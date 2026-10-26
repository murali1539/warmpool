package v1alpha1_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	warmpoolv1alpha1 "github.com/linkedin/warmpool/api/v1alpha1"
)

var (
	testEnv *envtest.Environment
	cfg     *rest.Config
	scheme  = runtime.NewScheme()
)

func TestMain(m *testing.M) {
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd")},
		ErrorIfCRDPathMissing: true,
	}

	var err error
	cfg, err = testEnv.Start()
	if err != nil {
		panic(err)
	}

	if err := warmpoolv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}

	code := m.Run()
	_ = testEnv.Stop()
	os.Exit(code)
}

func newClient(t *testing.T) client.Client {
	t.Helper()
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return c
}

func minimalPool(name string) *warmpoolv1alpha1.WorkerPool {
	return &warmpoolv1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: warmpoolv1alpha1.WorkerPoolSpec{
			Template: warmpoolv1alpha1.WorkerPodTemplate{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "worker", Image: "nginx:alpine"}},
				},
			},
		},
	}
}

func ptrInt32(v int32) *int32 { return &v }

// The API server, not the controller, must supply these defaults. A user who
// omits them should still get a usable, explicitly-documented pool.
func TestWorkerPoolDefaulting(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	pool := minimalPool("defaults")
	if err := c.Create(ctx, pool); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(ctx, pool) })

	got := &warmpoolv1alpha1.WorkerPool{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pool), got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Spec.WarmReplicas == nil || *got.Spec.WarmReplicas != 1 {
		t.Errorf("warmReplicas default = %v, want 1", got.Spec.WarmReplicas)
	}
	if got.Spec.ClaimPolicy != warmpoolv1alpha1.ClaimPolicyDelete {
		t.Errorf("claimPolicy default = %q, want %q", got.Spec.ClaimPolicy, warmpoolv1alpha1.ClaimPolicyDelete)
	}
	if got.DesiredWarmReplicas() != 1 {
		t.Errorf("DesiredWarmReplicas() = %d, want 1", got.DesiredWarmReplicas())
	}
	if got.EffectiveClaimPolicy() != warmpoolv1alpha1.ClaimPolicyDelete {
		t.Errorf("EffectiveClaimPolicy() = %q, want Delete", got.EffectiveClaimPolicy())
	}
}

// warmReplicas: 0 is a legitimate request ("keep nothing warm"), and must be
// distinguishable from "unset", which defaults to 1. This is why the field is a
// pointer rather than a plain int32.
func TestWorkerPoolZeroWarmReplicasIsNotDefaulted(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	pool := minimalPool("zero-warm")
	pool.Spec.WarmReplicas = ptrInt32(0)
	if err := c.Create(ctx, pool); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(ctx, pool) })

	got := &warmpoolv1alpha1.WorkerPool{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pool), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Spec.WarmReplicas == nil || *got.Spec.WarmReplicas != 0 {
		t.Fatalf("warmReplicas = %v, want explicit 0 (not defaulted to 1)", got.Spec.WarmReplicas)
	}
}

func TestWorkerPoolRejectsInvalidSpec(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	t.Run("negative warmReplicas", func(t *testing.T) {
		pool := minimalPool("negative-warm")
		pool.Spec.WarmReplicas = ptrInt32(-1)
		err := c.Create(ctx, pool)
		if err == nil {
			_ = c.Delete(ctx, pool)
			t.Fatal("expected rejection of negative warmReplicas")
		}
		if !apierrors.IsInvalid(err) {
			t.Fatalf("expected Invalid error, got %v", err)
		}
	})

	t.Run("unknown claimPolicy", func(t *testing.T) {
		pool := minimalPool("bad-policy")
		pool.Spec.ClaimPolicy = warmpoolv1alpha1.ClaimPolicy("Hoard")
		err := c.Create(ctx, pool)
		if err == nil {
			_ = c.Delete(ctx, pool)
			t.Fatal("expected rejection of unknown claimPolicy")
		}
		if !apierrors.IsInvalid(err) {
			t.Fatalf("expected Invalid error, got %v", err)
		}
	})

	t.Run("missing template", func(t *testing.T) {
		pool := &warmpoolv1alpha1.WorkerPool{
			ObjectMeta: metav1.ObjectMeta{Name: "no-template", Namespace: "default"},
		}
		err := c.Create(ctx, pool)
		if err == nil {
			_ = c.Delete(ctx, pool)
			t.Fatal("expected rejection of pool without a template")
		}
	})

	// Templates must not supply reserved controller labels or binding annotations.
	t.Run("reserved label prefix", func(t *testing.T) {
		pool := minimalPool("reserved-label")
		pool.Spec.Template.Metadata.Labels = map[string]string{
			"warmpool.linkedin.com/state": "warm",
		}
		err := c.Create(ctx, pool)
		if err == nil {
			_ = c.Delete(ctx, pool)
			t.Fatal("expected rejection of reserved label prefix")
		}
		if !apierrors.IsInvalid(err) {
			t.Fatalf("expected Invalid error, got %v", err)
		}
	})

	t.Run("reserved annotation prefix", func(t *testing.T) {
		pool := minimalPool("reserved-annotation")
		pool.Spec.Template.Metadata.Annotations = map[string]string{
			"warmpool.linkedin.com/claim-uid": "forged",
		}
		err := c.Create(ctx, pool)
		if err == nil {
			_ = c.Delete(ctx, pool)
			t.Fatal("expected rejection of reserved annotation prefix")
		}
		if !apierrors.IsInvalid(err) {
			t.Fatalf("expected Invalid error, got %v", err)
		}
	})

	// Ordinary user labels must still work -- the rule is a prefix guard, not a
	// ban on template metadata.
	t.Run("ordinary labels accepted", func(t *testing.T) {
		pool := minimalPool("ok-labels")
		pool.Spec.Template.Metadata.Labels = map[string]string{"app": "warm-worker"}
		if err := c.Create(ctx, pool); err != nil {
			t.Fatalf("ordinary template labels rejected: %v", err)
		}
		t.Cleanup(func() { _ = c.Delete(ctx, pool) })
	})
}

// Status must be a subresource: ordinary updates must not overwrite controller
// status, and status updates must preserve spec.
func TestWorkerPoolStatusIsSubresource(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	pool := minimalPool("status-sub")
	pool.Spec.WarmReplicas = ptrInt32(3)
	if err := c.Create(ctx, pool); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(ctx, pool) })

	// A spec-path update carrying status must leave status untouched.
	pool.Status.WarmReplicas = 99
	if err := c.Update(ctx, pool); err != nil {
		t.Fatalf("update: %v", err)
	}
	got := &warmpoolv1alpha1.WorkerPool{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pool), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.WarmReplicas != 0 {
		t.Errorf("status written through the main resource: warmReplicas = %d, want 0", got.Status.WarmReplicas)
	}

	// The status subresource accepts the full counter set.
	got.Status = warmpoolv1alpha1.WorkerPoolStatus{
		WarmReplicas:       3,
		ClaimedReplicas:    1,
		WarmingReplicas:    2,
		TotalPods:          6,
		ObservedGeneration: got.Generation,
		Conditions: []metav1.Condition{{
			Type:               warmpoolv1alpha1.PoolConditionWarmCapacityReady,
			Status:             metav1.ConditionTrue,
			Reason:             warmpoolv1alpha1.PoolReasonAtCapacity,
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: got.Generation,
		}},
	}
	if err := c.Status().Update(ctx, got); err != nil {
		t.Fatalf("status update: %v", err)
	}

	final := &warmpoolv1alpha1.WorkerPool{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pool), final); err != nil {
		t.Fatalf("get: %v", err)
	}
	if final.Status.WarmReplicas != 3 || final.Status.ClaimedReplicas != 1 ||
		final.Status.WarmingReplicas != 2 || final.Status.TotalPods != 6 {
		t.Errorf("status round-trip mismatch: %+v", final.Status)
	}
	if len(final.Status.Conditions) != 1 {
		t.Fatalf("conditions = %d, want 1", len(final.Status.Conditions))
	}
	if final.Spec.WarmReplicas == nil || *final.Spec.WarmReplicas != 3 {
		t.Errorf("status update clobbered spec: %v", final.Spec.WarmReplicas)
	}
}

// The shipped sample must stay valid against the generated CRD. This catches
// the common drift where a field is renamed in the types but not in the docs
// users actually copy-paste.
func TestSampleManifestIsValid(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "samples", "workerpool.yaml"))
	if err != nil {
		t.Fatalf("read sample: %v", err)
	}

	pool := &warmpoolv1alpha1.WorkerPool{}
	if err := yaml.UnmarshalStrict(raw, pool); err != nil {
		t.Fatalf("sample does not match the Go types: %v", err)
	}
	pool.Namespace = "default" // the sample's namespace does not exist in envtest

	if err := c.Create(ctx, pool); err != nil {
		t.Fatalf("API server rejected the sample: %v", err)
	}
	t.Cleanup(func() { _ = c.Delete(ctx, pool) })

	if pool.DesiredWarmReplicas() != 3 {
		t.Errorf("sample warmReplicas = %d, want 3 (README and tests assume 3)", pool.DesiredWarmReplicas())
	}
}

// Document the accounting identity for the active Pod counters.
func TestStatusAccountingIdentity(t *testing.T) {
	s := warmpoolv1alpha1.WorkerPoolStatus{
		WarmReplicas:    3,
		ClaimedReplicas: 2,
		WarmingReplicas: 1,
		TotalPods:       6,
	}
	if got := s.WarmReplicas + s.ClaimedReplicas + s.WarmingReplicas; got != s.TotalPods {
		t.Errorf("warm+claimed+warming = %d, totalPods = %d", got, s.TotalPods)
	}
}
