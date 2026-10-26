package controller

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	warmpoolv1alpha1 "github.com/linkedin/warmpool/api/v1alpha1"
)

type podOpt func(*corev1.Pod)

func testPod(opts ...podOpt) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
			Labels:    map[string]string{warmpoolv1alpha1.LabelPool: "default"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, o := range opts {
		o(pod)
	}
	return pod
}

func ready() podOpt {
	return func(p *corev1.Pod) {
		p.Status.Phase = corev1.PodRunning
		p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		})
	}
}

func notReady() podOpt {
	return func(p *corev1.Pod) {
		p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionFalse,
		})
	}
}

func phase(ph corev1.PodPhase) podOpt {
	return func(p *corev1.Pod) { p.Status.Phase = ph }
}

func terminating() podOpt {
	return func(p *corev1.Pod) {
		now := metav1.NewTime(time.Now())
		p.DeletionTimestamp = &now
		p.Finalizers = []string{"test"}
	}
}

func claimedBy(claim string) podOpt {
	return func(p *corev1.Pod) { p.Labels[warmpoolv1alpha1.LabelClaim] = claim }
}

// Requirement 4: terminating, failed, succeeded and not-ready Pods must never
// count as warm capacity.
func TestClassifyPod(t *testing.T) {
	tests := []struct {
		name string
		pod  *corev1.Pod
		want podClass
	}{
		{"running and ready is warm", testPod(ready()), podWarm},
		{"running but not ready is warming", testPod(notReady()), podWarming},
		{"no ready condition yet is warming", testPod(), podWarming},
		{"pending is warming", testPod(phase(corev1.PodPending)), podWarming},
		{"failed is terminal", testPod(phase(corev1.PodFailed)), podTerminal},
		{"succeeded is terminal", testPod(phase(corev1.PodSucceeded)), podTerminal},
		{"claimed is not warm", testPod(ready(), claimedBy("c1")), podClaimed},
		{"terminating is not warm", testPod(ready(), terminating()), podTerminating},

		// Ordering cases: the reason classification is a priority chain rather
		// than independent predicates.
		{
			"terminating outranks ready",
			testPod(ready(), terminating()),
			podTerminating,
		},
		{
			"terminating outranks claimed",
			testPod(ready(), claimedBy("c1"), terminating()),
			podTerminating,
		},
		{
			"failed outranks claimed",
			testPod(phase(corev1.PodFailed), claimedBy("c1")),
			podTerminal,
		},
		{
			// A Pod can retain a stale Ready=True condition after its phase has
			// moved on. Readiness alone would hand out an unusable Pod.
			"stale ready condition with non-running phase is not warm",
			testPod(phase(corev1.PodPending), ready(), phase(corev1.PodPending)),
			podWarming,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyPod(tc.pod); got != tc.want {
				t.Errorf("classifyPod() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCountPods(t *testing.T) {
	pods := []corev1.Pod{
		*testPod(ready()),
		*testPod(ready()),
		*testPod(notReady()),
		*testPod(phase(corev1.PodFailed)),
		*testPod(phase(corev1.PodSucceeded)),
		*testPod(ready(), claimedBy("c1")),
		*testPod(ready(), terminating()),
	}

	got := countPods(pods)
	want := poolCounts{warm: 2, warming: 1, claimed: 1}
	if got != want {
		t.Fatalf("countPods() = %+v, want %+v", got, want)
	}

	// Seven Pods in, four counted: the terminating Pod (which is Ready, so it
	// would otherwise look warm) and the two terminal ones are dropped rather
	// than banked into a bucket.
	if got.total() != 4 {
		t.Errorf("total() = %d, want 4 (warm 2 + warming 1 + claimed 1) from %d pods",
			got.total(), len(pods))
	}
	// supply drives Pod creation: warm plus in-flight warm.
	if got.supply() != 3 {
		t.Errorf("supply() = %d, want 3 (warm 2 + warming 1)", got.supply())
	}
}

// The overshoot guard, stated as arithmetic: while Pods are booting, the
// controller must compute a zero deficit rather than creating more.
//
// These cases build poolCounts directly, so they only exercise the arithmetic.
// That terminating and terminal Pods never reach a supply bucket in the first
// place is enforced by classifyPod and asserted in TestCountPods.
func TestDeficitAccountsForWarmingPods(t *testing.T) {
	tests := []struct {
		name    string
		desired int32
		counts  poolCounts
		want    int32
	}{
		{"empty pool", 3, poolCounts{}, 3},
		{"all warming", 3, poolCounts{warming: 3}, 0},
		{"partially warm", 3, poolCounts{warm: 1, warming: 1}, 1},
		{"at capacity", 3, poolCounts{warm: 3}, 0},
		{"claimed pods do not count as warm supply", 3, poolCounts{warm: 3, claimed: 2}, 0},
		{"claimed pod removed warm capacity", 3, poolCounts{warm: 2, claimed: 1}, 1},
		{"zero desired", 0, poolCounts{}, 0},
		{"surplus is negative, never creates", 1, poolCounts{warm: 3}, -2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.desired - tc.counts.supply(); got != tc.want {
				t.Errorf("deficit = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestBuildPod(t *testing.T) {
	pool := &warmpoolv1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "ns"},
		Spec: warmpoolv1alpha1.WorkerPoolSpec{
			Template: warmpoolv1alpha1.WorkerPodTemplate{
				Metadata: warmpoolv1alpha1.WorkerPodTemplateMetadata{
					Labels:      map[string]string{"app": "worker"},
					Annotations: map[string]string{"team": "infra"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "worker", Image: "nginx:alpine"}},
				},
			},
		},
	}

	pod := buildPod(pool)

	if pod.GenerateName != "default-" {
		t.Errorf("GenerateName = %q, want %q", pod.GenerateName, "default-")
	}
	if pod.Namespace != "ns" {
		t.Errorf("Namespace = %q, want ns", pod.Namespace)
	}
	if pod.Labels["app"] != "worker" {
		t.Error("user labels must be preserved")
	}
	if pod.Annotations["team"] != "infra" {
		t.Error("user annotations must be preserved")
	}
	if pod.Labels[warmpoolv1alpha1.LabelPool] != "default" {
		t.Error("pool label must be set")
	}
	if pod.Labels[warmpoolv1alpha1.LabelState] != warmpoolv1alpha1.StateWarm {
		t.Error("new pods must start in the warm state")
	}
	if pod.Labels[warmpoolv1alpha1.LabelClaim] != "" {
		t.Error("new pods must not be claimed")
	}
	if pod.Labels[warmpoolv1alpha1.LabelTemplateHash] == "" {
		t.Error("template hash must be set")
	}
	if len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Image != "nginx:alpine" {
		t.Error("user containers must be copied verbatim")
	}
}

// buildPod must not alias the pool's template: mutating a built Pod must never
// corrupt the cached WorkerPool object shared across reconciles.
func TestBuildPodDoesNotAliasTemplate(t *testing.T) {
	pool := &warmpoolv1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "ns"},
		Spec: warmpoolv1alpha1.WorkerPoolSpec{
			Template: warmpoolv1alpha1.WorkerPodTemplate{
				Metadata: warmpoolv1alpha1.WorkerPodTemplateMetadata{
					Labels: map[string]string{"app": "worker"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "worker", Image: "nginx:alpine"}},
				},
			},
		},
	}

	pod := buildPod(pool)
	pod.Spec.Containers[0].Image = "mutated"
	pod.Labels["app"] = "mutated"

	if pool.Spec.Template.Spec.Containers[0].Image != "nginx:alpine" {
		t.Error("buildPod aliased the template PodSpec")
	}
	if pool.Spec.Template.Metadata.Labels["app"] != "worker" {
		t.Error("buildPod aliased the template labels")
	}
}

func TestTemplateHashChangesWithTemplate(t *testing.T) {
	a := warmpoolv1alpha1.WorkerPodTemplate{
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "w", Image: "nginx:alpine"}}},
	}
	b := warmpoolv1alpha1.WorkerPodTemplate{
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "w", Image: "nginx:1.27"}}},
	}

	if templateHash(&a) == templateHash(&b) {
		t.Error("different templates must hash differently")
	}
	if templateHash(&a) != templateHash(a.DeepCopy()) {
		t.Error("hash must be stable for equal templates")
	}
}

func named(name string) podOpt {
	return func(p *corev1.Pod) { p.Name = name }
}

func createdAt(sec int) podOpt {
	return func(p *corev1.Pod) {
		p.CreationTimestamp = metav1.NewTime(time.Unix(int64(sec), 0))
	}
}

// Scale-down prefers currently not-ready Pods, then newer Pods within each
// readiness class. Claimed, terminating, and terminal Pods are excluded.
func TestSurplusCandidatesOrder(t *testing.T) {
	pods := []corev1.Pod{
		*testPod(named("warm-old"), createdAt(100), ready()),
		*testPod(named("claimed"), createdAt(100), ready(), claimedBy("c1")),
		*testPod(named("warming-old"), createdAt(100), notReady()),
		*testPod(named("warm-new"), createdAt(300), ready()),
		*testPod(named("terminating"), createdAt(100), ready(), terminating()),
		*testPod(named("warming-new"), createdAt(300), notReady()),
		*testPod(named("failed"), createdAt(100), phase(corev1.PodFailed)),
	}

	var got []string
	for _, p := range surplusCandidates(pods) {
		got = append(got, p.Name)
	}

	want := []string{"warming-new", "warming-old", "warm-new", "warm-old"}
	if len(got) != len(want) {
		t.Fatalf("candidates = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidates = %v, want %v", got, want)
		}
	}
}

// creationTimestamp has one-second granularity and a pool routinely creates
// several Pods inside one second, so ordering must not depend on the order the
// lister happened to return.
func TestSurplusCandidatesIsDeterministicOnEqualTimestamps(t *testing.T) {
	forward := []corev1.Pod{
		*testPod(named("a"), createdAt(100), ready()),
		*testPod(named("b"), createdAt(100), ready()),
		*testPod(named("c"), createdAt(100), ready()),
	}
	reversed := []corev1.Pod{forward[2], forward[1], forward[0]}

	order := func(pods []corev1.Pod) []string {
		var names []string
		for _, p := range surplusCandidates(pods) {
			names = append(names, p.Name)
		}
		return names
	}

	a, b := order(forward), order(reversed)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("order depends on input order: %v vs %v", a, b)
		}
	}
	if a[0] != "c" {
		t.Errorf("first candidate = %q, want %q (name breaks the tie, newest-first)", a[0], "c")
	}
}
