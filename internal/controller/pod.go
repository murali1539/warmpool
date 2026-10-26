package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	warmpoolv1alpha1 "github.com/linkedin/warmpool/api/v1alpha1"
)

// podClass is how the controller buckets a Pod when measuring warm capacity.
//
// Exactly one class applies to a Pod, and only Warm counts toward
// spec.warmReplicas. Everything else is explicitly excluded: terminating Pods
// are leaving, terminal Pods are dead, claimed Pods are spoken for, and warming
// Pods are not usable yet.
type podClass int

const (
	// podTerminating has a deletionTimestamp. It must not count as supply --
	// counting it would suppress the replacement the pool needs to create.
	podTerminating podClass = iota

	// podTerminal is Failed or Succeeded. Never usable again.
	podTerminal

	// podClaimed is assigned to a WorkerClaim. It is still owned by the pool
	// (the pool must not delete it when scaling down, and must replace the warm
	// capacity it removed) but it is not warm.
	podClaimed

	// podWarm is Ready, unclaimed and not terminating: the real thing a claim
	// can be handed.
	podWarm

	// podWarming is unclaimed and not ready, including a Pod that lost readiness.
	//
	// This counts as in-flight supply. Readiness is asynchronous and a
	// reconciler may run many times while a Pod boots, so without counting
	// these the controller would create a fresh Pod on every pass and overshoot
	// badly.
	podWarming
)

func (c podClass) String() string {
	switch c {
	case podTerminating:
		return "terminating"
	case podTerminal:
		return "terminal"
	case podClaimed:
		return "claimed"
	case podWarm:
		return "warm"
	case podWarming:
		return "warming"
	}
	return "unknown"
}

// classifyPod buckets a Pod. Order matters: a terminating Pod is terminating
// even if it is still Ready, and a dead Pod is dead even if it was claimed.
func classifyPod(pod *corev1.Pod) podClass {
	if pod.DeletionTimestamp != nil {
		return podTerminating
	}
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return podTerminal
	}
	if pod.Labels[warmpoolv1alpha1.LabelClaim] != "" {
		return podClaimed
	}
	if isPodReady(pod) {
		return podWarm
	}
	return podWarming
}

// isPodReady reports whether a Pod is Running with a true Ready condition.
//
// The phase check matters: a Pod can retain a stale Ready=True condition while
// its phase has moved on, and readiness alone would then hand out an unusable
// Pod.
func isPodReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// poolCounts is the observed breakdown of a pool's Pods.
//
// Only the buckets that drive a decision are tracked. Terminating and terminal
// Pods are classified but not counted: nothing reads them, and a counter no
// code consumes is drift waiting to happen.
type poolCounts struct {
	warm    int32
	warming int32
	claimed int32
}

// total counts active pool Pods, excluding terminal and terminating objects.
func (c poolCounts) total() int32 { return c.warm + c.warming + c.claimed }

// supply is warm capacity plus warm capacity already on the way. This is what
// spec.warmReplicas is compared against when deciding how many Pods to create;
// using warm alone would overshoot while Pods are still becoming Ready.
func (c poolCounts) supply() int32 { return c.warm + c.warming }

func countPods(pods []corev1.Pod) poolCounts {
	var c poolCounts
	for i := range pods {
		switch classifyPod(&pods[i]) {
		case podWarm:
			c.warm++
		case podWarming:
			c.warming++
		case podClaimed:
			c.claimed++
		case podTerminating, podTerminal:
			// Neither contributes active capacity or in-flight supply.
		}
	}
	return c
}

// templateHash fingerprints the template content used to create a Pod.
func templateHash(tpl *warmpoolv1alpha1.WorkerPodTemplate) string {
	encoded, err := json.Marshal(tpl)
	if err != nil {
		// A PodTemplate that cannot be marshalled cannot have come from the API
		// server; fall back to a constant rather than failing the reconcile.
		return "unencodable"
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])[:10]
}

// buildPod renders a warm Pod from the pool template.
//
// The controller injects only its own labels, the template hash, a generateName
// and (via the caller) an ownerReference. User containers are copied verbatim.
func buildPod(pool *warmpoolv1alpha1.WorkerPool) *corev1.Pod {
	labels := make(map[string]string, len(pool.Spec.Template.Metadata.Labels)+3)
	for k, v := range pool.Spec.Template.Metadata.Labels {
		labels[k] = v
	}
	// Written last so user labels can never override controller state. The API
	// server also rejects reserved keys in the template, so this is belt and
	// braces.
	labels[warmpoolv1alpha1.LabelPool] = pool.Name
	labels[warmpoolv1alpha1.LabelState] = warmpoolv1alpha1.StateWarm
	labels[warmpoolv1alpha1.LabelTemplateHash] = templateHash(&pool.Spec.Template)

	var annotations map[string]string
	if len(pool.Spec.Template.Metadata.Annotations) > 0 {
		annotations = make(map[string]string, len(pool.Spec.Template.Metadata.Annotations))
		for k, v := range pool.Spec.Template.Metadata.Annotations {
			annotations[k] = v
		}
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			// generateName, not a deterministic name: the controller may create
			// several Pods in one pass, and a name collision with a Pod still
			// terminating would fail the create.
			GenerateName: pool.Name + "-",
			Namespace:    pool.Namespace,
			Labels:       labels,
			Annotations:  annotations,
		},
		Spec: *pool.Spec.Template.Spec.DeepCopy(),
	}
}
