package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClaimPolicy describes what happens to a Pod when the WorkerClaim holding it
// is deleted. It makes the ownership/release contract explicit.
//
// Only Delete is supported. A Pod that has been claimed is never returned to
// the warm set: the controller cannot observe whether the claimant actually
// used the Pod, so it cannot establish that the Pod is free of the previous
// tenant's residue. Reuse would risk cross-tenant contamination.
// +kubebuilder:validation:Enum=Delete
type ClaimPolicy string

const (
	// ClaimPolicyDelete deletes a claim's worker rather than returning it to
	// the warm set. The pool replenishes warm capacity when the worker is claimed.
	ClaimPolicyDelete ClaimPolicy = "Delete"
)

// WorkerPodTemplateMetadata is the subset of ObjectMeta a user may set on warm
// Pods.
//
// Explicit fields provide a structural schema and allow validation of reserved
// label and annotation prefixes.
type WorkerPodTemplateMetadata struct {
	// Labels to apply to warm Pods.
	//
	// Keys under warmpool.linkedin.com/ are reserved for controller metadata.
	// This validation applies to the template, not to direct Pod writes.
	//
	// +kubebuilder:validation:XValidation:rule="self.all(k, !k.startsWith('warmpool.linkedin.com/'))",message="labels under the warmpool.linkedin.com/ prefix are reserved by the controller"
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations to apply to warm Pods.
	//
	// +kubebuilder:validation:XValidation:rule="self.all(k, !k.startsWith('warmpool.linkedin.com/'))",message="annotations under the warmpool.linkedin.com/ prefix are reserved by the controller"
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// WorkerPodTemplate describes the Pods the pool creates.
type WorkerPodTemplate struct {
	// +optional
	Metadata WorkerPodTemplateMetadata `json:"metadata,omitempty"`

	// Spec is the PodSpec used verbatim for warm Pods. The controller never
	// rewrites user containers.
	//
	// +kubebuilder:validation:Required
	Spec corev1.PodSpec `json:"spec"`
}

// WorkerPoolSpec describes the desired warm capacity of a pool.
type WorkerPoolSpec struct {
	// WarmReplicas is the desired number of ready, unclaimed Pods.
	//
	// Compare this target with status.warmReplicas; readiness and reconciliation
	// are asynchronous. Claimed Pods do not count toward it. Not-ready, unclaimed
	// Pods count as in-flight supply to avoid repeatedly creating replacements,
	// but are not reported as warm.
	//
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	// +optional
	WarmReplicas *int32 `json:"warmReplicas,omitempty"`

	// Template is the Pod template used to create warm workers.
	//
	// The controller only injects its own labels, a template hash, a
	// generateName and an ownerReference; user containers are never rewritten.
	// Keys under the warmpool.linkedin.com/ prefix are reserved and rejected in
	// template.metadata, so a user cannot forge warm or claimed state.
	//
	// +kubebuilder:validation:Required
	Template WorkerPodTemplate `json:"template"`

	// ClaimPolicy decides the fate of a Pod when its WorkerClaim is deleted.
	// Delete is the only supported value; the field exists to make the release
	// contract explicit rather than implicit.
	//
	// +kubebuilder:default=Delete
	// +optional
	ClaimPolicy ClaimPolicy `json:"claimPolicy,omitempty"`
}

// WorkerPoolStatus is the observed state of the pool.
//
// Terminating, Failed, and Succeeded Pods are excluded from all counters.
type WorkerPoolStatus struct {
	// WarmReplicas is the observed number of Ready, unclaimed, non-terminating
	// Pods. This is the field that corresponds to spec.warmReplicas.
	// +optional
	WarmReplicas int32 `json:"warmReplicas"`

	// ClaimedReplicas counts non-terminal, non-terminating Pods with a non-empty
	// claim label, regardless of readiness. They remain owned by the pool but
	// are excluded from scale-down and warm capacity.
	// +optional
	ClaimedReplicas int32 `json:"claimedReplicas"`

	// WarmingReplicas counts unclaimed, non-terminal, non-terminating Pods that
	// are not ready. This includes Pods that lost readiness. They count as
	// in-flight supply and have no warmup timeout.
	// +optional
	WarmingReplicas int32 `json:"warmingReplicas"`

	// TotalPods is warmReplicas + claimedReplicas + warmingReplicas. It excludes
	// terminal and terminating Pods even while their API objects still exist.
	// +optional
	TotalPods int32 `json:"totalPods"`

	// ObservedGeneration is the spec generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the pool's current state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// WorkerPool condition types.
const (
	// PoolConditionWarmCapacityReady is True when warm capacity meets the target
	// and the current reconcile has no Pod create/delete failure.
	PoolConditionWarmCapacityReady = "WarmCapacityReady"

	// PoolConditionTemplateHealthy is reserved; the controller does not
	// currently report template-health conditions.
	PoolConditionTemplateHealthy = "TemplateHealthy"
)

// WorkerPool condition reasons.
const (
	PoolReasonAtCapacity       = "AtCapacity"
	PoolReasonScalingUp        = "ScalingUp"
	PoolReasonScalingDown      = "ScalingDown"
	PoolReasonWarming          = "Warming"
	PoolReasonPodCreateFailed  = "PodCreateFailed"
	PoolReasonPodDeleteFailed  = "PodDeleteFailed"
	PoolReasonTemplateAccepted = "TemplateAccepted"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=wp,categories=warmpool
// +kubebuilder:printcolumn:name="Desired-Warm",type=integer,JSONPath=`.spec.warmReplicas`
// +kubebuilder:printcolumn:name="Warm",type=integer,JSONPath=`.status.warmReplicas`
// +kubebuilder:printcolumn:name="Warming",type=integer,JSONPath=`.status.warmingReplicas`
// +kubebuilder:printcolumn:name="Claimed",type=integer,JSONPath=`.status.claimedReplicas`
// +kubebuilder:printcolumn:name="Total",type=integer,JSONPath=`.status.totalPods`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// WorkerPool maintains a pool of ready, unclaimed Pods that can be handed out
// to WorkerClaims without paying Pod startup latency.
type WorkerPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WorkerPoolSpec   `json:"spec,omitempty"`
	Status WorkerPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WorkerPoolList contains a list of WorkerPool.
type WorkerPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkerPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WorkerPool{}, &WorkerPoolList{})
}

// DesiredWarmReplicas returns spec.warmReplicas, defaulted when unset.
func (p *WorkerPool) DesiredWarmReplicas() int32 {
	if p.Spec.WarmReplicas == nil {
		return 1
	}
	return *p.Spec.WarmReplicas
}

// EffectiveClaimPolicy returns spec.claimPolicy, defaulted when unset.
func (p *WorkerPool) EffectiveClaimPolicy() ClaimPolicy {
	if p.Spec.ClaimPolicy == "" {
		return ClaimPolicyDelete
	}
	return p.Spec.ClaimPolicy
}
