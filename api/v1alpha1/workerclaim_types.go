package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClaimPhase is the coarse lifecycle state of a WorkerClaim.
//
// The claim uses a phase rather than conditions because, unlike a pool, it has
// a single linear lifecycle with mutually exclusive states. Reason and Message
// carry the detail a condition would otherwise hold.
// +kubebuilder:validation:Enum=Pending;Claimed;Failed;Succeeded
type ClaimPhase string

const (
	// ClaimPhasePending means no assignment has been published. The claim may
	// be waiting for a usable pool or ready worker, or have an unpublished binding.
	ClaimPhasePending ClaimPhase = "Pending"

	// ClaimPhaseClaimed records a published Pod binding. It does not guarantee
	// continued readiness; Pod loss or completion is reflected asynchronously.
	ClaimPhaseClaimed ClaimPhase = "Claimed"

	// ClaimPhaseFailed is terminal. The claim will not be assigned another Pod;
	// obtaining a new worker means creating a new claim.
	ClaimPhaseFailed ClaimPhase = "Failed"

	// ClaimPhaseSucceeded is terminal: the assigned Pod completed successfully.
	ClaimPhaseSucceeded ClaimPhase = "Succeeded"
)

// Reasons reported in WorkerClaim.status.reason.
const (
	// ClaimReasonWaitingForWarmPod means the pool currently has no warm Pod.
	// This is not an error: the claim is woken when one becomes available.
	ClaimReasonWaitingForWarmPod = "WaitingForWarmPod"

	// ClaimReasonAssigned means a Pod was bound to this claim.
	ClaimReasonAssigned = "Assigned"

	// ClaimReasonPoolNotFound means spec.poolName does not resolve to a
	// WorkerPool in this namespace.
	ClaimReasonPoolNotFound = "PoolNotFound"

	// ClaimReasonPoolTerminating means new allocation is waiting for a pool
	// that is not being deleted.
	ClaimReasonPoolTerminating = "PoolTerminating"

	// ClaimReasonPodLost means the bound Pod failed or is deleting, or a
	// published binding is missing or mismatched. An unpublished binding can
	// also fail directly; the claim is never silently assigned a replacement.
	ClaimReasonPodLost = "PodLost"

	// ClaimReasonPodCompleted means the bound Pod reached Succeeded, even if
	// that completion was observed before the assignment was published.
	ClaimReasonPodCompleted = "PodCompleted"
)

// WorkerClaimSpec is a request for one warm Pod from a pool.
type WorkerClaimSpec struct {
	// PoolName is the WorkerPool to claim from, in this claim's namespace.
	//
	// Immutable: the claim's identity is "a worker from this pool". Allowing it
	// to change would let a claim that already holds a Pod from pool A start
	// pointing at pool B, leaving the Pod bound to a claim whose spec no longer
	// explains why it exists.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.poolName is immutable"
	PoolName string `json:"poolName"`
}

// WorkerClaimStatus is the observed state of the claim.
//
// There is no observedGeneration: the spec is a single immutable field, so the
// generation can never advance past the one the controller acted on.
type WorkerClaimStatus struct {
	// Phase is the claim's lifecycle state.
	//
	// +optional
	Phase ClaimPhase `json:"phase,omitempty"`

	// PodName identifies the assigned or last bound Pod and is retained on
	// terminal claims, even after the Pod is gone.
	//
	// Binding metadata is written on the Pod before status publication, so an
	// interrupted publication can be recovered from that Pod.
	//
	// +optional
	PodName string `json:"podName,omitempty"`

	// Reason is a short, machine-readable explanation of the current phase.
	//
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message is a human-readable elaboration of Reason.
	//
	// +optional
	Message string `json:"message,omitempty"`

	// AssignedAt records when Claimed status was first published. It is retained
	// on terminal claims, but absent when a recovered binding goes directly to
	// Failed or Succeeded without publishing Claimed.
	//
	// +optional
	AssignedAt *metav1.Time `json:"assignedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=wc,categories=warmpool
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.poolName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Pod",type=string,JSONPath=`.status.podName`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.reason`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// WorkerClaim requests exclusive use of one warm Pod from a WorkerPool.
//
// The binding is one-to-one in both directions: a claim holds at most one Pod,
// and a Pod carries at most one claim label. Deleting the claim deletes its
// Pod -- a used worker is never returned to the warm pool.
type WorkerClaim struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WorkerClaimSpec   `json:"spec,omitempty"`
	Status WorkerClaimStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WorkerClaimList contains a list of WorkerClaim.
type WorkerClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkerClaim `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WorkerClaim{}, &WorkerClaimList{})
}

// ClaimFinalizer keeps a claim alive until deletion has been requested for its
// bound Pods. It does not wait for Pod termination to finish.
//
// Pods are owned by the WorkerPool, so deleting a claim does not itself
// garbage-collect them.
const ClaimFinalizer = ReservedPrefix + "claim-protection"
