package v1alpha1

// Labels and annotations the controller writes on Pods it owns.
//
// These are the durable record of warm-pool state. The controller may keep
// in-memory caches as an optimization, but everything here must be
// reconstructable from the API server alone after a restart, so assignment
// state is written to Pod objects rather than held in process memory.
//
// The CRD rejects ReservedPrefix keys in spec.template.metadata. Direct Pod
// label writes are restricted separately by the supplied admission policy,
// when installed; that policy does not protect annotations.
const (
	// ReservedPrefix namespaces everything this controller owns.
	ReservedPrefix = "warmpool.linkedin.com/"

	// LabelPool records which WorkerPool a Pod belongs to.
	//
	// This is an index, not the source of truth: membership is decided by the
	// controlling ownerReference, which is keyed on the pool's UID and cannot
	// be redirected by editing labels. Stripping this label therefore hides a
	// Pod from kubectl selectors but never from its pool's accounting. The
	// controller does not repair this label; the admission policy restricts
	// changes to its configured allowed identities.
	LabelPool = ReservedPrefix + "pool"

	// LabelState is the assignment state of a Pod: warm or claimed.
	//
	// It describes assignment, not readiness. A Pod that was just created is
	// state=warm but is not yet Ready; readiness is always computed from the
	// Pod's phase and conditions, never from this label.
	LabelState = ReservedPrefix + "state"

	// LabelClaim records the WorkerClaim name. A non-empty value makes the Pod
	// ineligible for another assignment. It is persisted with AnnotationClaimUID
	// before claim status is published.
	LabelClaim = ReservedPrefix + "claim"

	// LabelTemplateHash fingerprints template content, not the pool's generation.
	LabelTemplateHash = ReservedPrefix + "template-hash"

	// AnnotationClaimUID guards against claim name reuse: a deleted and
	// recreated claim with the same name has a different UID and must not
	// inherit the old claim's Pod.
	AnnotationClaimUID = ReservedPrefix + "claim-uid"
)

// Values for LabelState.
const (
	StateWarm    = "warm"
	StateClaimed = "claimed"
)
