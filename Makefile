CONTROLLER_GEN_VERSION ?= v0.17.2
ENVTEST_VERSION ?= release-0.20
ENVTEST_K8S_VERSION ?= 1.31.0
LOCALBIN ?= $(shell pwd)/bin
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
KIND_CLUSTER ?= warm-pool
NAMESPACE ?= warmpool-demo

.PHONY: all
all: generate manifests build

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN)
$(CONTROLLER_GEN): $(LOCALBIN)
	test -s $(CONTROLLER_GEN) || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)

.PHONY: generate
generate: controller-gen ## Generate DeepCopy methods.
	$(CONTROLLER_GEN) object:headerFile=hack/boilerplate.go.txt paths=./api/...

.PHONY: manifests
manifests: controller-gen ## Generate CRDs and RBAC.
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd
	$(CONTROLLER_GEN) rbac:roleName=warmpool-manager paths=./internal/... output:rbac:artifacts:config=config/rbac

.PHONY: fmt vet build
fmt:
	go fmt ./...
vet:
	go vet ./...
build: fmt vet
	go build ./...

.PHONY: setup-envtest
setup-envtest: $(LOCALBIN)
	test -s $(LOCALBIN)/setup-envtest || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)

.PHONY: test
test: generate manifests fmt vet setup-envtest ## Run tests against a real API server via envtest.
	KUBEBUILDER_ASSETS="$(shell $(LOCALBIN)/setup-envtest use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
		go test ./... -count=1

.PHONY: install
install: manifests ## Install CRDs into the current cluster.
	# Server-side apply is required: the CRD embeds a full PodSpec schema, so the
	# client-side last-applied-configuration annotation exceeds the 256KB limit.
	kubectl apply --server-side --force-conflicts -f config/crd

.PHONY: uninstall
uninstall:
	kubectl delete --ignore-not-found -f config/crd

.PHONY: policy-install
policy-install: ## Install the admission policy protecting reserved Pod labels.
	kubectl apply -f config/policy/pod-label-protection.yaml

.PHONY: policy-install-dev
policy-install-dev: ## Same, but also exempts kubernetes-admin so `make run` works. kind only.
	kubectl apply -f config/policy/pod-label-protection.kind-dev.yaml

.PHONY: policy-uninstall
policy-uninstall:
	kubectl delete --ignore-not-found -f config/policy/pod-label-protection.yaml

.PHONY: namespace
namespace:
	kubectl create namespace $(NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -

.PHONY: run
run: manifests generate ## Run the controller against the current cluster.
	go run ./cmd/manager

.PHONY: kind-up
kind-up: ## Create a local kind cluster.
	kind create cluster --name $(KIND_CLUSTER)

.PHONY: kind-down
kind-down:
	kind delete cluster --name $(KIND_CLUSTER)

.PHONY: samples
samples: namespace ## Apply the example WorkerPool and WorkerClaim.
	kubectl apply -f config/samples/workerpool.yaml
	kubectl apply -f config/samples/workerclaim.yaml
