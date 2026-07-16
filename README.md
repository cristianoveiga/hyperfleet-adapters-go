# hyperfleet-adapters-go

Go implementation of the HyperFleet adapter pipeline. Five adapters run as independent processes, each watching for resource changes on the Orlop API server and reconciling a specific aspect of an OpenShift hosted cluster's lifecycle.

## Why Go (vs. the YAML/CEL pipeline)

The previous adapter framework drove reconciliation through YAML configuration and CEL expressions. This repo replaces it with compiled Go. The four most consequential differences:

**Typed, testable resource builders.** The ManifestWork builders (`manifest.Build()`) are pure functions that can be unit-tested without deploying anything. In the YAML pipeline, the exact manifest sent to Maestro could only be validated by running the full stack — wrong Hypershift field names and a missing `spec.clusterID` went undetected until the MC work-agent rejected them at apply time. A table-driven test on `manifest.Build()` catches those at `go test`.

**Compiled binary eliminates the runtime failure surface.** Field name typos, missing map keys, and type mismatches that the YAML/CEL interpreter silently ignores are compile errors in Go. The entire class of "wrong field name in the generated manifest" bugs cannot ship.

**Kubernetes controller pattern.** Each adapter uses the controller-runtime reconciler pattern — the same infrastructure Kubernetes itself uses for controllers. Multiple events for the same cluster collapse into a single reconcile, retries use exponential backoff automatically, and concurrency is controlled without custom locking. This is battle-tested infrastructure rather than bespoke queueing logic.

**Explicit, readable dependency gating.** Each reconciler's preconditions are ordinary Go code (`placement.Ready() && vr.Ready() && ...`) — readable in one place, testable with a mock API client, and debuggable with standard tooling. The equivalent in the YAML pipeline was CEL conditions and implicit stage ordering spread across multiple config files.

## Architecture

```
Orlop API server ←→ Adapter (controller-runtime reconciler) ←→ Maestro (ManifestWork) ←→ Management Cluster
```

Each adapter is a controller-runtime reconciler managed by a controller-runtime `Manager`. The Manager connects to the Orlop API server (a Kubernetes-compatible API server backed by an in-memory store) and watches `privatev1.Cluster` or `privatev1.NodePool` objects. Reconcilers never poll on a timer — they react to watch events from the API server, with an explicit `RequeueAfter` for self-healing.

### Key properties

**Controller-runtime event model.** The manager maintains a watch connection to the Orlop API server. Events (add/modify/delete) are deduped through a rate-limiting work queue before reaching `Reconcile()`. Multiple rapid changes to the same object produce a single reconcile call.

**Idempotent, generation-based applies.** ManifestWorks carry a `hyperfleet.io/generation` annotation. `CompareGenerations()` compares the new generation against the existing one in Maestro and decides whether to Create, Update, or Skip. Identical generations → no-op.

**Adaptive requeue for ManifestWork status.** After applying a ManifestWork, its status (Applied, HostedCluster available, etc.) is updated asynchronously by the work-agent on the management cluster. The `hc` and `nodepool` adapters use a two-speed requeue:

| State | Interval |
|---|---|
| ManifestWork not yet confirmed applied | 15 seconds (`requeuePending`) |
| Converged (applied) | 5 minutes (`requeueStable`) |

Once converged, the adapter backs off automatically — no external event source is needed.

## Adapters

| Subcommand | Watches | Responsibility |
|---|---|---|
| `version-resolution` | `Cluster` | Resolves OCP release version → release image via Cincinnati |
| `nodepool-vr` | `NodePool` | Same as above for node pools |
| `placement` | `Cluster` | Selects management cluster and DNS base domain |
| `hc` | `Cluster` | Creates/updates HostedCluster ManifestWork on the MC via Maestro |
| `nodepool` | `NodePool` | Creates/updates NodePool ManifestWork on the MC via Maestro |

### Pipeline order

```
version-resolution ──▶ placement ──▶ hc ──┐
                                           ├──▶ nodepool
nodepool-vr ───────────────────────────────┘
```

The `hc` adapter gates on `placement` and `version-resolution`. The `nodepool` adapter gates on `placement`, `hc` (must be available), and `nodepool-vr`.

## Status feedback model and adaptive requeue

### The gap: ManifestWork status changes don't trigger reconciles

The `hc` and `nodepool` adapters watch `Cluster` and `NodePool` objects respectively. After applying a ManifestWork to Maestro, the work-agent on the management cluster processes it asynchronously — the ManifestWork status (Applied, HostedCluster available, etc.) changes in Maestro independently of any event on the watched Kubernetes objects.

Because no Watch is registered for Maestro or ManifestWork status, the adapter would only re-read status on the next `RequeueAfter` tick. With a 5-minute requeue, status propagation was bounded by that full interval even when the ManifestWork had been applied within seconds.

### Why not a Maestro event bridge?

An alternative would be to subscribe to the Maestro gRPC event stream and push `GenericEvent`s into the controller queue whenever a ManifestWork status changes. This was rejected because:

- It creates a hard dependency on Maestro event delivery. If the event stream lags, disconnects, or drops events, the adapter silently stops reacting.
- It adds operational complexity: the adapter must maintain a persistent gRPC subscription and handle reconnection.
- Maestro events are a delivery mechanism, not a consistency guarantee — the adapter would still need to re-read status on reconnect.

### Adaptive requeue (implemented)

The `hc` and `nodepool` adapters check the condition they just wrote after each apply cycle. When the ManifestWork has not been confirmed as applied yet, they requeue quickly to pick up the Maestro status change as soon as it appears. Once the condition flips to `True`, they back off to the normal 5-minute interval.

## Development

### Prerequisites

- Go 1.26+
- A running Orlop API server and Maestro instance

### Build

```bash
make build          # produces bin/hyperfleet-adapters-go
make test           # run all tests
make lint           # golangci-lint
make docker-build   # build container image
```

### Run an adapter locally

```bash
./bin/hyperfleet-adapters-go hc \
  --orlop-url=http://hyperfleet-api:8080 \
  --maestro-grpc-addr=maestro-grpc:8090 \
  --maestro-http-addr=http://maestro:8000 \
  --log-level=info
```

### Common flags (all adapters)

| Flag | Env | Default | Description |
|---|---|---|---|
| `--orlop-url` | `ORLOP_URL` | `http://hyperfleet-api:8080` | Orlop API server URL |
| `--workers` | — | `10` | Concurrent reconcile goroutines |
| `--log-level` | `LOG_LEVEL` | `info` | Log level (debug/info/warn/error) |
| `--log-format` | `LOG_FORMAT` | `json` | Log format (json/text) |

## Repository layout

```
cmd/
  main.go                        # CLI root; registers all 5 subcommands
internal/
  adapters/
    versionresolution/           # version-resolution adapter
    nodepoolvrresolution/        # nodepool-vr adapter
    placement/                   # placement adapter (static + dynamic selector)
    hc/                          # hc adapter + ManifestWork builder
      manifest/
        manifestwork.go          # builds the HC ManifestWork spec
        manifestwork_test.go
    nodepool/                    # nodepool adapter + ManifestWork builder
      manifest/
        manifestwork.go          # builds the NodePool ManifestWork spec
        manifestwork_test.go
  conditions/                    # IsTrue / Set helpers for metav1.Condition slices
  maestroclient/                 # Maestro REST + gRPC client (ManifestWork CRUD)
  manifest/                      # Generation annotation utilities and discovery helpers
  transport/                     # transport.Client interface + maestro/mock implementations
  transportclient/               # Lower-level TransportClient interface (Apply/Get/Discover/Delete)
pkg/
  constants/                     # Shared annotation/label key constants
  errors/                        # Typed error hierarchy (APIError, K8sError, CELError, …)
  logger/                        # Structured logger (slog-backed, context-aware)
  version/                       # Binary version info (set via ldflags)
Dockerfile                       # Multi-stage: UBI9 builder → distroless nonroot
Makefile
```

## Deployment

Adapters are deployed via ArgoCD on the region cluster using Helm charts under `helm/charts/hyperkube-*-adapter/` in the [gcp-hcp-infra](https://github.com/openshift-online/gcp-hcp-infra) repository. All five adapters share the same container image — the subcommand determines which adapter runs.
