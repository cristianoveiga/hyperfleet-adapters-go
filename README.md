# hyperfleet-adapters-go

Go implementation of the HyperFleet adapter pipeline. Five adapters run as independent processes, each polling the HyperFleet API for state changes and reconciling a specific aspect of an OpenShift hosted cluster's lifecycle.

## Why Go (vs. the YAML/CEL pipeline)

The previous adapter framework drove reconciliation through YAML configuration and CEL expressions. This repo replaces it with compiled Go. The four most consequential differences:

**Typed, testable resource builders.** The ManifestWork builders (`manifest.Build()`) are pure functions that can be unit-tested without deploying anything. In the YAML pipeline, the exact manifest sent to Maestro could only be validated by running the full stack — wrong Hypershift field names and a missing `spec.clusterID` went undetected until the MC work-agent rejected them at apply time. A table-driven test on `manifest.Build()` catches those at `go test`.

**Compiled binary eliminates the runtime failure surface.** Field name typos, missing map keys, and type mismatches that the YAML/CEL interpreter silently ignores are compile errors in Go. The entire class of "wrong field name in the generated manifest" bugs cannot ship.

**Kubernetes controller pattern.** Each adapter uses the controller-runtime informer pattern — the same infrastructure Kubernetes itself uses for controllers. Multiple events for the same cluster collapse into a single reconcile, retries use exponential backoff automatically, and concurrency is controlled without custom locking. This is battle-tested infrastructure rather than bespoke queueing logic.

**Explicit, readable dependency gating.** Each reconciler's preconditions are ordinary Go code (`placement.Ready() && vr.Ready() && ...`) — readable in one place, testable with a mock API client, and debuggable with standard tooling. The equivalent in the YAML pipeline was CEL conditions and implicit stage ordering spread across multiple config files.

## Architecture

```
HyperFleet API ←→ Adapter ←→ Maestro (ManifestWork) ←→ Management Cluster
```

Each adapter runs a self-contained reconciliation loop backed by a polling store:

```
HyperFleet HTTP API
    │
    │  pollLoop (every 10s, or immediately on TriggerRepoll)
    ▼
hyperfleetStore  ── in-memory maps: clusters, nodepools ──▶  r.client.Get()
    │                (SHA-256 hash change detection)                │
    │  EventAdded / EventModified / EventDeleted                    │ reads from
    ▼                                                               │ informer cache
storectrl informer cache                                            │
    │  (maintains indexed copy; re-lists + re-watches on disconnect)│
    │                                                               │
    ▼                                                               │
event handler → rate-limiting work queue (deduplicates)            │
    │                                                               │
    ▼                                                               │
worker goroutine ──▶ Reconcile(ctx, req) ◀─────────────────────────┘
                         │
                         ├── r.hfClient.PutClusterStatus(...)  writes to HyperFleet API
                         └── store.TriggerRepoll(clusterID)    immediate re-poll
```

### Key properties

**Change detection via content hashing.** The polling loop computes a SHA-256 hash of each cluster/nodepool's API response on every tick. An event is only emitted when the hash changes, so reconcilers are not invoked on no-op polls.

**The informer pattern.** The storectrl cache wraps the polling store into a controller-runtime-compatible informer. It performs an initial List to populate its indexed cache, then opens a Watch stream to receive incremental events. Reconcilers read from this indexed cache (`r.client.Get`) — never directly from the HTTP API — so reads are always fast and do not add API load.

**TriggerRepoll for self-consistency.** After each successful status write, `TriggerRepoll` fires an immediate re-fetch of that cluster so the same adapter process sees its own write reflected in the cache quickly. Cross-adapter propagation (e.g. placement → hc) is bounded by the poll interval — each adapter runs as an independent process with its own store, so a write by one is not visible to another until the next poll tick.

**Deduplication.** The work queue collapses multiple events for the same cluster into a single reconcile call. If the polling loop fires an add and a modify before a worker picks up the request, only one reconcile runs.

## Adapters

| Subcommand | Watches | Responsibility |
|---|---|---|
| `version-resolution` | `HyperFleetCluster` | Resolves OCP release version → release image via Cincinnati |
| `nodepool-vr` | `HyperFleetNodePool` | Same as above for node pools |
| `placement` | `HyperFleetCluster` | Selects management cluster and DNS base domain |
| `hc` | `HyperFleetCluster` | Creates/updates HostedCluster ManifestWork on the MC via Maestro |
| `nodepool` | `HyperFleetNodePool` | Creates/updates NodePool ManifestWork on the MC via Maestro |

### Pipeline order

```
version-resolution ──▶ placement ──▶ hc ──┐
                                           ├──▶ nodepool
nodepool-vr ───────────────────────────────┘
```

The `hc` adapter gates on `placement` and `version-resolution`. The `nodepool` adapter gates on `placement`, `hc` (must be available), and `nodepool-vr`.

## Development

### Prerequisites

- Go 1.26+
- A running HyperFleet API and Maestro instance

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
  --api-url=http://hyperfleet-api:8000 \
  --maestro-grpc-addr=maestro-grpc:8090 \
  --maestro-http-addr=http://maestro:8000 \
  --log-level=info
```

### Common flags (all adapters)

| Flag | Env | Default | Description |
|---|---|---|---|
| `--api-url` | `HYPERFLEET_API_URL` | `http://hyperfleet-api:8000` | HyperFleet API base URL |
| `--poll-interval` | — | `10s` | How often to poll the HyperFleet API for changes |
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
  common/
    hyperfleetapi/               # HyperFleet API client and domain types
    workqueue/                   # Worker goroutine pool (drives reconcilers from informer events)
  hyperfleetstore/               # storectrl.Store backed by the HyperFleet HTTP API
    store.go                     # polling loop, hash-based change detection, Watch/List/Get
    watcher.go                   # pollingWatcher: buffered channel with overflow-close
    types.go                     # HyperFleetCluster, HyperFleetNodePool (runtime.Object)
    convert.go                   # clusterFromAPI / nodepoolFromAPI converters
    scheme.go                    # scheme registration for store types
  maestroclient/                 # Maestro REST API client (consumers, resource-bundles)
  transport/                     # Applies ManifestWork to Maestro via gRPC/REST
pkg/
  logger/                        # Structured logger (zap-backed)
  version/                       # Binary version info
Dockerfile                       # Multi-stage: UBI9 builder → distroless nonroot
Makefile
```

## Deployment

Adapters are deployed via ArgoCD on the region cluster using Helm charts under `helm/charts/hyperfleet-*-adapter-go/` in the [gcp-hcp-infra](https://github.com/openshift-online/gcp-hcp-infra) repository. All five adapters share the same container image — the subcommand determines which adapter runs.
