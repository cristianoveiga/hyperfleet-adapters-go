# hyperfleet-adapters-go

Go implementation of the HyperFleet adapter pipeline. Five adapters run as independent processes, each subscribing to GCP Pub/Sub events and reconciling a specific aspect of an OpenShift hosted cluster's lifecycle.

## Architecture

```
HyperFleet API ←→ Adapter ←→ Maestro (ManifestWork) ←→ Management Cluster
                      ↑
                 GCP Pub/Sub
                  (Sentinel)
```

Each adapter follows the same reconciliation loop:

1. Receive a cluster/nodepool event from Pub/Sub
2. `GET /clusters/{id}` (or `/nodepools/{id}`) from the HyperFleet API
3. Check dependencies via `GET /clusters/{id}/statuses`
4. Perform its specific action (resolve version, select placement, apply ManifestWork, etc.)
5. Report outcome via `PUT /clusters/{id}/statuses`
6. Requeue via Pub/Sub (Sentinel republishes events for unreconciled clusters every ~5s)

## Adapters

| Subcommand | Pub/Sub Subscription | Responsibility |
|---|---|---|
| `version-resolution` | `hyperfleet-cluster-events-vr-adapter` | Resolves OCP release version → release image via Cincinnati |
| `nodepool-vr` | `hyperfleet-nodepool-events-nodepool-vr-adapter` | Same as above for node pools |
| `placement` | `hyperfleet-cluster-events-placement-adapter` | Selects management cluster and DNS base domain |
| `hc` | `hyperfleet-cluster-events-hc-adapter` | Creates/updates HostedCluster ManifestWork on the MC via Maestro |
| `nodepool` | `hyperfleet-nodepool-events-nodepool-adapter` | Creates/updates NodePool ManifestWork on the MC via Maestro |

### Pipeline order

```
version-resolution ──┐
                      ├──▶ placement ──▶ hc ──▶ nodepool
nodepool-vr ─────────┘
```

The `hc` adapter waits for both `placement` and `version-resolution` to report ready before proceeding. The `nodepool` adapter waits for `hc` to be available.

## Development

### Prerequisites

- Go 1.25+
- Access to a GCP project with Pub/Sub and Secret Manager
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
  --pubsub-project=my-gcp-project \
  --subscription=hyperfleet-cluster-events-hc-adapter \
  --maestro-grpc-addr=maestro-grpc:8090 \
  --maestro-http-addr=http://maestro:8000 \
  --source-id=hc-adapter \
  --log-level=info
```

### Common flags (all adapters)

| Flag | Env | Default | Description |
|---|---|---|---|
| `--api-url` | `HYPERFLEET_API_URL` | `http://hyperfleet-api:8000` | HyperFleet API base URL |
| `--pubsub-project` | `PUBSUB_PROJECT` | — | GCP project for Pub/Sub |
| `--subscription` | — | adapter-specific | Pub/Sub subscription name |
| `--workers` | — | `10` | Concurrent reconcile goroutines |
| `--resync` | — | `5m` | Periodic resync interval |
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
    pubsub/                      # Pub/Sub subscriber wrapper
    workqueue/                   # Worker goroutine pool
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
