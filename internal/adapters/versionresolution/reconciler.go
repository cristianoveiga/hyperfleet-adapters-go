package versionresolution

import (
	"context"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/common/hyperfleetapi"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/hyperfleetstore"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

const (
	adapterName         = "version-resolution-adapter"
	defaultChannelGroup = "candidate"
	requeueLong         = 5 * time.Minute
)

// Reconciler resolves the OCP release image for a cluster via Cincinnati.
type Reconciler struct {
	hfClient   hyperfleetapi.Client
	cincinnati *CincinnatiClient
	log        logger.Logger
	client     client.Client
	store      interface{ TriggerRepoll(clusterID string) }
}

// NewReconciler creates a new version-resolution Reconciler.
func NewReconciler(hfClient hyperfleetapi.Client, cincinnati *CincinnatiClient, log logger.Logger, c client.Client, store interface{ TriggerRepoll(clusterID string) }) *Reconciler {
	return &Reconciler{
		hfClient:   hfClient,
		cincinnati: cincinnati,
		log:        log,
		client:     c,
		store:      store,
	}
}

// Reconcile runs the version-resolution loop for one cluster event from the store-backed cache.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	clusterID := req.Name

	var cluster hyperfleetstore.HyperFleetCluster
	if err := r.client.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.log.Infof(ctx, "vr: cluster %s not found, skipping", clusterID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("vr: get cluster %s: %w", clusterID, err)
	}

	// Skip if already reconciled.
	for _, cond := range cluster.Status.Conditions {
		if cond.Type == "Reconciled" && cond.Status == "True" {
			r.log.Infof(ctx, "vr: cluster %s: already reconciled, waiting for next event", clusterID)
			return reconcile.Result{}, nil
		}
	}

	version := cluster.Spec.Release.Version
	if version == "" {
		r.log.Infof(ctx, "vr: cluster %s: release version not set, waiting for next event", clusterID)
		return reconcile.Result{}, nil
	}

	// Check already resolved via AdapterStatuses pre-populated by the polling loop.
	vr := cluster.AdapterStatuses.VersionResolution()
	if vr.Ready() && vr.ReleaseVersion == version {
		r.log.Infof(ctx, "vr: cluster %s: version %s already resolved, waiting for next event", clusterID, version)
		return reconcile.Result{}, nil
	}

	channel := buildChannel(version)
	r.log.Infof(ctx, "vr: cluster %s: resolving version %s via channel %s", clusterID, version, channel)

	info, err := r.cincinnati.Resolve(ctx, version, channel)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("vr: cincinnati resolve for cluster %s: %w", clusterID, err)
	}
	if info == nil {
		r.log.Warnf(ctx, "vr: cluster %s: version %s not found in Cincinnati, waiting for next event", clusterID, version)
		return reconcile.Result{}, nil
	}

	payload := hyperfleetapi.StatusPayload{
		Adapter:            adapterName,
		ObservedGeneration: cluster.Generation,
		ObservedTime:       time.Now().UTC().Format(time.RFC3339),
		Conditions: []hyperfleetapi.Condition{
			{
				Type:    "Applied",
				Status:  "True",
				Reason:  "VersionResolved",
				Message: fmt.Sprintf("Version %s resolved", version),
			},
			{
				Type:    "Available",
				Status:  "True",
				Reason:  "VersionResolved",
				Message: fmt.Sprintf("Version %s resolved", version),
			},
			{
				Type:    "Health",
				Status:  "True",
				Reason:  "VersionResolved",
				Message: fmt.Sprintf("Version %s resolved", version),
			},
		},
		Data: map[string]any{
			"release_image":         info.Payload,
			"release_version":       info.Version,
			"release_channel":       channel,
			"release_channel_group": defaultChannelGroup,
		},
	}

	if err := r.hfClient.PutClusterStatus(ctx, clusterID, payload); err != nil {
		return reconcile.Result{}, fmt.Errorf("vr: put cluster status %s: %w", clusterID, err)
	}

	if r.store != nil {
		r.store.TriggerRepoll(clusterID)
	}

	r.log.Infof(ctx, "vr: cluster %s: resolved version %s", clusterID, version)
	return reconcile.Result{RequeueAfter: requeueLong}, nil
}

// buildChannel constructs the Cincinnati channel name from a version string.
// e.g. "4.22.0-ec.4" → "candidate-4.22"
func buildChannel(version string) string {
	parts := strings.Split(version, ".")
	major := "4"
	minor := "0"
	if len(parts) >= 1 {
		major = parts[0]
	}
	if len(parts) >= 2 {
		minor = parts[1]
	}
	return fmt.Sprintf("%s-%s.%s", defaultChannelGroup, major, minor)
}
