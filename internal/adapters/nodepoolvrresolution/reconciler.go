package nodepoolvrresolution

import (
	"context"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/versionresolution"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/common/hyperfleetapi"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/hyperfleetstore"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

const (
	adapterName         = "nodepool-vr-adapter"
	defaultChannelGroup = "candidate"
	requeueLong         = 5 * time.Minute
)

// Reconciler resolves the OCP release image for a node pool via Cincinnati.
type Reconciler struct {
	hfClient   hyperfleetapi.Client
	cincinnati *versionresolution.CincinnatiClient
	log        logger.Logger
	client     client.Client
	store      interface{ TriggerRepoll(clusterID string) }
}

// NewReconciler creates a new nodepool-vr Reconciler.
func NewReconciler(hfClient hyperfleetapi.Client, cincinnati *versionresolution.CincinnatiClient, log logger.Logger, c client.Client, store interface{ TriggerRepoll(clusterID string) }) *Reconciler {
	return &Reconciler{
		hfClient:   hfClient,
		cincinnati: cincinnati,
		log:        log,
		client:     c,
		store:      store,
	}
}

// Reconcile runs the nodepool-vr loop for one nodepool event from the store-backed cache.
// req.Namespace = clusterID, req.Name = nodepoolID.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	clusterID := req.Namespace
	nodepoolID := req.Name

	var np hyperfleetstore.HyperFleetNodePool
	if err := r.client.Get(ctx, req.NamespacedName, &np); err != nil {
		if apierrors.IsNotFound(err) {
			r.log.Infof(ctx, "nodepool-vr: nodepool %s not found, skipping", nodepoolID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("nodepool-vr: get nodepool %s: %w", nodepoolID, err)
	}

	// Verify the parent cluster still exists before resolving the version.
	clusterKey := types.NamespacedName{Namespace: hyperfleetstore.ClusterNamespace, Name: clusterID}
	var cluster hyperfleetstore.HyperFleetCluster
	if err := r.client.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.log.Infof(ctx, "nodepool-vr: cluster %s not found for nodepool %s, skipping", clusterID, nodepoolID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("nodepool-vr: get cluster %s: %w", clusterID, err)
	}

	version := np.Spec.Release.Version
	if version == "" {
		r.log.Infof(ctx, "nodepool-vr: nodepool %s: release version not set, waiting for next event", nodepoolID)
		return reconcile.Result{}, nil
	}

	// Check already resolved via AdapterStatuses pre-populated by the polling loop.
	vr := np.AdapterStatuses.NodePoolVR()
	if vr.Ready() && vr.ReleaseVersion == version {
		r.log.Infof(ctx, "nodepool-vr: nodepool %s: version %s already resolved, waiting for next event", nodepoolID, version)
		return reconcile.Result{}, nil
	}

	channel, err := buildChannel(version, defaultChannelGroup)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("nodepool-vr: build channel for nodepool %s: %w", nodepoolID, err)
	}
	r.log.Infof(ctx, "nodepool-vr: nodepool %s: resolving version %s via channel %s", nodepoolID, version, channel)

	info, err := r.cincinnati.Resolve(ctx, version, channel)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("nodepool-vr: cincinnati resolve for nodepool %s: %w", nodepoolID, err)
	}
	if info == nil {
		r.log.Warnf(ctx, "nodepool-vr: nodepool %s: version %s not found in Cincinnati, waiting for next event", nodepoolID, version)
		return reconcile.Result{}, nil
	}

	payload := hyperfleetapi.StatusPayload{
		Adapter:            adapterName,
		ObservedGeneration: np.HFGeneration,
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
				Reason:  "ReleaseImageAvailable",
				Message: fmt.Sprintf("Release image available: %s", info.Payload),
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
			"release_version":       version,
			"release_channel":       channel,
			"release_channel_group": defaultChannelGroup,
		},
	}

	if err := r.hfClient.PutNodePoolStatus(ctx, clusterID, nodepoolID, payload); err != nil {
		return reconcile.Result{}, fmt.Errorf("nodepool-vr: put nodepool status %s: %w", nodepoolID, err)
	}

	if r.store != nil {
		r.store.TriggerRepoll(clusterID)
	}

	r.log.Infof(ctx, "nodepool-vr: nodepool %s: resolved version %s", nodepoolID, version)
	return reconcile.Result{RequeueAfter: requeueLong}, nil
}

// buildChannel constructs the Cincinnati channel name from a version string and channel group.
// e.g. "4.22.0-ec.4" + "candidate" → "candidate-4.22"
func buildChannel(version, channelGroup string) (string, error) {
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid version string %q: expected at least major.minor", version)
	}
	major := parts[0]
	minor := parts[1]
	return fmt.Sprintf("%s-%s.%s", channelGroup, major, minor), nil
}
