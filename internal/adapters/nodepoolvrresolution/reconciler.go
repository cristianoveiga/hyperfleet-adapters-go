package nodepoolvrresolution

import (
	"context"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	privatev1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/versionresolution"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/conditions"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

const (
	adapterName         = "nodepool-vr-adapter"
	defaultChannelGroup = "candidate"
	requeueStable       = 5 * time.Minute
)

// Reconciler resolves the OCP release image for a node pool via Cincinnati.
type Reconciler struct {
	cincinnati *versionresolution.CincinnatiClient
	log        logger.Logger
	client     client.Client
}

// NewReconciler creates a new nodepool-vr Reconciler.
func NewReconciler(cincinnati *versionresolution.CincinnatiClient, log logger.Logger, c client.Client) *Reconciler {
	return &Reconciler{
		cincinnati: cincinnati,
		log:        log,
		client:     c,
	}
}

// Reconcile runs the nodepool-vr loop for one nodepool event.
// req.Namespace = project namespace, req.Name = nodepoolID.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	nodepoolID := req.Name

	var np privatev1.NodePool
	if err := r.client.Get(ctx, req.NamespacedName, &np); err != nil {
		if apierrors.IsNotFound(err) {
			r.log.Infof(ctx, "nodepool-vr: nodepool %s not found, skipping", nodepoolID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("nodepool-vr: get nodepool %s: %w", nodepoolID, err)
	}

	// Verify the parent cluster still exists.
	clusterID := np.Spec.ClusterID
	clusterKey := types.NamespacedName{Namespace: req.Namespace, Name: clusterID}
	var cluster privatev1.Cluster
	if err := r.client.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.log.Infof(ctx, "nodepool-vr: cluster %s not found for nodepool %s, skipping", clusterID, nodepoolID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("nodepool-vr: get cluster %s: %w", clusterID, err)
	}

	if np.Spec.Release == nil || np.Spec.Release.Version == "" {
		r.log.Infof(ctx, "nodepool-vr: nodepool %s: release version not set, waiting for next event", nodepoolID)
		if conditions.Set(&np.Status.Conditions, metav1.Condition{
			Type:               "NodePoolVersionResolved",
			Status:             metav1.ConditionUnknown,
			Reason:             "ReleaseVersionNotSet",
			Message:            "Release version not set in spec",
			ObservedGeneration: np.Generation,
		}) {
			if err := r.client.Status().Update(ctx, &np); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("nodepool-vr: update nodepool status %s: %w", nodepoolID, err)
			}
		}
		return reconcile.Result{}, nil
	}
	version := np.Spec.Release.Version

	// Check already resolved.
	if np.Status.VersionResolution != nil && np.Status.VersionResolution.ReleaseVersion == version {
		r.log.Infof(ctx, "nodepool-vr: nodepool %s: version %s already resolved, waiting for next event", nodepoolID, version)
		return reconcile.Result{}, nil
	}

	channelGroup := defaultChannelGroup
	if np.Spec.Release.ChannelGroup != "" {
		channelGroup = np.Spec.Release.ChannelGroup
	}
	channel, err := buildChannel(version, channelGroup)
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
		if conditions.Set(&np.Status.Conditions, metav1.Condition{
			Type:               "NodePoolVersionResolved",
			Status:             metav1.ConditionFalse,
			Reason:             "VersionNotFoundInCincinnati",
			Message:            fmt.Sprintf("Version %s not found in Cincinnati channel %s", version, channel),
			ObservedGeneration: np.Generation,
		}) {
			if err := r.client.Status().Update(ctx, &np); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("nodepool-vr: update nodepool status %s: %w", nodepoolID, err)
			}
		}
		return reconcile.Result{}, nil
	}

	// Write VR result and NodePoolVersionResolved condition to status.
	np.Status.VersionResolution = &privatev1.VersionResolutionResult{
		ReleaseImage:   info.Payload,
		ReleaseVersion: info.Version,
		ReleaseChannel: channel,
	}
	conditions.Set(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolVersionResolved",
		Status:             metav1.ConditionTrue,
		Reason:             "VersionResolved",
		Message:            fmt.Sprintf("Version %s resolved to image %s", version, info.Payload),
		ObservedGeneration: np.Generation,
	})
	if err := r.client.Status().Update(ctx, &np); err != nil {
		if apierrors.IsConflict(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("nodepool-vr: update nodepool status %s: %w", nodepoolID, err)
	}

	r.log.Infof(ctx, "nodepool-vr: nodepool %s: resolved version %s", nodepoolID, version)
	return reconcile.Result{RequeueAfter: requeueStable}, nil
}

// buildChannel constructs the Cincinnati channel name from a version string and channel group.
// e.g. "4.22.0-rc.5" + "candidate" → "candidate-4.22"
func buildChannel(version, channelGroup string) (string, error) {
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid version %q: expected at least major.minor", version)
	}
	return fmt.Sprintf("%s-%s.%s", channelGroup, parts[0], parts[1]), nil
}
