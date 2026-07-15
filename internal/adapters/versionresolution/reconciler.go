package versionresolution

import (
	"context"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	privatev1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

const (
	adapterName         = "version-resolution-adapter"
	defaultChannelGroup = "candidate"
	requeueLong         = 5 * time.Minute
)

// Reconciler resolves the OCP release image for a cluster via Cincinnati.
type Reconciler struct {
	cincinnati *CincinnatiClient
	log        logger.Logger
	client     client.Client
}

// NewReconciler creates a new version-resolution Reconciler.
func NewReconciler(cincinnati *CincinnatiClient, log logger.Logger, c client.Client) *Reconciler {
	return &Reconciler{
		cincinnati: cincinnati,
		log:        log,
		client:     c,
	}
}

// Reconcile runs the version-resolution loop for one cluster event.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	clusterID := req.Name

	var cluster privatev1.Cluster
	if err := r.client.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.log.Infof(ctx, "vr: cluster %s not found, skipping", clusterID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("vr: get cluster %s: %w", clusterID, err)
	}

	if cluster.Spec.Release == nil || cluster.Spec.Release.Version == "" {
		r.log.Infof(ctx, "vr: cluster %s: release version not set, waiting for next event", clusterID)
		return reconcile.Result{}, nil
	}
	version := cluster.Spec.Release.Version

	// Check already resolved.
	if cluster.Status.VersionResolution != nil && cluster.Status.VersionResolution.ReleaseVersion == version {
		r.log.Infof(ctx, "vr: cluster %s: version %s already resolved, waiting for next event", clusterID, version)
		return reconcile.Result{}, nil
	}

	channelGroup := defaultChannelGroup
	if cluster.Spec.Release.ChannelGroup != "" {
		channelGroup = cluster.Spec.Release.ChannelGroup
	}
	channel := buildChannel(version, channelGroup)
	r.log.Infof(ctx, "vr: cluster %s: resolving version %s via channel %s", clusterID, version, channel)

	info, err := r.cincinnati.Resolve(ctx, version, channel)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("vr: cincinnati resolve for cluster %s: %w", clusterID, err)
	}
	if info == nil {
		r.log.Warnf(ctx, "vr: cluster %s: version %s not found in Cincinnati, waiting for next event", clusterID, version)
		return reconcile.Result{}, nil
	}

	// Write VR result and VersionResolved condition to status.
	cluster.Status.VersionResolution = &privatev1.VersionResolutionResult{
		ReleaseImage:   info.Payload,
		ReleaseVersion: info.Version,
		ReleaseChannel: channel,
	}
	setCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               "VersionResolved",
		Status:             metav1.ConditionTrue,
		Reason:             "VersionResolved",
		Message:            fmt.Sprintf("Version %s resolved to image %s", version, info.Payload),
		ObservedGeneration: cluster.Generation,
		LastTransitionTime: metav1.Now(),
	})
	if err := r.client.Status().Update(ctx, &cluster); err != nil {
		if apierrors.IsConflict(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("vr: update cluster status %s: %w", clusterID, err)
	}

	r.log.Infof(ctx, "vr: cluster %s: resolved version %s", clusterID, version)
	return reconcile.Result{RequeueAfter: requeueLong}, nil
}

// buildChannel constructs the Cincinnati channel name from a version string and channel group.
// e.g. "4.22.0-ec.4" + "stable" → "stable-4.22"
func buildChannel(version, channelGroup string) string {
	parts := strings.Split(version, ".")
	major := "4"
	minor := "0"
	if len(parts) >= 1 {
		major = parts[0]
	}
	if len(parts) >= 2 {
		minor = parts[1]
	}
	return fmt.Sprintf("%s-%s.%s", channelGroup, major, minor)
}

// setCondition upserts a condition into the slice, preserving timestamps when status is unchanged.
func setCondition(conditions *[]metav1.Condition, c metav1.Condition) {
	if c.LastTransitionTime.IsZero() {
		c.LastTransitionTime = metav1.Now()
	}
	for i, existing := range *conditions {
		if existing.Type == c.Type {
			if existing.Status != c.Status {
				c.LastTransitionTime = metav1.Now()
			} else {
				c.LastTransitionTime = existing.LastTransitionTime
			}
			(*conditions)[i] = c
			return
		}
	}
	*conditions = append(*conditions, c)
}
