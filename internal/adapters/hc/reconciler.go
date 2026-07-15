// Package hc implements the hc-adapter reconciler for managing HostedClusters via ManifestWork.
package hc

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	privatev1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/hc/manifest"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/transport"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

const (
	adapterName = "hc-adapter"

	requeueReady = 5 * time.Minute

	// hostedClusterManifestIndex is the manifest index for the HostedCluster in the ManifestWork.
	hostedClusterManifestIndex = 3
)

// Reconciler implements the hc-adapter reconcile loop.
type Reconciler struct {
	transport transport.Client
	log       logger.Logger
	client    client.Client
}

// New creates a new Reconciler.
func New(transport transport.Client, log logger.Logger, c client.Client) *Reconciler {
	return &Reconciler{
		transport: transport,
		log:       log,
		client:    c,
	}
}

// Reconcile runs the hc-adapter loop for one cluster event.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	clusterID := req.Name
	log := r.log.With("adapter", adapterName).With("cluster_id", clusterID)

	var cluster privatev1.Cluster
	if err := r.client.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof(ctx, "cluster not found, skipping")
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("%s: get cluster: %w", adapterName, err)
	}

	// Check placement readiness.
	if cluster.Status.PlacementResult == nil || cluster.Status.PlacementResult.ManagementClusterName == "" {
		log.Infof(ctx, "placement not ready, waiting for next event")
		return reconcile.Result{}, nil
	}

	// Check version-resolution readiness.
	if cluster.Status.VersionResolution == nil {
		log.Infof(ctx, "version resolution not ready, waiting for next event")
		return reconcile.Result{}, nil
	}

	// Check version match (handle nil Release gracefully).
	if cluster.Spec.Release != nil && cluster.Status.VersionResolution.ReleaseVersion != cluster.Spec.Release.Version {
		log.Infof(ctx, "vr version %q does not match spec version %q, waiting for next event",
			cluster.Status.VersionResolution.ReleaseVersion, cluster.Spec.Release.Version)
		return reconcile.Result{}, nil
	}

	placement := cluster.Status.PlacementResult
	vr := cluster.Status.VersionResolution

	// Extract platform fields.
	var gcpProjectID, gcpRegion, gcpNetwork, gcpSubnet string
	var wifProjectNumber, wifPoolID, wifProviderID string
	var nodePoolEmail, controlPlaneEmail, cloudControllerEmail string
	var storageEmail, imageRegistryEmail, networkEmail string
	if gcp := cluster.Spec.Platform.GCP; gcp != nil {
		gcpProjectID = gcp.ProjectID
		gcpRegion = gcp.Region
		gcpNetwork = gcp.Network
		gcpSubnet = gcp.Subnet
		if wif := gcp.WorkloadIdentity; wif != nil {
			wifProjectNumber = wif.ProjectNumber
			wifPoolID = wif.PoolID
			wifProviderID = wif.ProviderID
			if sa := wif.ServiceAccountsRef; sa != nil {
				nodePoolEmail = sa.NodePoolEmail
				controlPlaneEmail = sa.ControlPlaneEmail
				cloudControllerEmail = sa.CloudControllerEmail
				storageEmail = sa.StorageEmail
				imageRegistryEmail = sa.ImageRegistryEmail
				networkEmail = sa.NetworkEmail
			}
		}
	}

	// Build ManifestWork.
	// TODO: ClusterIDUUID and CreatedBy are not yet in the orlop ClusterSpec.
	mwInput := manifest.Input{
		ClusterID:            clusterID,
		ClusterName:          cluster.Name,
		Generation:           cluster.Generation,
		CreatedBy:            "", // TODO: not in types
		InfraID:              cluster.Spec.InfraID,
		IssuerURL:            cluster.Spec.IssuerURL,
		ClusterIDUUID:        "", // TODO: not in types
		GCPProjectID:         gcpProjectID,
		GCPRegion:            gcpRegion,
		GCPNetwork:           gcpNetwork,
		GCPSubnet:            gcpSubnet,
		WIFProjectNumber:     wifProjectNumber,
		WIFPoolID:            wifPoolID,
		WIFProviderID:        wifProviderID,
		NodePoolEmail:        nodePoolEmail,
		ControlPlaneEmail:    controlPlaneEmail,
		CloudControllerEmail: cloudControllerEmail,
		StorageEmail:         storageEmail,
		ImageRegistryEmail:   imageRegistryEmail,
		NetworkEmail:         networkEmail,
		ReleaseImage:         vr.ReleaseImage,
		ReleaseChannel:       vr.ReleaseChannel,
		BaseDomain:           placement.BaseDomain,
	}

	mw, err := manifest.Build(mwInput)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("%s: build manifest work: %w", adapterName, err)
	}

	if err := r.transport.Apply(ctx, placement.ManagementClusterName, mw); err != nil {
		return reconcile.Result{}, fmt.Errorf("%s: apply manifest work: %w", adapterName, err)
	}

	mwName := mw.Name
	mwStatus, err := r.transport.GetStatus(ctx, placement.ManagementClusterName, mwName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			mwStatus = nil
		} else {
			return reconcile.Result{}, fmt.Errorf("%s: get manifest work status: %w", adapterName, err)
		}
	}

	// Write status conditions.
	r.applyStatusConditions(&cluster, mwStatus)
	if err := r.client.Status().Update(ctx, &cluster); err != nil {
		if apierrors.IsConflict(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("%s: update cluster status: %w", adapterName, err)
	}

	log.Infof(ctx, "hc-adapter: cluster %s reconciled, requeueing after %s", clusterID, requeueReady)
	return reconcile.Result{RequeueAfter: requeueReady}, nil
}

// applyStatusConditions derives conditions from the ManifestWork status and writes them to the cluster.
func (r *Reconciler) applyStatusConditions(cluster *privatev1.Cluster, mwStatus *transport.ManifestWorkStatus) {
	gen := cluster.Generation

	if mwStatus == nil {
		setCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               "ManifestWorkApplied",
			Status:             metav1.ConditionFalse,
			Reason:             "ManifestWorkNotFound",
			Message:            "ManifestWork has not been processed yet",
			ObservedGeneration: gen,
			LastTransitionTime: metav1.Now(),
		})
		setCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               "HostedClusterAvailable",
			Status:             metav1.ConditionFalse,
			Reason:             "ManifestWorkNotFound",
			Message:            "ManifestWork has not been processed yet",
			ObservedGeneration: gen,
			LastTransitionTime: metav1.Now(),
		})
		return
	}

	// Derive ManifestWorkApplied condition from top-level MW conditions.
	appliedStatus := conditionStatus(mwStatus.Conditions, "Applied")

	// Derive HostedClusterAvailable from HC manifest statusFeedback (index 3).
	availableStatus := string(metav1.ConditionFalse)
	if len(mwStatus.ResourceStatuses) > hostedClusterManifestIndex {
		hcFeedback := mwStatus.ResourceStatuses[hostedClusterManifestIndex]
		if v, ok := hcFeedback["availableCondition"]; ok {
			availableStatus = v
		}
	}

	setCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               "ManifestWorkApplied",
		Status:             metav1.ConditionStatus(appliedStatus),
		Reason:             "ManifestWorkApplied",
		ObservedGeneration: gen,
		LastTransitionTime: metav1.Now(),
	})
	setCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               "HostedClusterAvailable",
		Status:             metav1.ConditionStatus(availableStatus),
		Reason:             "HostedClusterAvailable",
		ObservedGeneration: gen,
		LastTransitionTime: metav1.Now(),
	})
}

// conditionStatus returns the status of the first condition matching condType,
// or "False" if not found.
func conditionStatus(conditions []metav1.Condition, condType string) string {
	for _, c := range conditions {
		if c.Type == condType {
			return string(c.Status)
		}
	}
	return "False"
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
