// Package nodepool implements the nodepool adapter reconciler.
package nodepool

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	privatev1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/nodepool/manifest"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/conditions"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/transport"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

const (
	adapterName       = "nodepool-adapter"
	requeuePending    = 15 * time.Second
	requeueAfterApply = 5 * time.Minute
)

// Reconciler implements the nodepool adapter reconciliation loop.
type Reconciler struct {
	transport transport.Client
	log       logger.Logger
	client    client.Client
}

// New creates a new nodepool Reconciler.
func New(transport transport.Client, log logger.Logger, c client.Client) *Reconciler {
	return &Reconciler{
		transport: transport,
		log:       log,
		client:    c,
	}
}

// Reconcile runs the nodepool adapter loop for one nodepool event.
// req.Namespace = project namespace, req.Name = nodepoolID.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	nodepoolID := req.Name
	log := r.log.With("nodepoolID", nodepoolID)

	// Read nodepool from cache.
	var np privatev1.NodePool
	if err := r.client.Get(ctx, req.NamespacedName, &np); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof(ctx, "nodepool %s not found, skipping", nodepoolID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("nodepool reconciler: get nodepool: %w", err)
	}

	// Read parent cluster from cache using the cluster ID from spec.
	clusterID := np.Spec.ClusterID
	log = log.With("clusterID", clusterID)
	var cluster privatev1.Cluster
	clusterKey := types.NamespacedName{Namespace: req.Namespace, Name: clusterID}
	if err := r.client.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof(ctx, "cluster %s not found for nodepool %s, skipping", clusterID, nodepoolID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("nodepool reconciler: get cluster: %w", err)
	}

	// Gate: cluster placement must be ready.
	if cluster.Status.PlacementResult == nil || cluster.Status.PlacementResult.ManagementClusterName == "" {
		log.Infof(ctx, "placement not ready for nodepool %s, waiting for next event", nodepoolID)
		if setWaitingNPConditions(&np, "PlacementNotReady", "Waiting for cluster placement to select a management cluster") {
			if err := r.client.Status().Update(ctx, &np); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("nodepool reconciler: update nodepool status: %w", err)
			}
		}
		return reconcile.Result{}, nil
	}

	// Gate: HC must be Available (HostedClusterAvailable condition on the cluster).
	if !conditions.IsTrue(cluster.Status.Conditions, "HostedClusterAvailable") {
		log.Infof(ctx, "hc not available for nodepool %s, waiting for next event", nodepoolID)
		if setWaitingNPConditions(&np, "HostedClusterNotAvailable", "Waiting for HostedCluster to become available") {
			if err := r.client.Status().Update(ctx, &np); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("nodepool reconciler: update nodepool status: %w", err)
			}
		}
		return reconcile.Result{}, nil
	}

	// Gate: nodepool VR must be ready.
	if np.Status.VersionResolution == nil || np.Status.VersionResolution.ReleaseVersion == "" {
		log.Infof(ctx, "nodepool VR not ready for nodepool %s, waiting for next event", nodepoolID)
		if setWaitingNPConditions(&np, "VersionResolutionNotReady", "Waiting for nodepool version resolution") {
			if err := r.client.Status().Update(ctx, &np); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("nodepool reconciler: update nodepool status: %w", err)
			}
		}
		return reconcile.Result{}, nil
	}

	// Gate: VR version must match spec version.
	if np.Spec.Release != nil && np.Status.VersionResolution.ReleaseVersion != np.Spec.Release.Version {
		log.Infof(ctx, "nodepool VR version %q does not match spec version %q, waiting for next event",
			np.Status.VersionResolution.ReleaseVersion, np.Spec.Release.Version)
		msg := fmt.Sprintf("VR version %q does not match spec version %q",
			np.Status.VersionResolution.ReleaseVersion, np.Spec.Release.Version)
		if setWaitingNPConditions(&np, "VersionMismatch", msg) {
			if err := r.client.Status().Update(ctx, &np); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("nodepool reconciler: update nodepool status: %w", err)
			}
		}
		return reconcile.Result{}, nil
	}

	// Extract nodepool GCP platform fields.
	var machineType, gcpRegion, zone string
	var diskSizeGB int32
	var diskType string
	if gcp := np.Spec.Platform.GCP; gcp != nil {
		gcpRegion = gcp.Region
		machineType = gcp.MachineType
		diskSizeGB = gcp.DiskSize
		diskType = gcp.DiskType
		if len(gcp.Zones) > 0 {
			zone = gcp.Zones[0]
		}
	}
	if machineType == "" {
		machineType = manifest.DefaultMachineType
	}
	if diskSizeGB == 0 {
		diskSizeGB = manifest.DefaultDiskSizeGB
	}
	if diskType == "" {
		diskType = manifest.DefaultDiskType
	}

	// Extract cluster GCP platform fields.
	var gcpSubnet string
	if gcp := cluster.Spec.Platform.GCP; gcp != nil {
		if gcpRegion == "" {
			gcpRegion = gcp.Region
		}
		gcpSubnet = gcp.Subnet
	}
	if zone == "" && gcpRegion != "" {
		zone = gcpRegion + "-a"
	}

	mw, err := manifest.Build(manifest.Input{
		NodePoolID:         nodepoolID,
		NodePoolName:       np.Name,
		NodePoolGeneration: np.Generation,
		ClusterID:          clusterID,
		ClusterName:        cluster.Name,
		Replicas:           defaultReplicas,
		MachineType:        machineType,
		GCPRegion:          gcpRegion,
		Zone:               zone,
		GCPSubnet:          gcpSubnet,
		DiskSizeGB:         diskSizeGB,
		DiskType:           diskType,
		ReleaseImage:       np.Status.VersionResolution.ReleaseImage,
	})
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("nodepool reconciler: build manifest work: %w", err)
	}

	managementCluster := cluster.Status.PlacementResult.ManagementClusterName
	mwName := fmt.Sprintf("%s-%s", nodepoolID, adapterName)

	if err := r.transport.Apply(ctx, managementCluster, mw); err != nil {
		return reconcile.Result{}, fmt.Errorf("nodepool reconciler: apply manifest work: %w", err)
	}

	mwStatus, err := r.transport.GetStatus(ctx, managementCluster, mwName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof(ctx, "manifest work %s not found yet, reporting unknown status", mwName)
			mwStatus = nil
		} else {
			return reconcile.Result{}, fmt.Errorf("nodepool reconciler: get manifest work status: %w", err)
		}
	}

	// Write nodepool status conditions — only update if something changed.
	if r.applyStatusConditions(&np, mwStatus) {
		if err := r.client.Status().Update(ctx, &np); err != nil {
			if apierrors.IsConflict(err) {
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, fmt.Errorf("nodepool reconciler: update nodepool status: %w", err)
		}
	}

	if !conditions.IsTrue(np.Status.Conditions, "NodePoolManifestWorkApplied") {
		log.Infof(ctx, "nodepool reconciler: nodepool %s MW not yet applied, requeueing after %s", nodepoolID, requeuePending)
		return reconcile.Result{RequeueAfter: requeuePending}, nil
	}
	log.Infof(ctx, "nodepool reconciler: nodepool %s reconciled, requeueing after %s", nodepoolID, requeueAfterApply)
	return reconcile.Result{RequeueAfter: requeueAfterApply}, nil
}

// setWaitingNPConditions sets NodePoolManifestWorkApplied and NodePoolAvailable to Unknown.
// Returns true if either condition changed.
func setWaitingNPConditions(np *privatev1.NodePool, reason, message string) bool {
	gen := np.Generation
	a := conditions.Set(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolManifestWorkApplied",
		Status:             metav1.ConditionUnknown,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: gen,
	})
	b := conditions.Set(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolAvailable",
		Status:             metav1.ConditionUnknown,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: gen,
	})
	return a || b
}

// applyStatusConditions derives conditions from the ManifestWork status and writes them to the nodepool.
// Returns true if any condition changed.
func (r *Reconciler) applyStatusConditions(np *privatev1.NodePool, mwStatus *transport.ManifestWorkStatus) bool {
	gen := np.Generation

	if mwStatus == nil {
		a := conditions.Set(&np.Status.Conditions, metav1.Condition{
			Type:               "NodePoolManifestWorkApplied",
			Status:             metav1.ConditionFalse,
			Reason:             "ManifestWorkNotFound",
			ObservedGeneration: gen,
		})
		b := conditions.Set(&np.Status.Conditions, metav1.Condition{
			Type:               "NodePoolAvailable",
			Status:             metav1.ConditionFalse,
			Reason:             "ManifestWorkNotFound",
			ObservedGeneration: gen,
		})
		return a || b
	}

	// Extract conditions from top-level ManifestWork conditions.
	appliedStatus := metav1.ConditionStatus("False")
	appliedReason := "Unknown"
	for _, c := range mwStatus.Conditions {
		if c.Type == "Applied" {
			appliedStatus = c.Status
			appliedReason = c.Reason
			break
		}
	}

	// Extract resource status from manifest index 0 (the NodePool).
	availableStatus := "False"
	allNodesHealthy := "False"
	if len(mwStatus.ResourceStatuses) > 0 {
		rs := mwStatus.ResourceStatuses[0]
		if v, ok := rs["readyCondition"]; ok {
			availableStatus = v
		}
		if v, ok := rs["allNodesHealthyCondition"]; ok {
			allNodesHealthy = v
		}
	}

	healthStatus := metav1.ConditionFalse
	if allNodesHealthy == "True" {
		healthStatus = metav1.ConditionTrue
	}

	a := conditions.Set(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolManifestWorkApplied",
		Status:             appliedStatus,
		Reason:             appliedReason,
		ObservedGeneration: gen,
	})
	b := conditions.Set(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolAvailable",
		Status:             metav1.ConditionStatus(availableStatus),
		ObservedGeneration: gen,
	})
	c := conditions.Set(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolHealthy",
		Status:             healthStatus,
		ObservedGeneration: gen,
	})
	return a || b || c
}


// defaultReplicas is the hardcoded default for this POC.
const defaultReplicas = int32(1)
