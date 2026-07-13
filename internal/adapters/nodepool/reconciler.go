// Package nodepool implements the nodepool adapter reconciler.
package nodepool

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/nodepool/manifest"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/common/hyperfleetapi"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/hyperfleetstore"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/transport"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

const (
	adapterName       = "nodepool-adapter"
	requeueAfterApply = 5 * time.Minute
)

// Reconciler implements the nodepool adapter reconciliation loop.
type Reconciler struct {
	api       hyperfleetapi.Client
	transport transport.Client
	log       logger.Logger
	client    client.Client
	store     interface{ TriggerRepoll(clusterID string) }
}

// New creates a new nodepool Reconciler.
func New(api hyperfleetapi.Client, transport transport.Client, log logger.Logger, c client.Client, store interface{ TriggerRepoll(clusterID string) }) *Reconciler {
	return &Reconciler{
		api:       api,
		transport: transport,
		log:       log,
		client:    c,
		store:     store,
	}
}

// Reconcile runs the nodepool adapter loop for one nodepool event from the store-backed cache.
// req.Namespace = clusterID, req.Name = nodepoolID.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	clusterID := req.Namespace
	nodepoolID := req.Name
	log := r.log.With("clusterID", clusterID).With("nodepoolID", nodepoolID)

	// Read nodepool from cache.
	var np hyperfleetstore.HyperFleetNodePool
	if err := r.client.Get(ctx, req.NamespacedName, &np); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof(ctx, "nodepool %s not found, skipping", nodepoolID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("nodepool reconciler: get nodepool: %w", err)
	}

	// Read parent cluster from cache (namespace="hyperfleet", name=clusterID).
	var cluster hyperfleetstore.HyperFleetCluster
	clusterKey := types.NamespacedName{Namespace: hyperfleetstore.ClusterNamespace, Name: clusterID}
	if err := r.client.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof(ctx, "cluster %s not found for nodepool %s, skipping", clusterID, nodepoolID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("nodepool reconciler: get cluster: %w", err)
	}

	// Gate checks using AdapterStatuses pre-populated by the polling loop.
	placement := cluster.AdapterStatuses.Placement()
	hc := cluster.AdapterStatuses.HCAdapter()
	nodepoolVR := np.AdapterStatuses.NodePoolVR()

	if !placement.Ready() {
		log.Infof(ctx, "placement not ready for nodepool %s, waiting for next event", nodepoolID)
		return reconcile.Result{}, nil
	}
	if !hc.Available() {
		log.Infof(ctx, "hc-adapter not available for nodepool %s, waiting for next event", nodepoolID)
		return reconcile.Result{}, nil
	}
	if !nodepoolVR.Ready() {
		log.Infof(ctx, "nodepool VR not ready for nodepool %s, waiting for next event", nodepoolID)
		return reconcile.Result{}, nil
	}
	if nodepoolVR.ReleaseVersion != np.Spec.Release.Version {
		log.Infof(ctx, "nodepool VR version %q does not match spec version %q for nodepool %s, waiting for next event",
			nodepoolVR.ReleaseVersion, np.Spec.Release.Version, nodepoolID)
		return reconcile.Result{}, nil
	}

	zone := np.Spec.Platform.GCP.Zone
	if zone == "" {
		zone = np.Spec.Platform.GCP.Region + "-a"
	}

	mw, err := manifest.Build(manifest.Input{
		NodePoolID:         nodepoolID,
		NodePoolName:       np.DisplayName,
		NodePoolGeneration: np.HFGeneration,
		ClusterID:          clusterID,
		ClusterName:        cluster.DisplayName,
		Replicas:           defaultReplicas,
		MachineType:        manifest.DefaultMachineType,
		GCPRegion:          np.Spec.Platform.GCP.Region,
		Zone:               zone,
		GCPSubnet:          cluster.Spec.Platform.GCP.Subnet,
		DiskSizeGB:         manifest.DefaultDiskSizeGB,
		DiskType:           manifest.DefaultDiskType,
		ReleaseImage:       nodepoolVR.ReleaseImage,
	})
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("nodepool reconciler: build manifest work: %w", err)
	}

	managementCluster := placement.ManagementClusterName
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

	payload := r.buildStatusPayload(np.HFGeneration, mwStatus)

	if err := r.api.PutNodePoolStatus(ctx, clusterID, nodepoolID, payload); err != nil {
		return reconcile.Result{}, fmt.Errorf("nodepool reconciler: put nodepool status: %w", err)
	}

	if r.store != nil {
		r.store.TriggerRepoll(clusterID)
	}

	log.Infof(ctx, "nodepool reconciler: nodepool %s reconciled, requeueing after %s", nodepoolID, requeueAfterApply)
	return reconcile.Result{RequeueAfter: requeueAfterApply}, nil
}

// buildStatusPayload constructs the StatusPayload from the ManifestWorkStatus.
func (r *Reconciler) buildStatusPayload(generation int64, mwStatus *transport.ManifestWorkStatus) hyperfleetapi.StatusPayload {
	now := time.Now().UTC().Format(time.RFC3339)

	if mwStatus == nil {
		return hyperfleetapi.StatusPayload{
			Adapter:            adapterName,
			ObservedGeneration: generation,
			ObservedTime:       now,
			Conditions: []hyperfleetapi.Condition{
				{Type: "Applied", Status: "Unknown", Reason: "ManifestWorkNotFound"},
				{Type: "Available", Status: "Unknown", Reason: "ManifestWorkNotFound"},
				{Type: "Health", Status: "Unknown", Reason: "ManifestWorkNotFound"},
			},
			Data: map[string]any{
				"replicas": "",
				"version":  "",
			},
		}
	}

	// Extract conditions from top-level ManifestWork conditions
	appliedStatus := "Unknown"
	appliedReason := "Unknown"
	for _, c := range mwStatus.Conditions {
		if c.Type == "Applied" {
			appliedStatus = string(c.Status)
			appliedReason = c.Reason
			break
		}
	}

	// Extract resource status from manifest index 0 (the NodePool)
	availableStatus := "Unknown"
	allNodesHealthy := "Unknown"
	replicas := ""
	version := ""

	if len(mwStatus.ResourceStatuses) > 0 {
		rs := mwStatus.ResourceStatuses[0]
		if v, ok := rs["readyCondition"]; ok {
			availableStatus = v
		}
		if v, ok := rs["allNodesHealthyCondition"]; ok {
			allNodesHealthy = v
		}
		if v, ok := rs["replicas"]; ok {
			replicas = v
		}
		if v, ok := rs["version"]; ok {
			version = v
		}
	}

	healthStatus := "False"
	if allNodesHealthy == "True" {
		healthStatus = "True"
	}

	conditions := []hyperfleetapi.Condition{
		{
			Type:   "Applied",
			Status: appliedStatus,
			Reason: appliedReason,
		},
		{
			Type:   "Available",
			Status: availableStatus,
		},
		{
			Type:   "Health",
			Status: healthStatus,
		},
	}

	return hyperfleetapi.StatusPayload{
		Adapter:            adapterName,
		ObservedGeneration: generation,
		ObservedTime:       now,
		Conditions:         conditions,
		Data: map[string]any{
			"replicas": replicas,
			"version":  version,
		},
	}
}

// defaultReplicas is the hardcoded default for this POC.
const defaultReplicas = int32(1)
