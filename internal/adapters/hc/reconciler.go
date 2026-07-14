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

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/hc/manifest"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/common/hyperfleetapi"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/hyperfleetstore"
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
	api       hyperfleetapi.Client
	transport transport.Client
	log       logger.Logger
	client    client.Client
}

// New creates a new Reconciler.
func New(api hyperfleetapi.Client, transport transport.Client, log logger.Logger, c client.Client) *Reconciler {
	return &Reconciler{
		api:       api,
		transport: transport,
		log:       log,
		client:    c,
	}
}

// Reconcile runs the hc-adapter loop for one cluster event from the store-backed cache.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	clusterID := req.Name
	log := r.log.With("adapter", adapterName).With("cluster_id", clusterID)

	var cluster hyperfleetstore.HyperFleetCluster
	if err := r.client.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof(ctx, "cluster not found, skipping")
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("%s: get cluster: %w", adapterName, err)
	}

	// Skip if already reconciled.
	for _, cond := range cluster.Status.Conditions {
		if cond.Type == "Reconciled" && cond.Status == "True" {
			log.Infof(ctx, "cluster already reconciled, waiting for next event")
			return reconcile.Result{}, nil
		}
	}

	// Check placement and version-resolution readiness via AdapterStatuses pre-populated by the polling loop.
	placement := cluster.AdapterStatuses.Placement()
	vr := cluster.AdapterStatuses.VersionResolution()

	if !placement.Ready() || !vr.Ready() || vr.ReleaseVersion != cluster.Spec.Release.Version {
		log.Infof(ctx, "dependencies not ready (placement=%v, vr=%v), waiting for next event",
			placement.Ready(), vr.Ready())
		return reconcile.Result{}, nil
	}

	// Build ManifestWork.
	mwInput := manifest.Input{
		ClusterID:            clusterID,
		ClusterName:          cluster.DisplayName,
		Generation:           cluster.HFGeneration,
		CreatedBy:            cluster.CreatedBy,
		InfraID:              cluster.Spec.InfraID,
		IssuerURL:            cluster.Spec.IssuerURL,
		ClusterIDUUID:        cluster.Spec.ClusterID,
		GCPProjectID:         cluster.Spec.Platform.GCP.ProjectID,
		GCPRegion:            cluster.Spec.Platform.GCP.Region,
		GCPNetwork:           cluster.Spec.Platform.GCP.Network,
		GCPSubnet:            cluster.Spec.Platform.GCP.Subnet,
		GCPEndpointAccess:    cluster.Spec.Platform.GCP.EndpointAccess,
		WIFProjectNumber:     cluster.Spec.Platform.GCP.WorkloadIdentity.ProjectNumber,
		WIFPoolID:            cluster.Spec.Platform.GCP.WorkloadIdentity.PoolID,
		WIFProviderID:        cluster.Spec.Platform.GCP.WorkloadIdentity.ProviderID,
		NodePoolEmail:        cluster.Spec.Platform.GCP.WorkloadIdentity.ServiceAccountsRef.NodePool,
		ControlPlaneEmail:    cluster.Spec.Platform.GCP.WorkloadIdentity.ServiceAccountsRef.ControlPlane,
		CloudControllerEmail: cluster.Spec.Platform.GCP.WorkloadIdentity.ServiceAccountsRef.CloudController,
		StorageEmail:         cluster.Spec.Platform.GCP.WorkloadIdentity.ServiceAccountsRef.Storage,
		ImageRegistryEmail:   cluster.Spec.Platform.GCP.WorkloadIdentity.ServiceAccountsRef.ImageRegistry,
		NetworkEmail:         cluster.Spec.Platform.GCP.WorkloadIdentity.ServiceAccountsRef.Network,
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

	payload := r.buildStatusPayload(cluster.HFGeneration, mwStatus)

	if err := r.api.PutClusterStatus(ctx, clusterID, payload); err != nil {
		return reconcile.Result{}, fmt.Errorf("%s: put cluster status: %w", adapterName, err)
	}


	log.Infof(ctx, "hc-adapter: cluster %s reconciled, requeueing after %s", clusterID, requeueReady)
	return reconcile.Result{RequeueAfter: requeueReady}, nil
}

// buildStatusPayload constructs the StatusPayload from the ManifestWork status.
func (r *Reconciler) buildStatusPayload(generation int64, mwStatus *transport.ManifestWorkStatus) hyperfleetapi.StatusPayload {
	payload := hyperfleetapi.StatusPayload{
		Adapter:            adapterName,
		ObservedGeneration: generation,
		ObservedTime:       time.Now().UTC().Format(time.RFC3339),
	}

	if mwStatus == nil {
		// ManifestWork not yet processed.
		payload.Conditions = []hyperfleetapi.Condition{
			{Type: "Applied", Status: "Unknown", Reason: "ManifestWorkNotFound", Message: "ManifestWork has not been processed yet"},
			{Type: "Available", Status: "Unknown", Reason: "ManifestWorkNotFound", Message: "ManifestWork has not been processed yet"},
			{Type: "Health", Status: "False", Reason: "ManifestWorkNotFound", Message: "ManifestWork has not been processed yet"},
		}
		payload.Data = map[string]any{
			"available": false,
		}
		return payload
	}

	// Derive Applied condition from top-level MW conditions.
	appliedStatus := conditionStatus(mwStatus.Conditions, "Applied")

	// Derive Available and Health from HC manifest statusFeedback (index 3).
	availableStatus := "Unknown"
	healthStatus := "False"

	if len(mwStatus.ResourceStatuses) > hostedClusterManifestIndex {
		hcFeedback := mwStatus.ResourceStatuses[hostedClusterManifestIndex]
		if v, ok := hcFeedback["availableCondition"]; ok {
			availableStatus = v
		}
		if degraded, ok := hcFeedback["degradedCondition"]; ok {
			if degraded == "False" {
				healthStatus = "True"
			} else {
				healthStatus = "False"
			}
		}
	}

	payload.Conditions = []hyperfleetapi.Condition{
		{Type: "Applied", Status: appliedStatus, Reason: "ManifestWorkApplied"},
		{Type: "Available", Status: availableStatus, Reason: "HostedClusterAvailable"},
		{Type: "Health", Status: healthStatus, Reason: "HostedClusterHealth"},
	}
	payload.Data = map[string]any{
		"available": availableStatus == "True",
	}

	return payload
}

// conditionStatus returns the status of the first condition matching condType,
// or "Unknown" if not found.
func conditionStatus(conditions []metav1.Condition, condType string) string {
	for _, c := range conditions {
		if c.Type == condType {
			return string(c.Status)
		}
	}
	return "Unknown"
}
