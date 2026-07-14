package placement

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/common/hyperfleetapi"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/hyperfleetstore"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

const (
	adapterName  = "placement-adapter"
	requeueAfter = 5 * time.Minute
)

// Reconciler implements the placement adapter reconcile loop.
// It reads clusters from the store-backed cache and writes placement status
// directly to the HyperFleet API.
type Reconciler struct {
	client     client.Client
	hfClient   hyperfleetapi.Client
	selector   Selector
	candidates []Candidate
	log        logger.Logger
}

// NewReconciler creates a new placement Reconciler.
func NewReconciler(hfClient hyperfleetapi.Client, selector Selector, candidates []Candidate, log logger.Logger, c client.Client) *Reconciler {
	return &Reconciler{
		hfClient:   hfClient,
		selector:   selector,
		candidates: candidates,
		log:        log,
		client:     c,
	}
}

// Reconcile runs the placement reconciliation loop for the cluster identified
// by req.Name (= clusterID) in namespace req.Namespace (= "hyperfleet").
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	clusterID := req.Name

	// Step 1: Read cluster from the store-backed cache.
	var cluster hyperfleetstore.HyperFleetCluster
	if err := r.client.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.log.Infof(ctx, "placement: cluster %s not found, skipping", clusterID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("placement: get cluster %s: %w", clusterID, err)
	}

	// Step 2: If cluster has Reconciled condition "True" → skip.
	for _, c := range cluster.Status.Conditions {
		if c.Type == "Reconciled" && c.Status == "True" {
			r.log.Infof(ctx, "placement: cluster %s already reconciled, waiting for next event", clusterID)
			return reconcile.Result{}, nil
		}
	}

	// Step 3: Check AdapterStatuses from the cache (pre-populated by the polling loop).
	statuses := cluster.AdapterStatuses
	placement := statuses.Placement()
	if placement.Ready() {
		r.log.Infof(ctx, "placement: cluster %s already placed (mc=%s, domain=%s), waiting for next event",
			clusterID, placement.ManagementClusterName, placement.BaseDomain)
		return reconcile.Result{}, nil
	}

	// Step 4: Select MC and DNS zone.
	mc, domain, err := r.selector.Select(ctx, r.candidates)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("placement: select MC for cluster %s: %w", clusterID, err)
	}

	r.log.Infof(ctx, "placement: cluster %s: selected MC %s, domain %s", clusterID, mc, domain)

	// Step 5: PUT /clusters/{id}/statuses.
	payload := hyperfleetapi.StatusPayload{
		Adapter:      adapterName,
		ObservedTime: time.Now().UTC().Format(time.RFC3339),
		Conditions: []hyperfleetapi.Condition{
			{
				Type:    "Applied",
				Status:  "True",
				Reason:  "PlacementDecided",
				Message: "MC and DNS zone selected",
			},
			{
				Type:    "Available",
				Status:  "True",
				Reason:  "PlacementReady",
				Message: fmt.Sprintf("Management cluster: %s, base domain: %s", mc, domain),
			},
			{
				Type:    "Health",
				Status:  "True",
				Reason:  "PlacementReady",
				Message: fmt.Sprintf("Management cluster: %s, base domain: %s", mc, domain),
			},
		},
		Data: map[string]any{
			"managementClusterName": mc,
			"baseDomain":            domain,
		},
	}

	if err := r.hfClient.PutClusterStatus(ctx, clusterID, payload); err != nil {
		return reconcile.Result{}, fmt.Errorf("placement: put cluster status for %s: %w", clusterID, err)
	}

	// Step 6: Requeue.
	r.log.Infof(ctx, "placement: cluster %s placed, requeueing after %s", clusterID, requeueAfter)
	return reconcile.Result{RequeueAfter: requeueAfter}, nil
}

