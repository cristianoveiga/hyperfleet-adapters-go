package placement

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	privatev1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/conditions"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

const (
	adapterName  = "placement-adapter"
	requeueStable = 5 * time.Minute
)

// Reconciler implements the placement adapter reconcile loop.
type Reconciler struct {
	client     client.Client
	selector   Selector
	candidates []Candidate
	log        logger.Logger
}

// NewReconciler creates a new placement Reconciler.
func NewReconciler(selector Selector, candidates []Candidate, log logger.Logger, c client.Client) *Reconciler {
	return &Reconciler{
		selector:   selector,
		candidates: candidates,
		log:        log,
		client:     c,
	}
}

// Reconcile runs the placement reconciliation loop for the cluster identified
// by req.Name (= clusterID) in namespace req.Namespace (= project namespace).
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	clusterID := req.Name

	// Step 1: Read cluster from the cache.
	var cluster privatev1.Cluster
	if err := r.client.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.log.Infof(ctx, "placement: cluster %s not found, skipping", clusterID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("placement: get cluster %s: %w", clusterID, err)
	}

	// Step 2: Check if already placed.
	if cluster.Status.PlacementResult != nil && cluster.Status.PlacementResult.ManagementClusterName != "" {
		r.log.Infof(ctx, "placement: cluster %s already placed (mc=%s, domain=%s), waiting for next event",
			clusterID, cluster.Status.PlacementResult.ManagementClusterName, cluster.Status.PlacementResult.BaseDomain)
		return reconcile.Result{}, nil
	}

	// Step 3: Select MC and DNS zone.
	mc, domain, err := r.selector.Select(ctx, r.candidates)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("placement: select MC for cluster %s: %w", clusterID, err)
	}

	r.log.Infof(ctx, "placement: cluster %s: selected MC %s, domain %s", clusterID, mc, domain)

	// Step 4: Write placement result and ManagementClusterSelected condition to status.
	cluster.Status.PlacementResult = &privatev1.PlacementResult{
		ManagementClusterName: mc,
		BaseDomain:            domain,
	}
	conditions.Set(&cluster.Status.Conditions, metav1.Condition{
		Type:               "ManagementClusterSelected",
		Status:             metav1.ConditionTrue,
		Reason:             "PlacementDecided",
		Message:            fmt.Sprintf("Management cluster: %s, base domain: %s", mc, domain),
		ObservedGeneration: cluster.Generation,
	})
	if err := r.client.Status().Update(ctx, &cluster); err != nil {
		if apierrors.IsConflict(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("placement: update cluster status %s: %w", clusterID, err)
	}

	r.log.Infof(ctx, "placement: cluster %s placed, requeueing after %s", clusterID, requeueStable)
	return reconcile.Result{RequeueAfter: requeueStable}, nil
}
