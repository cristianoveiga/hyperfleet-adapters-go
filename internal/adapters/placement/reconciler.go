package placement

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	privatev1alpha1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1alpha1"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

const (
	adapterName  = "placement-adapter"
	requeueAfter = 5 * time.Minute
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
// by req.Name (= clusterID) in namespace req.Namespace (= "hyperfleet").
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	clusterID := req.Name

	// Step 1: Read cluster from the cache.
	var cluster privatev1alpha1.Cluster
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

	// Step 4: Write placement result and status conditions to status.
	cluster.Status.PlacementResult = &privatev1alpha1.PlacementResult{
		ManagementClusterName: mc,
		BaseDomain:            domain,
	}
	setCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               "Applied",
		Status:             metav1.ConditionTrue,
		Reason:             "PlacementDecided",
		Message:            "MC and DNS zone selected",
		ObservedGeneration: cluster.Generation,
		LastTransitionTime: metav1.Now(),
	})
	setCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               "Available",
		Status:             metav1.ConditionTrue,
		Reason:             "PlacementReady",
		Message:            fmt.Sprintf("Management cluster: %s, base domain: %s", mc, domain),
		ObservedGeneration: cluster.Generation,
		LastTransitionTime: metav1.Now(),
	})
	if err := r.client.Status().Update(ctx, &cluster); err != nil {
		if apierrors.IsConflict(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("placement: update cluster status %s: %w", clusterID, err)
	}

	// Step 6: Requeue.
	r.log.Infof(ctx, "placement: cluster %s placed, requeueing after %s", clusterID, requeueAfter)
	return reconcile.Result{RequeueAfter: requeueAfter}, nil
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
