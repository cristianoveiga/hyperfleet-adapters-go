package nodepool

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	workv1 "open-cluster-management.io/api/work/v1"

	privatev1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/transport"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/transport/mock"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestLogger(t *testing.T) logger.Logger {
	t.Helper()
	log, err := logger.NewLogger(logger.Config{
		Level:     "debug",
		Format:    logger.FormatText,
		Output:    "stdout",
		Component: "test",
		Version:   "test",
	})
	require.NoError(t, err)
	return log
}

// mockStatusWriter captures Status().Update calls and can return a configured error.
type mockStatusWriter struct {
	called    bool
	updateErr error
	captured  client.Object
}

func (m *mockStatusWriter) Update(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
	m.called = true
	m.captured = obj
	return m.updateErr
}
func (m *mockStatusWriter) Create(_ context.Context, _ client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
	return nil
}
func (m *mockStatusWriter) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
	return nil
}
func (m *mockStatusWriter) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
	return nil
}

// mockStoreClient is a minimal client.Client backed by a fixed NodePool and Cluster.
type mockStoreClient struct {
	nodepool     *privatev1.NodePool
	cluster      *privatev1.Cluster
	npGetErr     error
	clsGetErr    error
	statusWriter *mockStatusWriter
}

func (m *mockStoreClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	switch o := obj.(type) {
	case *privatev1.NodePool:
		if m.npGetErr != nil {
			return m.npGetErr
		}
		if m.nodepool == nil {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "nodepool"}, "")
		}
		*o = *m.nodepool
		return nil
	case *privatev1.Cluster:
		if m.clsGetErr != nil {
			return m.clsGetErr
		}
		if m.cluster == nil {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "cluster"}, "")
		}
		*o = *m.cluster
		return nil
	default:
		return fmt.Errorf("unexpected type %T", obj)
	}
}

func (m *mockStoreClient) Status() client.SubResourceWriter { return m.statusWriter }

func (m *mockStoreClient) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	return nil
}
func (m *mockStoreClient) Create(_ context.Context, _ client.Object, _ ...client.CreateOption) error {
	return nil
}
func (m *mockStoreClient) Delete(_ context.Context, _ client.Object, _ ...client.DeleteOption) error {
	return nil
}
func (m *mockStoreClient) Update(_ context.Context, _ client.Object, _ ...client.UpdateOption) error {
	return nil
}
func (m *mockStoreClient) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.PatchOption) error {
	return nil
}
func (m *mockStoreClient) DeleteAllOf(_ context.Context, _ client.Object, _ ...client.DeleteAllOfOption) error {
	return nil
}
func (m *mockStoreClient) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.ApplyOption) error {
	return nil
}
func (m *mockStoreClient) SubResource(_ string) client.SubResourceClient { return nil }
func (m *mockStoreClient) Scheme() *runtime.Scheme                       { return nil }
func (m *mockStoreClient) RESTMapper() meta.RESTMapper                   { return nil }
func (m *mockStoreClient) GroupVersionKindFor(_ runtime.Object) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, nil
}
func (m *mockStoreClient) IsObjectNamespaced(_ runtime.Object) (bool, error) { return false, nil }

// errTransport is a transport.Client that returns configurable errors.
type errTransport struct {
	applyErr        error
	getStatusErr    error
	getStatusResult *transport.ManifestWorkStatus
}

func (e *errTransport) Apply(_ context.Context, _ string, _ *workv1.ManifestWork) error {
	return e.applyErr
}
func (e *errTransport) GetStatus(_ context.Context, _, _ string) (*transport.ManifestWorkStatus, error) {
	return e.getStatusResult, e.getStatusErr
}
func (e *errTransport) Delete(_ context.Context, _, _ string) error { return nil }

// npReq returns a reconcile.Request for the given clusterID/nodepoolID pair.
func npReq(clusterID, nodepoolID string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: clusterID, Name: nodepoolID},
	}
}

// conflictErr returns a Kubernetes conflict error.
func conflictErr() error {
	return apierrors.NewConflict(schema.GroupResource{Resource: "nodepools"}, "test", fmt.Errorf("conflict"))
}

// testNodePool creates a NodePool. If vrVersion is non-empty, the spec and VR status are set.
func testNodePool(vrVersion string) *privatev1.NodePool {
	np := &privatev1.NodePool{}
	np.SetName("np-test")
	np.SetNamespace("cluster-test")
	np.Spec = privatev1.NodePoolSpec{
		ClusterID: "cluster-test",
		Platform: privatev1.NodePoolPlatformSpec{
			Type: "GCP",
			GCP: &privatev1.GCPNodePoolPlatform{
				MachineType: "n2-standard-4",
				DiskSizeGB:  100,
				DiskType:    "pd-ssd",
				Zone:        "us-central1-b",
			},
		},
	}
	if vrVersion != "" {
		np.Spec.Release = privatev1.ReleaseSpec{Version: vrVersion}
		np.Status.VersionResolution = &privatev1.VersionResolutionResult{
			ReleaseImage:   "quay.io/openshift-release-dev/ocp-release:4.16.0-x86_64",
			ReleaseVersion: vrVersion,
		}
	}
	return np
}

// testCluster creates a Cluster with placement and optionally the HC Available condition.
func testCluster(placementReady, hcAvailable bool) *privatev1.Cluster {
	c := &privatev1.Cluster{}
	c.SetName("cluster-test")
	c.SetNamespace("hyperfleet")
	c.Spec = privatev1.ClusterSpec{
		Platform: privatev1.ClusterPlatformSpec{
			Type: "GCP",
			GCP: &privatev1.GCPClusterPlatform{
				Subnet: "my-subnet",
				Region: "us-central1",
			},
		},
	}
	if placementReady {
		c.Status.PlacementResult = &privatev1.PlacementResult{
			ManagementClusterName: "mc-us-c1",
			BaseDomain:            "hc.example.com",
		}
	}
	if hcAvailable {
		c.Status.Conditions = []metav1.Condition{
			{Type: "HostedClusterAvailable", Status: metav1.ConditionTrue, Reason: "Available"},
		}
	}
	return c
}

// buildReconciler wires up a nodepool Reconciler with injectable errors.
func buildReconciler(
	t *testing.T,
	np *privatev1.NodePool,
	cluster *privatev1.Cluster,
	tr transport.Client,
	npGetErr, clsGetErr, statusErr error,
) (*Reconciler, *mockStoreClient) {
	t.Helper()
	storeClient := &mockStoreClient{
		nodepool:     np,
		cluster:      cluster,
		npGetErr:     npGetErr,
		clsGetErr:    clsGetErr,
		statusWriter: &mockStatusWriter{updateErr: statusErr},
	}
	return New(tr, newTestLogger(t), storeClient), storeClient
}

// ---------------------------------------------------------------------------
// Test cases – early exits: NodePool get
// ---------------------------------------------------------------------------

func TestReconcile_NodePoolNotFound(t *testing.T) {
	tr := mock.New()
	r, _ := buildReconciler(t, nil, nil, tr, nil, nil, nil) // nil nodepool → NotFound

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-missing"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_NodePoolGetError(t *testing.T) {
	tr := mock.New()
	r, _ := buildReconciler(t, nil, nil, tr, fmt.Errorf("etcd timeout"), nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "get nodepool")
}

// ---------------------------------------------------------------------------
// Test cases – early exits: Cluster get
// ---------------------------------------------------------------------------

func TestReconcile_ClusterNotFound(t *testing.T) {
	np := testNodePool("4.16.0")

	tr := mock.New()
	r, _ := buildReconciler(t, np, nil, tr, nil, nil, nil) // nil cluster → NotFound

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_ClusterGetError(t *testing.T) {
	np := testNodePool("4.16.0")

	tr := mock.New()
	r, _ := buildReconciler(t, np, nil, tr, nil, fmt.Errorf("etcd timeout"), nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "get cluster")
}

// ---------------------------------------------------------------------------
// Test cases – early exits: placement gate
// ---------------------------------------------------------------------------

func TestReconcile_NoPlacement(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(false, true) // placement not ready

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_NoPlacement_StatusUpdateConflict(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(false, true)

	tr := mock.New()
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, conflictErr())

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
}

func TestReconcile_NoPlacement_StatusUpdateError(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(false, true)

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, fmt.Errorf("server error"))

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update nodepool status")
}

// ---------------------------------------------------------------------------
// Test cases – early exits: HC availability gate
// ---------------------------------------------------------------------------

func TestReconcile_HCNotAvailable(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, false) // HC not available

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_HCNotAvailable_StatusUpdateConflict(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, false)

	tr := mock.New()
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, conflictErr())

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
}

func TestReconcile_HCNotAvailable_StatusUpdateError(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, false)

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, fmt.Errorf("server error"))

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update nodepool status")
}

// ---------------------------------------------------------------------------
// Test cases – early exits: VR gates
// ---------------------------------------------------------------------------

func TestReconcile_NodePoolVRNotReady(t *testing.T) {
	np := testNodePool("") // no VR
	cluster := testCluster(true, true)

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_VRNotReady_StatusUpdateConflict(t *testing.T) {
	np := testNodePool("")
	cluster := testCluster(true, true)

	tr := mock.New()
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, conflictErr())

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
}

func TestReconcile_VRNotReady_StatusUpdateError(t *testing.T) {
	np := testNodePool("")
	cluster := testCluster(true, true)

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, fmt.Errorf("server error"))

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update nodepool status")
}

func TestReconcile_VRVersionMismatch(t *testing.T) {
	np := testNodePool("4.15.0")           // VR resolved to 4.15.0
	np.Spec.Release.Version = "4.16.0"    // but spec wants 4.16.0
	cluster := testCluster(true, true)

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_VRVersionMismatch_StatusUpdateConflict(t *testing.T) {
	np := testNodePool("4.15.0")
	np.Spec.Release.Version = "4.16.0"
	cluster := testCluster(true, true)

	tr := mock.New()
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, conflictErr())

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
}

func TestReconcile_VRVersionMismatch_StatusUpdateError(t *testing.T) {
	np := testNodePool("4.15.0")
	np.Spec.Release.Version = "4.16.0"
	cluster := testCluster(true, true)

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, fmt.Errorf("server error"))

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update nodepool status")
}

// ---------------------------------------------------------------------------
// Test cases – platform field defaults
// ---------------------------------------------------------------------------

// TestReconcile_DefaultPlatformValues verifies that when the NodePool has no GCP platform
// spec set, the reconciler applies default machine type, disk size, disk type, and derives
// the zone from the cluster's GCP region.
func TestReconcile_DefaultPlatformValues(t *testing.T) {
	np := testNodePool("4.16.0")
	np.Spec.Platform.GCP = nil // no GCP spec → all defaults apply

	cluster := testCluster(true, true) // cluster has GCP.Region = "us-central1"

	mwName := np.Name + "-" + adapterName

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: []map[string]string{
			{"readyCondition": "True", "allNodesHealthyCondition": "True"},
		},
	}

	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeueStable, result.RequeueAfter)
	require.Len(t, tr.ApplyCalls, 1)
	// Zone should be derived from region: "us-central1" → "us-central1-a"
	// MachineType/DiskSizeGB/DiskType should be defaults (validated inside manifest.Build).
}

// TestReconcile_ZoneDerivedFromRegion verifies that when the NodePool GCP spec exists
// but zone is empty, the zone is derived from the cluster's region.
func TestReconcile_ZoneDerivedFromRegion(t *testing.T) {
	np := testNodePool("4.16.0")
	np.Spec.Platform.GCP.Zone = "" // explicit empty zone → derived from cluster region

	cluster := testCluster(true, true) // cluster region = "us-central1"

	mwName := np.Name + "-" + adapterName

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: []map[string]string{
			{"readyCondition": "True", "allNodesHealthyCondition": "True"},
		},
	}

	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeueStable, result.RequeueAfter)
	require.Len(t, tr.ApplyCalls, 1)
}

// ---------------------------------------------------------------------------
// Test cases – transport errors
// ---------------------------------------------------------------------------

func TestReconcile_TransportApplyError(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	tr := &errTransport{applyErr: fmt.Errorf("maestro unavailable")}
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "apply manifest work")
}

func TestReconcile_TransportGetStatusError(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	tr := &errTransport{getStatusErr: fmt.Errorf("grpc unavailable")}
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "get manifest work status")
}

// TestReconcile_MWStatusNil_RequeuesPending verifies that a not-found from GetStatus maps
// to a nil mwStatus, sets both conditions to False, and requeues with the pending interval.
func TestReconcile_MWStatusNil_RequeuesPending(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	notFoundErr := apierrors.NewNotFound(
		schema.GroupResource{Group: "work.open-cluster-management.io", Resource: "manifestworks"},
		"np-test-nodepool-adapter",
	)
	tr := &errTransport{getStatusErr: notFoundErr}
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
}

// ---------------------------------------------------------------------------
// Test cases – happy path and condition-driven requeue
// ---------------------------------------------------------------------------

func TestReconcile_HappyPath(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	tr := mock.New()
	mwName := np.Name + "-" + adapterName
	tr.StatusOverrides["mc-us-c1/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: []map[string]string{
			{
				"readyCondition":           "True",
				"allNodesHealthyCondition": "True",
				"replicas":                 "2",
				"version":                  "4.16.0",
			},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeueStable, result.RequeueAfter)

	require.Len(t, tr.ApplyCalls, 1)
	require.Equal(t, "mc-us-c1", tr.ApplyCalls[0].TargetCluster)
	require.Equal(t, mwName, tr.ApplyCalls[0].Work.Name)
	require.True(t, storeClient.statusWriter.called, "expected Status().Update to be called")
}

// TestReconcile_MWNotApplied_RequeuesPending verifies that when Applied=False the
// reconciler requeues with the pending interval.
func TestReconcile_MWNotApplied_RequeuesPending(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	mwName := np.Name + "-" + adapterName
	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionFalse, Reason: "ApplyFailed"},
		},
		ResourceStatuses: []map[string]string{
			{"readyCondition": "False"},
		},
	}

	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)
}

// TestReconcile_StatusUpdateConflict_ReturnsNoError verifies that a conflict error on
// Status.Update after applyStatusConditions is silently swallowed.
func TestReconcile_StatusUpdateConflict_ReturnsNoError(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	mwName := np.Name + "-" + adapterName
	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: []map[string]string{
			{"readyCondition": "True", "allNodesHealthyCondition": "True"},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, conflictErr())

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter) // conflict → immediate return
	require.True(t, storeClient.statusWriter.called)
}

// TestReconcile_StatusUpdateError_ReturnsError verifies that a non-conflict error on
// Status.Update after applyStatusConditions is propagated.
func TestReconcile_StatusUpdateError_ReturnsError(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	mwName := np.Name + "-" + adapterName
	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, fmt.Errorf("server error"))

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update nodepool status")
	require.True(t, storeClient.statusWriter.called)
}
