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

// mockStatusWriter captures Status().Update calls.
type mockStatusWriter struct {
	called bool
}

func (m *mockStatusWriter) Update(_ context.Context, _ client.Object, _ ...client.SubResourceUpdateOption) error {
	m.called = true
	return nil
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

func (m *mockStoreClient) Status() client.SubResourceWriter {
	if m.statusWriter == nil {
		m.statusWriter = &mockStatusWriter{}
	}
	return m.statusWriter
}

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

// npReq returns a reconcile.Request for the given clusterID/nodepoolID pair.
func npReq(clusterID, nodepoolID string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: clusterID, Name: nodepoolID},
	}
}

// testNodePool creates a NodePool with VR ready.
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
				DiskSize:    100,
				DiskType:    "pd-ssd",
				Zones:       []string{"us-central1-b"},
			},
		},
	}
	if vrVersion != "" {
		np.Status.VersionResolution = &privatev1.VersionResolutionResult{
			ReleaseImage:   "quay.io/openshift-release-dev/ocp-release:4.16.0-x86_64",
			ReleaseVersion: vrVersion,
		}
	}
	return np
}

// testCluster creates a Cluster with placement ready and HC Available condition set.
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

// buildReconciler wires up a nodepool Reconciler.
func buildReconciler(
	t *testing.T,
	np *privatev1.NodePool,
	cluster *privatev1.Cluster,
	tr *mock.Client,
) (*Reconciler, *mockStoreClient) {
	t.Helper()
	storeClient := &mockStoreClient{nodepool: np, cluster: cluster}
	return New(tr, newTestLogger(t), storeClient), storeClient
}

// ---------------------------------------------------------------------------
// Test cases
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

	r, storeClient := buildReconciler(t, np, cluster, tr)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeueStable, result.RequeueAfter)

	require.Len(t, tr.ApplyCalls, 1)
	require.Equal(t, "mc-us-c1", tr.ApplyCalls[0].TargetCluster)
	require.Equal(t, mwName, tr.ApplyCalls[0].Work.Name)
	require.NotNil(t, storeClient.statusWriter)
	require.True(t, storeClient.statusWriter.called, "expected Status().Update to be called")
}

func TestReconcile_NoPlacement(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(false, true) // placement not ready

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_HCNotAvailable(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, false) // HC not available

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_NodePoolVRNotReady(t *testing.T) {
	np := testNodePool("") // no VR
	cluster := testCluster(true, true)

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_NodePoolNotFound(t *testing.T) {
	tr := mock.New()
	r, _ := buildReconciler(t, nil, nil, tr) // nil nodepool → NotFound

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-missing"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}
