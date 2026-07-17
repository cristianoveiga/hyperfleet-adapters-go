package hc_test

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

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/hc"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/transport"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/transport/mock"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

// testLogger returns a logger for tests.
func testLogger(t *testing.T) logger.Logger {
	t.Helper()
	log, err := logger.NewLogger(logger.Config{
		Level:     "error",
		Format:    "text",
		Output:    "stderr",
		Component: "test",
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

// mockStoreClient is a minimal client.Client backed by a fixed Cluster.
type mockStoreClient struct {
	cluster      *privatev1.Cluster
	getErr       error
	statusWriter *mockStatusWriter
}

func (m *mockStoreClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if m.getErr != nil {
		return m.getErr
	}
	if m.cluster == nil {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "cluster"}, "")
	}
	c, ok := obj.(*privatev1.Cluster)
	if !ok {
		return fmt.Errorf("unexpected type %T", obj)
	}
	*c = *m.cluster
	return nil
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

// clusterReq returns a reconcile.Request for the given cluster name.
func clusterReq(name string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: "hyperfleet", Name: name},
	}
}

// buildReadyCluster creates a Cluster with placement and VR results set.
func buildReadyCluster(clusterID, version string) *privatev1.Cluster {
	c := &privatev1.Cluster{}
	c.SetName(clusterID)
	c.SetNamespace("hyperfleet")
	c.SetGeneration(2)
	c.Spec = privatev1.ClusterSpec{
		InfraID: "infra-xyz",
		Release: privatev1.ReleaseSpec{Version: version},
		Platform: privatev1.ClusterPlatformSpec{
			Type: "GCP",
			GCP: &privatev1.GCPClusterPlatform{
				ProjectID: "my-project",
				Region:    "us-central1",
				Network:   "my-vpc",
				Subnet:    "my-subnet",
			},
		},
	}
	c.Status = privatev1.ClusterStatus{
		PlacementResult: &privatev1.PlacementResult{
			ManagementClusterName: "mc-cluster-1",
			BaseDomain:            "example.com",
		},
		VersionResolution: &privatev1.VersionResolutionResult{
			ReleaseImage:   "quay.io/openshift-release-dev/ocp-release:4.15.0-x86_64",
			ReleaseVersion: version,
			ReleaseChannel: "stable-4.15",
		},
	}
	return c
}

// buildReconciler wires up an hc.Reconciler backed by the store client and transport mock.
func buildReconciler(
	t *testing.T,
	cluster *privatev1.Cluster,
	tr *mock.Client,
) (*hc.Reconciler, *mockStoreClient) {
	t.Helper()
	storeClient := &mockStoreClient{cluster: cluster}
	return hc.New(tr, testLogger(t), storeClient), storeClient
}

// TestReconcile_HappyPath verifies the full reconcile path when all dependencies are ready.
func TestReconcile_HappyPath(t *testing.T) {
	clusterID := "cluster-abc"
	mwName := clusterID + "-hc-adapter"
	mcName := "mc-cluster-1"

	cluster := buildReadyCluster(clusterID, "4.15.0")

	tr := mock.New()
	tr.StatusOverrides[mcName+"/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Now()},
		},
		ResourceStatuses: []map[string]string{
			{}, {}, {}, // indices 0-2
			{"availableCondition": "True", "degradedCondition": "False"}, // index 3 = HC
		},
	}

	r, storeClient := buildReconciler(t, cluster, tr)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, result.RequeueAfter)

	require.Len(t, tr.ApplyCalls, 1)
	require.Equal(t, mcName, tr.ApplyCalls[0].TargetCluster)
	require.Equal(t, mwName, tr.ApplyCalls[0].Work.Name)
	require.NotNil(t, storeClient.statusWriter)
	require.True(t, storeClient.statusWriter.called, "expected Status().Update to be called")
}

// TestReconcile_DependenciesNotReady_NoPlacement verifies requeue when placement is missing.
func TestReconcile_DependenciesNotReady_NoPlacement(t *testing.T) {
	clusterID := "cluster-abc"
	cluster := &privatev1.Cluster{}
	cluster.SetName(clusterID)
	cluster.SetNamespace("hyperfleet")
	cluster.Status.VersionResolution = &privatev1.VersionResolutionResult{
		ReleaseImage:   "quay.io/openshift-release-dev/ocp-release:4.15.0-x86_64",
		ReleaseVersion: "4.15.0",
	}
	// PlacementResult is nil

	tr := mock.New()
	r, _ := buildReconciler(t, cluster, tr)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

// TestReconcile_DependenciesNotReady_VRVersionMismatch verifies requeue when VR version doesn't match.
func TestReconcile_DependenciesNotReady_VRVersionMismatch(t *testing.T) {
	clusterID := "cluster-abc"
	// Cluster wants 4.15.0 but VR resolved 4.14.9.
	cluster := buildReadyCluster(clusterID, "4.14.9")
	cluster.Spec.Release = privatev1.ReleaseSpec{Version: "4.15.0"}

	tr := mock.New()
	r, _ := buildReconciler(t, cluster, tr)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

// TestReconcile_ClusterNotFound verifies that a 404 returns empty Result with no error.
func TestReconcile_ClusterNotFound(t *testing.T) {
	tr := mock.New()
	r, _ := buildReconciler(t, nil, tr) // nil cluster → NotFound

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-missing"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}
