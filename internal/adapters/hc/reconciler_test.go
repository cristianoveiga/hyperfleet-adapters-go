package hc_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/hc"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/common/hyperfleetapi"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/hyperfleetstore"
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

// mockStoreClient is a minimal client.Client backed by a fixed HyperFleetCluster.
type mockStoreClient struct {
	cluster *hyperfleetstore.HyperFleetCluster
	getErr  error
}

func (m *mockStoreClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if m.getErr != nil {
		return m.getErr
	}
	if m.cluster == nil {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "cluster"}, "")
	}
	c, ok := obj.(*hyperfleetstore.HyperFleetCluster)
	if !ok {
		return fmt.Errorf("unexpected type %T", obj)
	}
	*c = *m.cluster
	return nil
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
func (m *mockStoreClient) Status() client.SubResourceWriter              { return nil }
func (m *mockStoreClient) Scheme() *runtime.Scheme                       { return nil }
func (m *mockStoreClient) RESTMapper() meta.RESTMapper                   { return nil }
func (m *mockStoreClient) GroupVersionKindFor(_ runtime.Object) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, nil
}
func (m *mockStoreClient) IsObjectNamespaced(_ runtime.Object) (bool, error) { return false, nil }

// clusterReq returns a reconcile.Request for the given cluster name.
func clusterReq(name string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: hyperfleetstore.ClusterNamespace, Name: name},
	}
}

// readyStatuses returns AdapterStatuses with placement and VR data ready.
func readyStatuses(version string) hyperfleetapi.AdapterStatuses {
	return hyperfleetapi.AdapterStatuses{
		{
			Adapter: "placement-adapter",
			Data: map[string]any{
				"managementClusterName": "mc-cluster-1",
				"baseDomain":            "example.com",
			},
		},
		{
			Adapter: "version-resolution-adapter",
			Data: map[string]any{
				"release_image":   "quay.io/openshift-release-dev/ocp-release:4.15.0-x86_64",
				"release_version": version,
				"release_channel": "stable-4.15",
			},
		},
	}
}

// buildCluster creates a HyperFleetCluster for tests.
func buildCluster(clusterID string, conditions []hyperfleetapi.Condition, statuses hyperfleetapi.AdapterStatuses) *hyperfleetstore.HyperFleetCluster {
	c := &hyperfleetstore.HyperFleetCluster{
		DisplayName:  "my-cluster",
		HFGeneration: 2,
		CreatedBy:    "alice@redhat.com",
		Spec: hyperfleetapi.ClusterSpec{
			InfraID:   "infra-xyz",
			IssuerURL: "https://issuer.example.com",
			ClusterID: "550e8400-e29b-41d4-a716-446655440000",
			Release:   hyperfleetapi.ReleaseSpec{Version: "4.15.0"},
			Platform: hyperfleetapi.GCPPlatform{
				Type: "GCP",
				GCP: hyperfleetapi.GCPConfig{
					ProjectID: "my-project",
					Region:    "us-central1",
					Network:   "my-vpc",
					Subnet:    "my-subnet",
					WorkloadIdentity: hyperfleetapi.WIFConfig{
						ProjectNumber: "12345",
						PoolID:        "pool",
						ProviderID:    "provider",
						ServiceAccountsRef: hyperfleetapi.WIFServiceAccounts{
							NodePool:        "np@sa.iam.gserviceaccount.com",
							ControlPlane:    "cp@sa.iam.gserviceaccount.com",
							CloudController: "cc@sa.iam.gserviceaccount.com",
							Storage:         "st@sa.iam.gserviceaccount.com",
							ImageRegistry:   "ir@sa.iam.gserviceaccount.com",
							Network:         "nw@sa.iam.gserviceaccount.com",
						},
					},
				},
			},
		},
		Status: hyperfleetapi.ClusterStatus{
			Conditions: conditions,
		},
		AdapterStatuses: statuses,
	}
	c.SetName(clusterID)
	c.SetNamespace(hyperfleetstore.ClusterNamespace)
	return c
}

// buildReconciler wires up an hc.Reconciler backed by the store client and transport mock.
func buildReconciler(
	t *testing.T,
	cluster *hyperfleetstore.HyperFleetCluster,
	tr *mock.Client,
	putCapture *bool,
) *hc.Reconciler {
	t.Helper()

	hfSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			*putCapture = true
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(hfSrv.Close)

	apiClient := hyperfleetapi.New(hfSrv.URL, "v1", testLogger(t))
	storeClient := &mockStoreClient{cluster: cluster}
	return hc.New(apiClient, tr, testLogger(t), storeClient)
}

// TestReconcile_HappyPath verifies the full reconcile path when all dependencies are ready.
func TestReconcile_HappyPath(t *testing.T) {
	clusterID := "cluster-abc"
	mwName := clusterID + "-hc-adapter"
	mcName := "mc-cluster-1"

	cluster := buildCluster(clusterID, nil, readyStatuses("4.15.0"))

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

	var putCalled bool
	r := buildReconciler(t, cluster, tr, &putCalled)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, result.RequeueAfter)

	require.Len(t, tr.ApplyCalls, 1)
	require.Equal(t, mcName, tr.ApplyCalls[0].TargetCluster)
	require.Equal(t, mwName, tr.ApplyCalls[0].Work.Name)
	require.True(t, putCalled, "expected PutClusterStatus to be called")
}

// TestReconcile_DependenciesNotReady_NoPlacement verifies requeue when placement is missing.
func TestReconcile_DependenciesNotReady_NoPlacement(t *testing.T) {
	clusterID := "cluster-abc"
	statusesNoPlacement := hyperfleetapi.AdapterStatuses{
		{
			Adapter: "version-resolution-adapter",
			Data: map[string]any{
				"release_image":   "quay.io/openshift-release-dev/ocp-release:4.15.0-x86_64",
				"release_version": "4.15.0",
				"release_channel": "stable-4.15",
			},
		},
	}
	cluster := buildCluster(clusterID, nil, statusesNoPlacement)

	tr := mock.New()
	var putCalled bool
	r := buildReconciler(t, cluster, tr, &putCalled)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

// TestReconcile_DependenciesNotReady_VRVersionMismatch verifies requeue when VR version doesn't match.
func TestReconcile_DependenciesNotReady_VRVersionMismatch(t *testing.T) {
	clusterID := "cluster-abc"
	// Cluster wants 4.15.0 but VR resolved 4.14.9.
	cluster := buildCluster(clusterID, nil, readyStatuses("4.14.9"))

	tr := mock.New()
	var putCalled bool
	r := buildReconciler(t, cluster, tr, &putCalled)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

// TestReconcile_ClusterNotFound verifies that a 404 returns empty Result with no error.
func TestReconcile_ClusterNotFound(t *testing.T) {
	tr := mock.New()
	var putCalled bool
	r := buildReconciler(t, nil, tr, &putCalled) // nil cluster → NotFound

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-missing"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

// TestReconcile_AlreadyReconciled verifies that a cluster with Reconciled=True is skipped.
func TestReconcile_AlreadyReconciled(t *testing.T) {
	clusterID := "cluster-abc"
	reconciledConditions := []hyperfleetapi.Condition{
		{Type: "Reconciled", Status: "True", Reason: "Done"},
	}
	cluster := buildCluster(clusterID, reconciledConditions, readyStatuses("4.15.0"))

	tr := mock.New()
	var putCalled bool
	r := buildReconciler(t, cluster, tr, &putCalled)

	_, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Empty(t, tr.ApplyCalls)
}
