package nodepool

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/common/hyperfleetapi"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/hyperfleetstore"
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

// mockStoreClient is a minimal client.Client backed by a fixed HyperFleetNodePool
// and HyperFleetCluster. Get dispatches by object type.
type mockStoreClient struct {
	nodepool  *hyperfleetstore.HyperFleetNodePool
	cluster   *hyperfleetstore.HyperFleetCluster
	npGetErr  error
	clsGetErr error
}

func (m *mockStoreClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	switch o := obj.(type) {
	case *hyperfleetstore.HyperFleetNodePool:
		if m.npGetErr != nil {
			return m.npGetErr
		}
		if m.nodepool == nil {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "nodepool"}, "")
		}
		*o = *m.nodepool
		return nil
	case *hyperfleetstore.HyperFleetCluster:
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

// noopStore satisfies the store.TriggerRepoll dependency.
type noopStore struct{}

func (n *noopStore) TriggerRepoll(_ string) {}

// npReq returns a reconcile.Request for the given clusterID/nodepoolID pair.
func npReq(clusterID, nodepoolID string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: clusterID, Name: nodepoolID},
	}
}

// writeJSON writes v as JSON with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v) //nolint:errcheck
	}
}

// buildReadyStatuses creates cluster and nodepool statuses that pass all gates.
func buildReadyStatuses(specVersion string) (hyperfleetapi.AdapterStatuses, hyperfleetapi.AdapterStatuses) {
	clusterStatuses := hyperfleetapi.AdapterStatuses{
		{
			Adapter: "placement-adapter",
			Data: map[string]any{
				"managementClusterName": "mc-us-c1",
				"baseDomain":            "hc.example.com",
			},
		},
		{
			Adapter: "hc-adapter",
			Conditions: []hyperfleetapi.Condition{
				{Type: "Available", Status: "True"},
			},
		},
	}
	nodepoolStatuses := hyperfleetapi.AdapterStatuses{
		{
			Adapter: "nodepool-vr-adapter",
			Data: map[string]any{
				"release_image":   "quay.io/openshift-release-dev/ocp-release:4.16.0-x86_64",
				"release_version": specVersion,
			},
		},
	}
	return clusterStatuses, nodepoolStatuses
}

func testNodePool(specVersion string, clusterStatuses, nodepoolStatuses hyperfleetapi.AdapterStatuses) *hyperfleetstore.HyperFleetNodePool {
	np := &hyperfleetstore.HyperFleetNodePool{
		ClusterID:    "cluster-test",
		DisplayName:  "my-nodepool",
		HFGeneration: 5,
		Spec: hyperfleetapi.NodePoolSpec{
			Release: hyperfleetapi.ReleaseSpec{Version: specVersion},
			Platform: hyperfleetapi.NodePoolGCPPlatform{
				Type: "GCP",
				GCP: hyperfleetapi.NodePoolGCPConf{
					ProjectID: "my-project",
					Region:    "us-central1",
					Zone:      "us-central1-b",
				},
			},
		},
		AdapterStatuses: nodepoolStatuses,
	}
	np.SetName("np-test")
	np.SetNamespace("cluster-test")
	return np
}

func testCluster(clusterStatuses hyperfleetapi.AdapterStatuses) *hyperfleetstore.HyperFleetCluster {
	c := &hyperfleetstore.HyperFleetCluster{
		DisplayName: "my-cluster",
		Spec: hyperfleetapi.ClusterSpec{
			Platform: hyperfleetapi.GCPPlatform{
				Type: "GCP",
				GCP: hyperfleetapi.GCPConfig{
					Subnet: "my-subnet",
					Region: "us-central1",
				},
			},
		},
		AdapterStatuses: clusterStatuses,
	}
	c.SetName("cluster-test")
	c.SetNamespace(hyperfleetstore.ClusterNamespace)
	return c
}

// buildReconciler wires up a nodepool Reconciler with a store-backed client
// and a PUT-only HTTP server for the HyperFleet API.
func buildReconciler(
	t *testing.T,
	np *hyperfleetstore.HyperFleetNodePool,
	cluster *hyperfleetstore.HyperFleetCluster,
	tr *mock.Client,
	putCapture *bool,
) *Reconciler {
	t.Helper()

	hfSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			*putCapture = true
			w.WriteHeader(http.StatusOK)
			return
		}
		writeJSON(w, http.StatusNotFound, nil)
	}))
	t.Cleanup(hfSrv.Close)

	apiClient := hyperfleetapi.New(hfSrv.URL, "v1", newTestLogger(t))
	storeClient := &mockStoreClient{nodepool: np, cluster: cluster}
	return New(apiClient, tr, newTestLogger(t), storeClient, &noopStore{})
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

func TestReconcile_HappyPath(t *testing.T) {
	clusterStatuses, nodepoolStatuses := buildReadyStatuses("4.16.0")
	np := testNodePool("4.16.0", clusterStatuses, nodepoolStatuses)
	cluster := testCluster(clusterStatuses)

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

	var putCalled bool
	r := buildReconciler(t, np, cluster, tr, &putCalled)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeueAfterApply, result.RequeueAfter)

	require.Len(t, tr.ApplyCalls, 1)
	require.Equal(t, "mc-us-c1", tr.ApplyCalls[0].TargetCluster)
	require.Equal(t, mwName, tr.ApplyCalls[0].Work.Name)
	require.True(t, putCalled)
}

func TestReconcile_NoPlacement(t *testing.T) {
	// cluster statuses: no placement adapter
	clusterStatuses := hyperfleetapi.AdapterStatuses{
		{
			Adapter: "hc-adapter",
			Conditions: []hyperfleetapi.Condition{
				{Type: "Available", Status: "True"},
			},
		},
	}
	_, nodepoolStatuses := buildReadyStatuses("4.16.0")
	np := testNodePool("4.16.0", clusterStatuses, nodepoolStatuses)
	cluster := testCluster(clusterStatuses)

	tr := mock.New()
	var putCalled bool
	r := buildReconciler(t, np, cluster, tr, &putCalled)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_HCNotAvailable(t *testing.T) {
	clusterStatuses := hyperfleetapi.AdapterStatuses{
		{
			Adapter: "placement-adapter",
			Data: map[string]any{
				"managementClusterName": "mc-us-c1",
				"baseDomain":            "hc.example.com",
			},
		},
		{
			Adapter:    "hc-adapter",
			Conditions: []hyperfleetapi.Condition{}, // not Available
		},
	}
	_, nodepoolStatuses := buildReadyStatuses("4.16.0")
	np := testNodePool("4.16.0", clusterStatuses, nodepoolStatuses)
	cluster := testCluster(clusterStatuses)

	tr := mock.New()
	var putCalled bool
	r := buildReconciler(t, np, cluster, tr, &putCalled)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_NodePoolVRNotReady(t *testing.T) {
	clusterStatuses, _ := buildReadyStatuses("4.16.0")
	nodepoolStatuses := hyperfleetapi.AdapterStatuses{} // no nodepool-vr-adapter
	np := testNodePool("4.16.0", clusterStatuses, nodepoolStatuses)
	cluster := testCluster(clusterStatuses)

	tr := mock.New()
	var putCalled bool
	r := buildReconciler(t, np, cluster, tr, &putCalled)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_NodePoolNotFound(t *testing.T) {
	tr := mock.New()
	var putCalled bool
	r := buildReconciler(t, nil, nil, tr, &putCalled) // nil nodepool → NotFound

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-missing"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_VRVersionMismatch(t *testing.T) {
	// Spec says 4.16.0 but VR reports 4.15.0 → should skip
	clusterStatuses, _ := buildReadyStatuses("4.16.0")
	nodepoolStatuses := hyperfleetapi.AdapterStatuses{
		{
			Adapter: "nodepool-vr-adapter",
			Data: map[string]any{
				"release_image":   "quay.io/openshift-release-dev/ocp-release:4.15.0-x86_64",
				"release_version": "4.15.0", // mismatch
			},
		},
	}
	np := testNodePool("4.16.0", clusterStatuses, nodepoolStatuses)
	cluster := testCluster(clusterStatuses)

	tr := mock.New()
	var putCalled bool
	r := buildReconciler(t, np, cluster, tr, &putCalled)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}
