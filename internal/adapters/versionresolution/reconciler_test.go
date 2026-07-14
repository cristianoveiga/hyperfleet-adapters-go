package versionresolution

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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/common/hyperfleetapi"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/hyperfleetstore"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

// ---- helpers ----------------------------------------------------------------

func newTestLogger(t *testing.T) logger.Logger {
	t.Helper()
	log, err := logger.NewLogger(logger.Config{
		Level:     "error",
		Format:    logger.FormatText,
		Component: "test",
		Version:   "test",
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

// newMockCincinnati builds a simple httptest server that returns a Cincinnati
// graph containing the given release, or an empty graph if release is nil.
func newMockCincinnati(release *ReleaseInfo) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		graph := CincinnatiGraph{}
		if release != nil {
			graph.Nodes = []ReleaseInfo{*release}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(graph) //nolint:errcheck
	}))
}

// clusterReq returns a reconcile.Request for the given cluster name.
func clusterReq(name string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: hyperfleetstore.ClusterNamespace, Name: name},
	}
}

// buildReconciler wires up a Reconciler backed by the store client, with the
// given Cincinnati mock server and a capture channel for PUT payloads.
func buildReconciler(
	t *testing.T,
	cluster *hyperfleetstore.HyperFleetCluster,
	cincSrv *httptest.Server,
	putCapture *[]hyperfleetapi.StatusPayload,
) *Reconciler {
	t.Helper()

	hfSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			var payload hyperfleetapi.StatusPayload
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			*putCapture = append(*putCapture, payload)
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(hfSrv.Close)

	hfClient := hyperfleetapi.New(hfSrv.URL, "v1", newTestLogger(t))
	cincClient := NewCincinnatiClient(cincSrv.URL, "amd64")
	storeClient := &mockStoreClient{cluster: cluster}
	return NewReconciler(hfClient, cincClient, newTestLogger(t), storeClient)
}

// ---- tests ------------------------------------------------------------------

func TestReconciler_HappyPath(t *testing.T) {
	release := &ReleaseInfo{
		Version: "4.22.0-ec.4",
		Payload: "quay.io/openshift-release-dev/ocp-release:4.22.0-ec.4-x86_64",
	}
	cincSrv := newMockCincinnati(release)
	defer cincSrv.Close()

	cluster := &hyperfleetstore.HyperFleetCluster{
		Spec: hyperfleetapi.ClusterSpec{
			Release: hyperfleetapi.ReleaseSpec{Version: "4.22.0-ec.4"},
		},
		AdapterStatuses: hyperfleetapi.AdapterStatuses{}, // not yet resolved
	}
	cluster.SetName("cluster-1")
	cluster.SetNamespace(hyperfleetstore.ClusterNamespace)
	cluster.SetGeneration(3)

	var puts []hyperfleetapi.StatusPayload
	r := buildReconciler(t, cluster, cincSrv, &puts)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{RequeueAfter: requeueLong}, result)
	require.Len(t, puts, 1, "expected one PUT")

	put := puts[0]
	require.Equal(t, adapterName, put.Adapter)
	require.Equal(t, int64(3), put.ObservedGeneration)
	require.Equal(t, release.Payload, put.Data["release_image"])
	require.Equal(t, "4.22.0-ec.4", put.Data["release_version"])
	require.Equal(t, "candidate-4.22", put.Data["release_channel"])
	require.Equal(t, "candidate", put.Data["release_channel_group"])
	require.Len(t, put.Conditions, 3)

	// Verify ObservedTime is a valid RFC3339 timestamp.
	_, parseErr := time.Parse(time.RFC3339, put.ObservedTime)
	require.NoError(t, parseErr)
}

func TestReconciler_AlreadyResolved(t *testing.T) {
	cincSrv := newMockCincinnati(nil)
	defer cincSrv.Close()

	cluster := &hyperfleetstore.HyperFleetCluster{
		Spec: hyperfleetapi.ClusterSpec{
			Release: hyperfleetapi.ReleaseSpec{Version: "4.22.0-ec.4"},
		},
		AdapterStatuses: hyperfleetapi.AdapterStatuses{
			{
				Adapter: adapterName,
				Data: map[string]any{
					"release_image":         "quay.io/openshift-release-dev/ocp-release:4.22.0-ec.4-x86_64",
					"release_version":       "4.22.0-ec.4",
					"release_channel":       "candidate-4.22",
					"release_channel_group": "candidate",
				},
			},
		},
	}
	cluster.SetName("cluster-2")
	cluster.SetNamespace(hyperfleetstore.ClusterNamespace)

	var puts []hyperfleetapi.StatusPayload
	r := buildReconciler(t, cluster, cincSrv, &puts)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-2"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
	require.Empty(t, puts)
}

func TestReconciler_ClusterNotFound(t *testing.T) {
	cincSrv := newMockCincinnati(nil)
	defer cincSrv.Close()

	var puts []hyperfleetapi.StatusPayload
	r := buildReconciler(t, nil, cincSrv, &puts) // nil cluster → NotFound

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-404"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
	require.Empty(t, puts)
}

func TestReconciler_ReconciledCluster(t *testing.T) {
	cincSrv := newMockCincinnati(nil)
	defer cincSrv.Close()

	cluster := &hyperfleetstore.HyperFleetCluster{
		Spec: hyperfleetapi.ClusterSpec{
			Release: hyperfleetapi.ReleaseSpec{Version: "4.22.0-ec.4"},
		},
		Status: hyperfleetapi.ClusterStatus{
			Conditions: []hyperfleetapi.Condition{
				{Type: "Reconciled", Status: "True", Reason: "Done"},
			},
		},
		AdapterStatuses: hyperfleetapi.AdapterStatuses{},
	}
	cluster.SetName("cluster-3")
	cluster.SetNamespace(hyperfleetstore.ClusterNamespace)

	var puts []hyperfleetapi.StatusPayload
	r := buildReconciler(t, cluster, cincSrv, &puts)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-3"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
	require.Empty(t, puts)
}

func TestReconciler_VersionNotInCincinnati(t *testing.T) {
	// Cincinnati returns an empty graph (no matching node).
	cincSrv := newMockCincinnati(nil)
	defer cincSrv.Close()

	cluster := &hyperfleetstore.HyperFleetCluster{
		Spec: hyperfleetapi.ClusterSpec{
			Release: hyperfleetapi.ReleaseSpec{Version: "4.22.0-ec.4"},
		},
		AdapterStatuses: hyperfleetapi.AdapterStatuses{},
	}
	cluster.SetName("cluster-5")
	cluster.SetNamespace(hyperfleetstore.ClusterNamespace)

	var puts []hyperfleetapi.StatusPayload
	r := buildReconciler(t, cluster, cincSrv, &puts)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-5"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
	require.Empty(t, puts)
}

func TestBuildChannel(t *testing.T) {
	cases := []struct {
		version string
		want    string
	}{
		{"4.22.0-ec.4", "candidate-4.22"},
		{"4.16.3", "candidate-4.16"},
		{"4.15.0", "candidate-4.15"},
	}
	for _, tc := range cases {
		t.Run(tc.version, func(t *testing.T) {
			require.Equal(t, tc.want, buildChannel(tc.version))
		})
	}
}
