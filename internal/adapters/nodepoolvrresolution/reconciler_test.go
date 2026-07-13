package nodepoolvrresolution

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

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/versionresolution"
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

// mockStoreClient is a minimal client.Client backed by a fixed HyperFleetNodePool
// and HyperFleetCluster. Get dispatches by object type.
type mockStoreClient struct {
	nodepool   *hyperfleetstore.HyperFleetNodePool
	cluster    *hyperfleetstore.HyperFleetCluster
	npGetErr   error
	clsGetErr  error
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

// newMockCincinnati builds a simple httptest server that returns a Cincinnati
// graph containing the given release, or an empty graph if release is nil.
func newMockCincinnati(release *versionresolution.ReleaseInfo) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		type graph struct {
			Nodes []versionresolution.ReleaseInfo `json:"nodes"`
		}
		g := graph{}
		if release != nil {
			g.Nodes = []versionresolution.ReleaseInfo{*release}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(g) //nolint:errcheck
	}))
}

// npReq returns a reconcile.Request for the given clusterID/nodepoolID pair.
func npReq(clusterID, nodepoolID string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: clusterID, Name: nodepoolID},
	}
}

// buildReconciler wires up a Reconciler backed by the store client.
func buildReconciler(
	t *testing.T,
	np *hyperfleetstore.HyperFleetNodePool,
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
	cincClient := versionresolution.NewCincinnatiClient(cincSrv.URL, "amd64")
	storeClient := &mockStoreClient{nodepool: np, cluster: cluster}
	return NewReconciler(hfClient, cincClient, newTestLogger(t), storeClient, &noopStore{})
}

// ---- tests ------------------------------------------------------------------

func TestReconciler_HappyPath(t *testing.T) {
	release := &versionresolution.ReleaseInfo{
		Version: "4.22.0-ec.4",
		Payload: "quay.io/openshift-release-dev/ocp-release:4.22.0-ec.4-x86_64",
	}
	cincSrv := newMockCincinnati(release)
	defer cincSrv.Close()

	np := &hyperfleetstore.HyperFleetNodePool{
		ClusterID:    "cluster-1",
		HFGeneration: 3,
		Spec: hyperfleetapi.NodePoolSpec{
			Release: hyperfleetapi.ReleaseSpec{Version: "4.22.0-ec.4"},
		},
		AdapterStatuses: hyperfleetapi.AdapterStatuses{}, // not yet resolved
	}
	np.SetName("np-1")
	np.SetNamespace("cluster-1")

	cluster := &hyperfleetstore.HyperFleetCluster{}
	cluster.SetName("cluster-1")
	cluster.SetNamespace(hyperfleetstore.ClusterNamespace)

	var puts []hyperfleetapi.StatusPayload
	r := buildReconciler(t, np, cluster, cincSrv, &puts)

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

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

	require.Equal(t, "Applied", put.Conditions[0].Type)
	require.Equal(t, "True", put.Conditions[0].Status)
	require.Equal(t, "VersionResolved", put.Conditions[0].Reason)

	require.Equal(t, "Available", put.Conditions[1].Type)
	require.Equal(t, "True", put.Conditions[1].Status)
	require.Equal(t, "ReleaseImageAvailable", put.Conditions[1].Reason)

	require.Equal(t, "Health", put.Conditions[2].Type)
	require.Equal(t, "True", put.Conditions[2].Status)
	require.Equal(t, "VersionResolved", put.Conditions[2].Reason)

	_, parseErr := time.Parse(time.RFC3339, put.ObservedTime)
	require.NoError(t, parseErr)
}

func TestReconciler_AlreadyResolved(t *testing.T) {
	cincSrv := newMockCincinnati(nil)
	defer cincSrv.Close()

	np := &hyperfleetstore.HyperFleetNodePool{
		ClusterID: "cluster-1",
		Spec: hyperfleetapi.NodePoolSpec{
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
	np.SetName("np-2")
	np.SetNamespace("cluster-1")

	cluster := &hyperfleetstore.HyperFleetCluster{}
	cluster.SetName("cluster-1")
	cluster.SetNamespace(hyperfleetstore.ClusterNamespace)

	var puts []hyperfleetapi.StatusPayload
	r := buildReconciler(t, np, cluster, cincSrv, &puts)

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-2"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
	require.Empty(t, puts)
}

func TestReconciler_NodepoolNotFound(t *testing.T) {
	cincSrv := newMockCincinnati(nil)
	defer cincSrv.Close()

	var puts []hyperfleetapi.StatusPayload
	r := buildReconciler(t, nil, nil, cincSrv, &puts) // nil nodepool → NotFound

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-404"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
	require.Empty(t, puts)
}

func TestReconciler_VersionNotInCincinnati(t *testing.T) {
	// Cincinnati returns an empty graph (no matching node).
	cincSrv := newMockCincinnati(nil)
	defer cincSrv.Close()

	np := &hyperfleetstore.HyperFleetNodePool{
		ClusterID: "cluster-1",
		Spec: hyperfleetapi.NodePoolSpec{
			Release: hyperfleetapi.ReleaseSpec{Version: "4.22.0-ec.4"},
		},
		AdapterStatuses: hyperfleetapi.AdapterStatuses{},
	}
	np.SetName("np-5")
	np.SetNamespace("cluster-1")

	cluster := &hyperfleetstore.HyperFleetCluster{}
	cluster.SetName("cluster-1")
	cluster.SetNamespace(hyperfleetstore.ClusterNamespace)

	var puts []hyperfleetapi.StatusPayload
	r := buildReconciler(t, np, cluster, cincSrv, &puts)

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-5"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
	require.Empty(t, puts)
}

func TestReconciler_EmptyVersion(t *testing.T) {
	cincSrv := newMockCincinnati(nil)
	defer cincSrv.Close()

	np := &hyperfleetstore.HyperFleetNodePool{
		ClusterID: "cluster-1",
		Spec: hyperfleetapi.NodePoolSpec{
			Release: hyperfleetapi.ReleaseSpec{Version: ""}, // empty
		},
		AdapterStatuses: hyperfleetapi.AdapterStatuses{},
	}
	np.SetName("np-6")
	np.SetNamespace("cluster-1")

	cluster := &hyperfleetstore.HyperFleetCluster{}
	cluster.SetName("cluster-1")
	cluster.SetNamespace(hyperfleetstore.ClusterNamespace)

	var puts []hyperfleetapi.StatusPayload
	r := buildReconciler(t, np, cluster, cincSrv, &puts)

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-6"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
	require.Empty(t, puts)
}

func TestBuildChannel(t *testing.T) {
	cases := []struct {
		version      string
		channelGroup string
		want         string
		wantErr      bool
	}{
		{"4.22.0-ec.4", "candidate", "candidate-4.22", false},
		{"4.16.3", "candidate", "candidate-4.16", false},
		{"4.15.0", "candidate", "candidate-4.15", false},
		{"4", "candidate", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.version, func(t *testing.T) {
			got, err := buildChannel(tc.version, tc.channelGroup)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
}
