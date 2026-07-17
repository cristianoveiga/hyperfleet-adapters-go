package versionresolution

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	privatev1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1"

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

// mockStatusWriter captures status update calls.
type mockStatusWriter struct {
	updateErr error
	called    bool
}

func (m *mockStatusWriter) Update(_ context.Context, _ client.Object, _ ...client.SubResourceUpdateOption) error {
	m.called = true
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

// mockStoreClient is a minimal client.Client backed by a fixed Cluster.
type mockStoreClient struct {
	cluster      *privatev1.Cluster
	getErr       error
	updateCalled bool
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

func (m *mockStoreClient) Update(_ context.Context, _ client.Object, _ ...client.UpdateOption) error {
	m.updateCalled = true
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
		NamespacedName: client.ObjectKey{Namespace: "hyperfleet", Name: name},
	}
}

// buildReconciler wires up a Reconciler backed by the store client and Cincinnati mock.
func buildReconciler(
	t *testing.T,
	cluster *privatev1.Cluster,
	cincSrv *httptest.Server,
) (*Reconciler, *mockStoreClient) {
	t.Helper()
	storeClient := &mockStoreClient{cluster: cluster}
	cincClient := NewCincinnatiClient(cincSrv.URL, "amd64")
	return NewReconciler(cincClient, newTestLogger(t), storeClient), storeClient
}

// ---- tests ------------------------------------------------------------------

func TestReconciler_HappyPath(t *testing.T) {
	release := &ReleaseInfo{
		Version: "4.22.0-ec.4",
		Payload: "quay.io/openshift-release-dev/ocp-release:4.22.0-ec.4-x86_64",
	}
	cincSrv := newMockCincinnati(release)
	defer cincSrv.Close()

	cluster := &privatev1.Cluster{}
	cluster.SetName("cluster-1")
	cluster.SetNamespace("hyperfleet")
	cluster.SetGeneration(3)
	cluster.Spec.Release = privatev1.ReleaseSpec{Version: "4.22.0-ec.4"}

	r, storeClient := buildReconciler(t, cluster, cincSrv)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{RequeueAfter: requeueStable}, result)
	require.False(t, storeClient.updateCalled, "expected no spec Update (result written to status)")
	require.NotNil(t, storeClient.statusWriter)
	require.True(t, storeClient.statusWriter.called, "expected Status().Update to be called")
}

func TestReconciler_AlreadyResolved(t *testing.T) {
	cincSrv := newMockCincinnati(nil)
	defer cincSrv.Close()

	cluster := &privatev1.Cluster{}
	cluster.SetName("cluster-2")
	cluster.SetNamespace("hyperfleet")
	cluster.Spec.Release = privatev1.ReleaseSpec{Version: "4.22.0-ec.4"}
	cluster.Status.VersionResolution = &privatev1.VersionResolutionResult{
		ReleaseImage:   "quay.io/openshift-release-dev/ocp-release:4.22.0-ec.4-x86_64",
		ReleaseVersion: "4.22.0-ec.4",
		ReleaseChannel: "candidate-4.22",
	}

	r, storeClient := buildReconciler(t, cluster, cincSrv)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-2"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
	require.False(t, storeClient.updateCalled, "expected no spec Update")
}

func TestReconciler_ClusterNotFound(t *testing.T) {
	cincSrv := newMockCincinnati(nil)
	defer cincSrv.Close()

	r, _ := buildReconciler(t, nil, cincSrv) // nil cluster → NotFound

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-404"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
}

func TestReconciler_VersionNotSet(t *testing.T) {
	cincSrv := newMockCincinnati(nil)
	defer cincSrv.Close()

	cluster := &privatev1.Cluster{}
	cluster.SetName("cluster-3")
	cluster.SetNamespace("hyperfleet")
	// Release is nil — version not set

	r, storeClient := buildReconciler(t, cluster, cincSrv)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-3"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
	require.False(t, storeClient.updateCalled)
}

func TestReconciler_VersionNotInCincinnati(t *testing.T) {
	// Cincinnati returns an empty graph (no matching node).
	cincSrv := newMockCincinnati(nil)
	defer cincSrv.Close()

	cluster := &privatev1.Cluster{}
	cluster.SetName("cluster-5")
	cluster.SetNamespace("hyperfleet")
	cluster.Spec.Release = privatev1.ReleaseSpec{Version: "4.22.0-ec.4"}

	r, storeClient := buildReconciler(t, cluster, cincSrv)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-5"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
	require.False(t, storeClient.updateCalled)
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
			require.Equal(t, tc.want, buildChannel(tc.version, "candidate"))
		})
	}
}
