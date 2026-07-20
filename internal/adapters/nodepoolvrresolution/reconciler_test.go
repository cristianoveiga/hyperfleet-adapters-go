package nodepoolvrresolution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	privatev1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/versionresolution"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/conditions"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

// ─── mocks ────────────────────────────────────────────────────────────────────

// statusWriter is a SubResourceWriter that captures the object passed to Update
// and can return a configured error.
type statusWriter struct {
	called    bool
	updateErr error
	captured  client.Object
}

func (m *statusWriter) Update(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
	m.called = true
	m.captured = obj
	return m.updateErr
}
func (m *statusWriter) Create(_ context.Context, _ client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
	return nil
}
func (m *statusWriter) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
	return nil
}
func (m *statusWriter) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
	return nil
}

// storeClient is a minimal client.Client backed by a fixed NodePool and Cluster
// with configurable error injection for Get and Status().Update.
type storeClient struct {
	nodepool     *privatev1.NodePool
	cluster      *privatev1.Cluster
	npGetErr     error
	clsGetErr    error
	statusWriter *statusWriter
}

func (m *storeClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
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

func (m *storeClient) Status() client.SubResourceWriter { return m.statusWriter }

func (m *storeClient) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	return nil
}
func (m *storeClient) Create(_ context.Context, _ client.Object, _ ...client.CreateOption) error {
	return nil
}
func (m *storeClient) Delete(_ context.Context, _ client.Object, _ ...client.DeleteOption) error {
	return nil
}
func (m *storeClient) Update(_ context.Context, _ client.Object, _ ...client.UpdateOption) error {
	return nil
}
func (m *storeClient) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.PatchOption) error {
	return nil
}
func (m *storeClient) DeleteAllOf(_ context.Context, _ client.Object, _ ...client.DeleteAllOfOption) error {
	return nil
}
func (m *storeClient) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.ApplyOption) error {
	return nil
}
func (m *storeClient) SubResource(_ string) client.SubResourceClient { return nil }
func (m *storeClient) Scheme() *runtime.Scheme                       { return nil }
func (m *storeClient) RESTMapper() meta.RESTMapper                   { return nil }
func (m *storeClient) GroupVersionKindFor(_ runtime.Object) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, nil
}
func (m *storeClient) IsObjectNamespaced(_ runtime.Object) (bool, error) { return false, nil }

// ─── helpers ──────────────────────────────────────────────────────────────────

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

// makeNP builds a NodePool with Spec.Release.Version set.
func makeNP(clusterID, npID, version string) *privatev1.NodePool {
	np := &privatev1.NodePool{}
	np.SetName(npID)
	np.SetNamespace(clusterID)
	np.Spec.ClusterID = clusterID
	np.Spec.Release.Version = version
	return np
}

// makeCluster builds a minimal Cluster.
func makeCluster(id string) *privatev1.Cluster {
	c := &privatev1.Cluster{}
	c.SetName(id)
	c.SetNamespace("hyperfleet")
	return c
}

// buildReconciler constructs a Reconciler backed by the given NodePool and
// Cluster. cincURL points to a Cincinnati test server (may be empty for paths
// that never reach it). statusErr is returned by Status().Update.
func buildReconciler(
	t *testing.T,
	np *privatev1.NodePool,
	cluster *privatev1.Cluster,
	cincURL string,
	npGetErr, clsGetErr, statusErr error,
) (*Reconciler, *storeClient) {
	t.Helper()
	sw := &statusWriter{updateErr: statusErr}
	store := &storeClient{
		nodepool: np, cluster: cluster,
		npGetErr: npGetErr, clsGetErr: clsGetErr,
		statusWriter: sw,
	}
	if cincURL == "" {
		cincURL = "http://127.0.0.1:0" // unreachable; only used if the test reaches Cincinnati
	}
	cinc := versionresolution.NewCincinnatiClient(cincURL, "amd64")
	return NewReconciler(cinc, newTestLogger(t), store), store
}

// npReq returns a reconcile.Request for the given clusterID/nodepoolID pair.
func npReq(clusterID, nodepoolID string) reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKey{Namespace: clusterID, Name: nodepoolID}}
}

// cincServer starts a test server that returns the given nodes as a Cincinnati graph.
func cincServer(t *testing.T, nodes []versionresolution.ReleaseInfo) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		graph := versionresolution.CincinnatiGraph{Nodes: nodes}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(graph)
	}))
}

// cincServerError starts a test server that always returns the given HTTP status.
func cincServerError(t *testing.T, statusCode int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte("internal error"))
	}))
}

// conflictErr returns a Kubernetes 409 Conflict error.
func conflictErr() error {
	return apierrors.NewConflict(schema.GroupResource{Resource: "nodepools"}, "np", errors.New("conflict"))
}

// ─── NodePool / Cluster fetch errors ─────────────────────────────────────────

func TestReconciler_NodepoolNotFound(t *testing.T) {
	r, _ := buildReconciler(t, nil, nil, "", nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-404"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
}

func TestReconciler_NodepoolGetError(t *testing.T) {
	r, _ := buildReconciler(t, nil, nil, "", fmt.Errorf("etcd timeout"), nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "get nodepool")
}

func TestReconciler_ClusterNotFoundForNodepool(t *testing.T) {
	np := makeNP("cluster-1", "np-1", "")
	r, _ := buildReconciler(t, np, nil, "", nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
}

func TestReconciler_ClusterGetError(t *testing.T) {
	np := makeNP("cluster-1", "np-1", "4.15.0")
	r, _ := buildReconciler(t, np, nil, "", nil, fmt.Errorf("etcd timeout"), nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "get cluster")
}

// ─── release version not set ──────────────────────────────────────────────────

func TestReconciler_VersionNotSet_SetsConditionAndReturns(t *testing.T) {
	np := makeNP("cluster-1", "np-1", "") // no version

	r, store := buildReconciler(t, np, makeCluster("cluster-1"), "", nil, nil, nil)
	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)
	assert.True(t, store.statusWriter.called)

	updated := store.statusWriter.captured.(*privatev1.NodePool)
	require.Len(t, updated.Status.Conditions, 1)
	assert.Equal(t, "ReleaseVersionNotSet", updated.Status.Conditions[0].Reason)
}

func TestReconciler_VersionNotSet_StatusUpdateError(t *testing.T) {
	np := makeNP("cluster-1", "np-1", "")

	r, _ := buildReconciler(t, np, makeCluster("cluster-1"), "", nil, nil, fmt.Errorf("api-server busy"))
	_, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update nodepool status")
}

// ─── already resolved ─────────────────────────────────────────────────────────

func TestReconciler_AlreadyResolved_SkipsWithoutCincinnati(t *testing.T) {
	np := makeNP("cluster-1", "np-1", "4.15.0")
	np.Status.VersionResolution = &privatev1.VersionResolutionResult{ReleaseVersion: "4.15.0"}

	r, store := buildReconciler(t, np, makeCluster("cluster-1"), "", nil, nil, nil)
	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)
	assert.False(t, store.statusWriter.called, "status must not be updated when already resolved")
}

// ─── channel construction ─────────────────────────────────────────────────────

func TestReconciler_InvalidVersion_BuildChannelError(t *testing.T) {
	np := makeNP("cluster-1", "np-1", "4") // no minor → invalid for buildChannel

	r, _ := buildReconciler(t, np, makeCluster("cluster-1"), "", nil, nil, nil)
	_, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "build channel")
}

func TestReconciler_CustomChannelGroup_UsedInCincinnatiRequest(t *testing.T) {
	version := "4.15.0"
	var capturedChannel string
	customSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedChannel = r.URL.Query().Get("channel")
		graph := versionresolution.CincinnatiGraph{
			Nodes: []versionresolution.ReleaseInfo{{Version: version, Payload: "quay.io/ocp:4.15.0"}},
		}
		_ = json.NewEncoder(w).Encode(graph)
	}))
	defer customSrv.Close()

	np := makeNP("cluster-1", "np-1", version)
	np.Spec.Release.ChannelGroup = "stable"

	r, _ := buildReconciler(t, np, makeCluster("cluster-1"), customSrv.URL, nil, nil, nil)
	_, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.NoError(t, err)
	assert.Equal(t, "stable-4.15", capturedChannel)
}

// ─── Cincinnati error paths ───────────────────────────────────────────────────

func TestReconciler_CincinnatiError(t *testing.T) {
	errSrv := cincServerError(t, http.StatusInternalServerError)
	defer errSrv.Close()

	np := makeNP("cluster-1", "np-1", "4.15.0")
	r, _ := buildReconciler(t, np, makeCluster("cluster-1"), errSrv.URL, nil, nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cincinnati resolve")
}

func TestReconciler_CincinnatiVersionNotFound_SetsCondition(t *testing.T) {
	srv := cincServer(t, []versionresolution.ReleaseInfo{
		{Version: "4.99.0", Payload: "quay.io/ocp:4.99.0"}, // different version → not found
	})
	defer srv.Close()

	np := makeNP("cluster-1", "np-1", "4.15.0")
	r, store := buildReconciler(t, np, makeCluster("cluster-1"), srv.URL, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)
	assert.True(t, store.statusWriter.called)

	updated := store.statusWriter.captured.(*privatev1.NodePool)
	require.Len(t, updated.Status.Conditions, 1)
	cond := updated.Status.Conditions[0]
	assert.Equal(t, "NodePoolVersionResolved", cond.Type)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "VersionNotFoundInCincinnati", cond.Reason)
}

func TestReconciler_CincinnatiVersionNotFound_StatusUpdateError(t *testing.T) {
	srv := cincServer(t, nil) // empty graph → version not found
	defer srv.Close()

	np := makeNP("cluster-1", "np-1", "4.15.0")
	r, _ := buildReconciler(t, np, makeCluster("cluster-1"), srv.URL, nil, nil, fmt.Errorf("api busy"))

	_, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update nodepool status")
}

// ─── successful resolution ────────────────────────────────────────────────────

func TestReconciler_ResolvesVersion_WritesStatusAndRequeues(t *testing.T) {
	version := "4.15.0"
	payload := "quay.io/openshift-release-dev/ocp-release:4.15.0-x86_64"

	srv := cincServer(t, []versionresolution.ReleaseInfo{{Version: version, Payload: payload}})
	defer srv.Close()

	np := makeNP("cluster-1", "np-1", version)
	r, store := buildReconciler(t, np, makeCluster("cluster-1"), srv.URL, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.NoError(t, err)
	assert.Equal(t, requeueStable, result.RequeueAfter)
	assert.True(t, store.statusWriter.called)

	updated := store.statusWriter.captured.(*privatev1.NodePool)
	require.NotNil(t, updated.Status.VersionResolution)
	assert.Equal(t, payload, updated.Status.VersionResolution.ReleaseImage)
	assert.Equal(t, version, updated.Status.VersionResolution.ReleaseVersion)
	assert.Equal(t, "candidate-4.15", updated.Status.VersionResolution.ReleaseChannel)

	require.Len(t, updated.Status.Conditions, 1)
	cond := updated.Status.Conditions[0]
	assert.Equal(t, "NodePoolVersionResolved", cond.Type)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "VersionResolved", cond.Reason)
}

func TestReconciler_ResolvesVersion_StatusUpdateConflict_ReturnsNoError(t *testing.T) {
	// A 409 conflict during the final status update is swallowed; another
	// reconcile triggered by the watch will pick up the nodepool.
	version := "4.15.0"
	srv := cincServer(t, []versionresolution.ReleaseInfo{{Version: version, Payload: "quay.io/ocp:4.15.0"}})
	defer srv.Close()

	np := makeNP("cluster-1", "np-1", version)
	r, store := buildReconciler(t, np, makeCluster("cluster-1"), srv.URL, nil, nil, conflictErr())

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.NoError(t, err, "conflict must be swallowed")
	assert.Equal(t, reconcile.Result{}, result)
	assert.True(t, store.statusWriter.called)
}

func TestReconciler_ResolvesVersion_StatusUpdateError(t *testing.T) {
	version := "4.15.0"
	srv := cincServer(t, []versionresolution.ReleaseInfo{{Version: version, Payload: "quay.io/ocp:4.15.0"}})
	defer srv.Close()

	np := makeNP("cluster-1", "np-1", version)
	r, _ := buildReconciler(t, np, makeCluster("cluster-1"), srv.URL, nil, nil, fmt.Errorf("disk full"))

	_, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update nodepool status")
}

// ─── buildChannel ─────────────────────────────────────────────────────────────

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

// ─── conditions helper (smoke test via reconciler) ────────────────────────────

func TestSetCondition(t *testing.T) {
	t.Run("appends new condition", func(t *testing.T) {
		var conds []metav1.Condition
		conditions.Set(&conds, metav1.Condition{Type: "Applied", Status: metav1.ConditionTrue, Reason: "Test"})
		require.Len(t, conds, 1)
		require.Equal(t, "Applied", conds[0].Type)
		require.Equal(t, metav1.ConditionTrue, conds[0].Status)
		require.False(t, conds[0].LastTransitionTime.IsZero())
	})

	t.Run("preserves LastTransitionTime on same status", func(t *testing.T) {
		ts := metav1.Now()
		conds := []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, LastTransitionTime: ts, Reason: "Test"},
		}
		conditions.Set(&conds, metav1.Condition{Type: "Applied", Status: metav1.ConditionTrue, Reason: "Test"})
		require.Len(t, conds, 1)
		require.Equal(t, ts, conds[0].LastTransitionTime)
	})

	t.Run("updates LastTransitionTime on status change", func(t *testing.T) {
		old := metav1.Now()
		conds := []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionFalse, LastTransitionTime: old, Reason: "Test"},
		}
		conditions.Set(&conds, metav1.Condition{Type: "Applied", Status: metav1.ConditionTrue, Reason: "Test"})
		require.Len(t, conds, 1)
		require.False(t, conds[0].LastTransitionTime.IsZero())
	})
}