package nodepoolvrresolution

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	privatev1alpha1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1alpha1"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/versionresolution"
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

// mockStoreClient is a minimal client.Client backed by a fixed NodePool and Cluster.
type mockStoreClient struct {
	nodepool  *privatev1alpha1.NodePool
	cluster   *privatev1alpha1.Cluster
	npGetErr  error
	clsGetErr error
}

func (m *mockStoreClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	switch o := obj.(type) {
	case *privatev1alpha1.NodePool:
		if m.npGetErr != nil {
			return m.npGetErr
		}
		if m.nodepool == nil {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "nodepool"}, "")
		}
		*o = *m.nodepool
		return nil
	case *privatev1alpha1.Cluster:
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

// npReq returns a reconcile.Request for the given clusterID/nodepoolID pair.
func npReq(clusterID, nodepoolID string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: clusterID, Name: nodepoolID},
	}
}

// buildReconciler wires up a Reconciler backed by the store client.
func buildReconciler(
	t *testing.T,
	np *privatev1alpha1.NodePool,
	cluster *privatev1alpha1.Cluster,
) *Reconciler {
	t.Helper()
	// Cincinnati client is created but the current reconciler returns early before
	// using it (pending NodePoolSpec.Release field). Use a dummy URL.
	cincClient := versionresolution.NewCincinnatiClient("http://localhost:0", "amd64")
	storeClient := &mockStoreClient{nodepool: np, cluster: cluster}
	return NewReconciler(cincClient, newTestLogger(t), storeClient)
}

// ---- tests ------------------------------------------------------------------

func TestReconciler_NodepoolNotFound(t *testing.T) {
	r := buildReconciler(t, nil, nil) // nil nodepool → NotFound

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-404"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
}

func TestReconciler_ClusterNotFoundForNodepool(t *testing.T) {
	np := &privatev1alpha1.NodePool{}
	np.SetName("np-1")
	np.SetNamespace("cluster-1")

	r := buildReconciler(t, np, nil) // nil cluster → NotFound

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-1"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
}

func TestReconciler_AlreadyResolved(t *testing.T) {
	np := &privatev1alpha1.NodePool{}
	np.SetName("np-2")
	np.SetNamespace("cluster-1")
	np.Status.VersionResolution = &privatev1alpha1.VersionResolutionResult{
		ReleaseImage:   "quay.io/openshift-release-dev/ocp-release:4.22.0-ec.4-x86_64",
		ReleaseVersion: "4.22.0-ec.4",
	}

	cluster := &privatev1alpha1.Cluster{}
	cluster.SetName("cluster-1")
	cluster.SetNamespace("hyperfleet")

	r := buildReconciler(t, np, cluster)

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-2"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
}

func TestReconciler_NoVersionResolution_ReturnsNoOp(t *testing.T) {
	// NodePool has no VersionResolution and no Release field (pending types).
	// Reconciler should return early with no-op.
	np := &privatev1alpha1.NodePool{}
	np.SetName("np-3")
	np.SetNamespace("cluster-1")

	cluster := &privatev1alpha1.Cluster{}
	cluster.SetName("cluster-1")
	cluster.SetNamespace("hyperfleet")

	r := buildReconciler(t, np, cluster)

	result, err := r.Reconcile(context.Background(), npReq("cluster-1", "np-3"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
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
