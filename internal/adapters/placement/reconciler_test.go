package placement

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	privatev1alpha1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1alpha1"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

// mockSelector is a simple Selector implementation for tests.
type mockSelector struct {
	mcName     string
	baseDomain string
	err        error
}

func (m *mockSelector) Select(_ context.Context, _ []Candidate) (string, string, error) {
	return m.mcName, m.baseDomain, m.err
}

// testLogger creates a logger for tests.
func testLogger(t *testing.T) logger.Logger {
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

// testCandidates returns a default candidate list for tests.
func testCandidates() []Candidate {
	return []Candidate{
		{Name: "mc-us-c1", BaseDomains: []string{"hc-us-central1-abc.example.com"}},
	}
}

// mockStatusWriter is a minimal SubResourceWriter for status updates.
type mockStatusWriter struct {
	updateErr error
}

func (m *mockStatusWriter) Update(_ context.Context, _ client.Object, _ ...client.SubResourceUpdateOption) error {
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

// mockStoreClient is a minimal client.Client that captures Update/Status calls.
type mockStoreClient struct {
	cluster      *privatev1alpha1.Cluster
	getErr       error
	updateErr    error
	statusWriter *mockStatusWriter
	updateCalled bool
}

func (m *mockStoreClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if m.getErr != nil {
		return m.getErr
	}
	if m.cluster == nil {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "cluster"}, "")
	}
	c, ok := obj.(*privatev1alpha1.Cluster)
	if !ok {
		return fmt.Errorf("unexpected type %T", obj)
	}
	*c = *m.cluster
	return nil
}

func (m *mockStoreClient) Update(_ context.Context, _ client.Object, _ ...client.UpdateOption) error {
	m.updateCalled = true
	return m.updateErr
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

// buildCluster builds a Cluster for use in tests.
func buildCluster(id string, placed bool) *privatev1alpha1.Cluster {
	c := &privatev1alpha1.Cluster{}
	c.SetName(id)
	c.SetNamespace("hyperfleet")
	if placed {
		c.Status.PlacementResult = &privatev1alpha1.PlacementResult{
			ManagementClusterName: "mc-us-c1",
			BaseDomain:            "hc-us-central1-abc.example.com",
		}
	}
	return c
}

func TestReconciler(t *testing.T) {
	tests := []struct {
		name           string
		clusterID      string
		cluster        *privatev1alpha1.Cluster // nil → NotFound
		selector       *mockSelector
		expectUpdate   bool
		expectedResult reconcile.Result
		expectError    bool
	}{
		{
			name:           "happy path: selects MC and domain, updates status",
			clusterID:      "cluster-1",
			cluster:        buildCluster("cluster-1", false),
			selector:       &mockSelector{mcName: "mc-us-c1", baseDomain: "hc-us-central1-abc.example.com"},
			expectUpdate:   false,
			expectedResult: reconcile.Result{RequeueAfter: requeueAfter},
		},
		{
			name:           "already placed: no update, empty result",
			clusterID:      "cluster-2",
			cluster:        buildCluster("cluster-2", true),
			selector:       &mockSelector{mcName: "mc-us-c1", baseDomain: "hc-us-central1-abc.example.com"},
			expectUpdate:   false,
			expectedResult: reconcile.Result{},
		},
		{
			name:           "cluster not found: return empty result, no error",
			clusterID:      "cluster-missing",
			cluster:        nil, // → NotFoundError
			selector:       &mockSelector{mcName: "mc-us-c1", baseDomain: "hc-us-central1-abc.example.com"},
			expectUpdate:   false,
			expectedResult: reconcile.Result{},
			expectError:    false,
		},
		{
			name:         "selector error: return error",
			clusterID:    "cluster-4",
			cluster:      buildCluster("cluster-4", false),
			selector:     &mockSelector{err: fmt.Errorf("no candidates available")},
			expectUpdate: false,
			expectError:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			storeClient := &mockStoreClient{cluster: tc.cluster}

			reconciler := &Reconciler{
				client:     storeClient,
				selector:   tc.selector,
				candidates: testCandidates(),
				log:        testLogger(t),
			}

			req := reconcile.Request{
				NamespacedName: client.ObjectKey{
					Namespace: "hyperfleet",
					Name:      tc.clusterID,
				},
			}
			result, err := reconciler.Reconcile(context.Background(), req)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectedResult, result)
			require.Equal(t, tc.expectUpdate, storeClient.updateCalled, "Update called mismatch")
		})
	}
}

func TestSetCondition(t *testing.T) {
	t.Run("appends new condition", func(t *testing.T) {
		var conds []metav1.Condition
		setCondition(&conds, metav1.Condition{Type: "Applied", Status: metav1.ConditionTrue})
		require.Len(t, conds, 1)
		require.Equal(t, "Applied", conds[0].Type)
		require.Equal(t, metav1.ConditionTrue, conds[0].Status)
		require.False(t, conds[0].LastTransitionTime.IsZero())
	})

	t.Run("preserves LastTransitionTime on same status update", func(t *testing.T) {
		transitioned := metav1.Now()
		conds := []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, LastTransitionTime: transitioned},
		}
		setCondition(&conds, metav1.Condition{Type: "Applied", Status: metav1.ConditionTrue})
		require.Len(t, conds, 1)
		require.Equal(t, transitioned, conds[0].LastTransitionTime)
	})

	t.Run("updates LastTransitionTime on status change", func(t *testing.T) {
		old := metav1.Now()
		conds := []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionFalse, LastTransitionTime: old},
		}
		setCondition(&conds, metav1.Condition{Type: "Applied", Status: metav1.ConditionTrue})
		require.Len(t, conds, 1)
		require.False(t, conds[0].LastTransitionTime.IsZero())
	})
}

func TestRoundRobinSelector(t *testing.T) {
	t.Run("selects candidates in round-robin order", func(t *testing.T) {
		candidates := []Candidate{
			{Name: "mc-a", BaseDomains: []string{"a.example.com"}},
			{Name: "mc-b", BaseDomains: []string{"b.example.com"}},
		}

		sel := NewRoundRobinSelector()
		ctx := context.Background()

		mc1, domain1, err := sel.Select(ctx, candidates)
		require.NoError(t, err)
		require.Equal(t, "mc-a", mc1)
		require.Equal(t, "a.example.com", domain1)

		mc2, domain2, err := sel.Select(ctx, candidates)
		require.NoError(t, err)
		require.Equal(t, "mc-b", mc2)
		require.Equal(t, "b.example.com", domain2)

		// Wraps around.
		mc3, _, err := sel.Select(ctx, candidates)
		require.NoError(t, err)
		require.Equal(t, "mc-a", mc3)
	})

	t.Run("returns error when no candidates", func(t *testing.T) {
		sel := NewRoundRobinSelector()
		_, _, err := sel.Select(context.Background(), nil)
		require.Error(t, err)
	})

	t.Run("returns error when candidate has no base domains", func(t *testing.T) {
		sel := NewRoundRobinSelector()
		candidates := []Candidate{
			{Name: "mc-empty", BaseDomains: []string{}},
		}
		_, _, err := sel.Select(context.Background(), candidates)
		require.Error(t, err)
	})
}
