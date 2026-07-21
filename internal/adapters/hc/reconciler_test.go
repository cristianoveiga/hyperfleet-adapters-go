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
	workv1 "open-cluster-management.io/api/work/v1"

	privatev1 "github.com/thetechnick/orlop-gcp-hcp/api/private/v1"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/adapters/hc"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/transport"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/transport/mock"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// mockStatusWriter captures Status().Update calls and can return a configured error.
type mockStatusWriter struct {
	called    bool
	updateErr error
	captured  client.Object
}

func (m *mockStatusWriter) Update(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
	m.called = true
	m.captured = obj
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

func (m *mockStoreClient) Status() client.SubResourceWriter { return m.statusWriter }

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

// errTransport is a transport.Client that returns configurable errors.
type errTransport struct {
	applyErr        error
	getStatusErr    error
	getStatusResult *transport.ManifestWorkStatus
}

func (e *errTransport) Apply(_ context.Context, _ string, _ *workv1.ManifestWork) error {
	return e.applyErr
}
func (e *errTransport) GetStatus(_ context.Context, _, _ string) (*transport.ManifestWorkStatus, error) {
	return e.getStatusResult, e.getStatusErr
}
func (e *errTransport) Delete(_ context.Context, _, _ string) error { return nil }

// clusterReq returns a reconcile.Request for the given cluster name.
func clusterReq(name string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: "hyperfleet", Name: name},
	}
}

// conflictErr returns a Kubernetes conflict error.
func conflictErr() error {
	return apierrors.NewConflict(schema.GroupResource{Resource: "clusters"}, "test", fmt.Errorf("conflict"))
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

// buildReconciler wires up an hc.Reconciler backed by the given store and transport.
// getErr is injected into the cluster Get call; statusErr is returned by Status().Update.
func buildReconciler(
	t *testing.T,
	cluster *privatev1.Cluster,
	getErr error,
	tr transport.Client,
	statusErr error,
) (*hc.Reconciler, *mockStoreClient) {
	t.Helper()
	storeClient := &mockStoreClient{
		cluster:      cluster,
		getErr:       getErr,
		statusWriter: &mockStatusWriter{updateErr: statusErr},
	}
	return hc.New(tr, testLogger(t), storeClient), storeClient
}

// ---------------------------------------------------------------------------
// Test cases – early exits
// ---------------------------------------------------------------------------

// TestReconcile_ClusterNotFound verifies that a 404 on Get returns an empty Result with no error.
func TestReconcile_ClusterNotFound(t *testing.T) {
	tr := mock.New()
	r, _ := buildReconciler(t, nil, nil, tr, nil) // nil cluster → NotFound

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-missing"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

// TestReconcile_ClusterGetError verifies that a non-404 error from Get is propagated.
func TestReconcile_ClusterGetError(t *testing.T) {
	tr := mock.New()
	r, _ := buildReconciler(t, nil, fmt.Errorf("etcd timeout"), tr, nil)

	_, err := r.Reconcile(context.Background(), clusterReq("cluster-abc"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "get cluster")
	require.Empty(t, tr.ApplyCalls)
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
	// PlacementResult is nil.

	tr := mock.New()
	r, _ := buildReconciler(t, cluster, nil, tr, nil)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

// TestReconcile_PlacementNotReady_StatusUpdateConflict verifies that a conflict error on the
// waiting-conditions Status.Update is silently swallowed.
func TestReconcile_PlacementNotReady_StatusUpdateConflict(t *testing.T) {
	clusterID := "cluster-abc"
	cluster := &privatev1.Cluster{}
	cluster.SetName(clusterID)
	cluster.SetNamespace("hyperfleet")
	// No conditions → setWaitingConditions adds them → returns true → Status.Update called.

	tr := mock.New()
	r, storeClient := buildReconciler(t, cluster, nil, tr, conflictErr())

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called, "expected Status.Update to be called")
	require.Empty(t, tr.ApplyCalls)
}

// TestReconcile_PlacementNotReady_StatusUpdateError verifies that a non-conflict error on
// Status.Update is propagated.
func TestReconcile_PlacementNotReady_StatusUpdateError(t *testing.T) {
	clusterID := "cluster-abc"
	cluster := &privatev1.Cluster{}
	cluster.SetName(clusterID)
	cluster.SetNamespace("hyperfleet")

	tr := mock.New()
	r, _ := buildReconciler(t, cluster, nil, tr, fmt.Errorf("server error"))

	_, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update cluster status")
}

// TestReconcile_VRNil_SetsWaitingConditions verifies that a nil VersionResolution sets
// waiting conditions and returns an empty result.
func TestReconcile_VRNil_SetsWaitingConditions(t *testing.T) {
	clusterID := "cluster-abc"
	cluster := &privatev1.Cluster{}
	cluster.SetName(clusterID)
	cluster.SetNamespace("hyperfleet")
	cluster.Status.PlacementResult = &privatev1.PlacementResult{
		ManagementClusterName: "mc-cluster-1",
		BaseDomain:            "example.com",
	}
	// VersionResolution is nil.

	tr := mock.New()
	r, storeClient := buildReconciler(t, cluster, nil, tr, nil)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
	require.Empty(t, tr.ApplyCalls)
}

// TestReconcile_VRNil_StatusUpdateConflict verifies that a conflict on Status.Update when
// VR is nil is silently swallowed.
func TestReconcile_VRNil_StatusUpdateConflict(t *testing.T) {
	clusterID := "cluster-abc"
	cluster := &privatev1.Cluster{}
	cluster.SetName(clusterID)
	cluster.SetNamespace("hyperfleet")
	cluster.Status.PlacementResult = &privatev1.PlacementResult{ManagementClusterName: "mc-1"}

	tr := mock.New()
	r, storeClient := buildReconciler(t, cluster, nil, tr, conflictErr())

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
}

// TestReconcile_VRNil_StatusUpdateError verifies that a non-conflict error on Status.Update
// when VR is nil is propagated.
func TestReconcile_VRNil_StatusUpdateError(t *testing.T) {
	clusterID := "cluster-abc"
	cluster := &privatev1.Cluster{}
	cluster.SetName(clusterID)
	cluster.SetNamespace("hyperfleet")
	cluster.Status.PlacementResult = &privatev1.PlacementResult{ManagementClusterName: "mc-1"}

	tr := mock.New()
	r, _ := buildReconciler(t, cluster, nil, tr, fmt.Errorf("write error"))

	_, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update cluster status")
}

// TestReconcile_DependenciesNotReady_VRVersionMismatch verifies requeue when VR version
// doesn't match the spec version.
func TestReconcile_DependenciesNotReady_VRVersionMismatch(t *testing.T) {
	clusterID := "cluster-abc"
	// Cluster wants 4.15.0 but VR resolved 4.14.9.
	cluster := buildReadyCluster(clusterID, "4.14.9")
	cluster.Spec.Release = privatev1.ReleaseSpec{Version: "4.15.0"}

	tr := mock.New()
	r, _ := buildReconciler(t, cluster, nil, tr, nil)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

// TestReconcile_VRVersionMismatch_StatusUpdateConflict verifies conflict is swallowed
// on the version-mismatch waiting condition update.
func TestReconcile_VRVersionMismatch_StatusUpdateConflict(t *testing.T) {
	clusterID := "cluster-abc"
	cluster := buildReadyCluster(clusterID, "4.14.9")
	cluster.Spec.Release = privatev1.ReleaseSpec{Version: "4.15.0"}

	tr := mock.New()
	r, storeClient := buildReconciler(t, cluster, nil, tr, conflictErr())

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
}

// TestReconcile_VRVersionMismatch_StatusUpdateError verifies that a non-conflict error
// on Status.Update when versions mismatch is propagated.
func TestReconcile_VRVersionMismatch_StatusUpdateError(t *testing.T) {
	clusterID := "cluster-abc"
	cluster := buildReadyCluster(clusterID, "4.14.9")
	cluster.Spec.Release = privatev1.ReleaseSpec{Version: "4.15.0"}

	tr := mock.New()
	r, _ := buildReconciler(t, cluster, nil, tr, fmt.Errorf("write error"))

	_, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update cluster status")
}

// ---------------------------------------------------------------------------
// Test cases – manifest build and transport
// ---------------------------------------------------------------------------

// TestReconcile_ManifestBuildError verifies that a manifest.Build failure is propagated.
// A nil GCP spec leaves required fields empty, triggering validation inside Build.
func TestReconcile_ManifestBuildError(t *testing.T) {
	clusterID := "cluster-abc"
	cluster := buildReadyCluster(clusterID, "4.15.0")
	cluster.Spec.Platform.GCP = nil // gcpProjectID="" → Build fails validation

	tr := mock.New()
	r, _ := buildReconciler(t, cluster, nil, tr, nil)

	_, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.Error(t, err)
	require.Contains(t, err.Error(), "build manifest work")
	require.Empty(t, tr.ApplyCalls)
}

// TestReconcile_TransportApplyError verifies that a transport Apply failure is propagated.
func TestReconcile_TransportApplyError(t *testing.T) {
	clusterID := "cluster-abc"
	cluster := buildReadyCluster(clusterID, "4.15.0")

	tr := &errTransport{applyErr: fmt.Errorf("maestro unavailable")}
	r, _ := buildReconciler(t, cluster, nil, tr, nil)

	_, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.Error(t, err)
	require.Contains(t, err.Error(), "apply manifest work")
}

// TestReconcile_TransportGetStatusError verifies that a non-404 GetStatus error is propagated.
func TestReconcile_TransportGetStatusError(t *testing.T) {
	clusterID := "cluster-abc"
	cluster := buildReadyCluster(clusterID, "4.15.0")

	tr := &errTransport{getStatusErr: fmt.Errorf("grpc unavailable")}
	r, _ := buildReconciler(t, cluster, nil, tr, nil)

	_, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.Error(t, err)
	require.Contains(t, err.Error(), "get manifest work status")
}

// TestReconcile_MWStatusNil_RequeuesPending verifies that a not-found GetStatus maps to
// nil mwStatus, sets both conditions to False, and requeues with the pending interval.
func TestReconcile_MWStatusNil_RequeuesPending(t *testing.T) {
	clusterID := "cluster-abc"
	cluster := buildReadyCluster(clusterID, "4.15.0")

	notFoundErr := apierrors.NewNotFound(
		schema.GroupResource{Group: "work.open-cluster-management.io", Resource: "manifestworks"},
		"cluster-abc-hc-adapter",
	)
	tr := &errTransport{getStatusErr: notFoundErr}
	r, storeClient := buildReconciler(t, cluster, nil, tr, nil)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, 15*time.Second, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
}

// ---------------------------------------------------------------------------
// Test cases – happy path and condition-driven requeue
// ---------------------------------------------------------------------------

// TestReconcile_HappyPath verifies the full reconcile path when all dependencies are ready
// and the ManifestWork has been applied successfully.
func TestReconcile_HappyPath(t *testing.T) {
	clusterID := "cluster-abc"
	mwName := clusterID + "-hc-adapter"
	mcName := "mc-cluster-1"

	cluster := buildReadyCluster(clusterID, "4.15.0")

	tr := mock.New()
	tr.StatusOverrides[mcName+"/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully", LastTransitionTime: metav1.Now()},
		},
		ResourceStatuses: []map[string]string{
			{}, {}, {}, // indices 0-2
			{"availableCondition": "True", "degradedCondition": "False"}, // index 3 = HC
		},
	}

	r, storeClient := buildReconciler(t, cluster, nil, tr, nil)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, result.RequeueAfter)
	require.Len(t, tr.ApplyCalls, 1)
	require.Equal(t, mcName, tr.ApplyCalls[0].TargetCluster)
	require.Equal(t, mwName, tr.ApplyCalls[0].Work.Name)
	require.True(t, storeClient.statusWriter.called, "expected Status().Update to be called")
}

// TestReconcile_HCFeedback_SetsHostedClusterResult verifies that controlPlaneEndpoint and
// version fields from HC status feedback are written to cluster.Status.HostedClusterResult.
func TestReconcile_HCFeedback_SetsHostedClusterResult(t *testing.T) {
	clusterID := "cluster-abc"
	mwName := clusterID + "-hc-adapter"
	mcName := "mc-cluster-1"

	cluster := buildReadyCluster(clusterID, "4.15.0")

	tr := mock.New()
	tr.StatusOverrides[mcName+"/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully", LastTransitionTime: metav1.Now()},
		},
		ResourceStatuses: []map[string]string{
			{}, {}, {},
			{
				"availableCondition":   "True",
				"controlPlaneEndpoint": "api.my-cluster-user.example.com",
				"version":              "4.15.0",
			},
		},
	}

	r, storeClient := buildReconciler(t, cluster, nil, tr, nil)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, result.RequeueAfter)

	require.True(t, storeClient.statusWriter.called)
	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.NotNil(t, captured.Status.HostedClusterResult)
	require.Equal(t, "api.my-cluster-user.example.com", captured.Status.HostedClusterResult.APIEndpoint)
	require.Equal(t, "4.15.0", captured.Status.HostedClusterResult.Version)
}

// TestReconcile_MWNoAppliedCondition_RequeuesPending verifies that when the ManifestWork
// status has no "Applied" condition, the reconciler requeues with the pending interval.
// This also exercises the mwCondition default return path.
func TestReconcile_MWNoAppliedCondition_RequeuesPending(t *testing.T) {
	clusterID := "cluster-abc"
	mwName := clusterID + "-hc-adapter"
	mcName := "mc-cluster-1"

	cluster := buildReadyCluster(clusterID, "4.15.0")

	tr := mock.New()
	tr.StatusOverrides[mcName+"/"+mwName] = &transport.ManifestWorkStatus{} // no conditions at all

	r, _ := buildReconciler(t, cluster, nil, tr, nil)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, 15*time.Second, result.RequeueAfter)
}

// TestReconcile_MWNotApplied_RequeuesPending verifies that when the Applied condition is
// explicitly False, the reconciler requeues with the pending interval.
func TestReconcile_MWNotApplied_RequeuesPending(t *testing.T) {
	clusterID := "cluster-abc"
	mwName := clusterID + "-hc-adapter"
	mcName := "mc-cluster-1"

	cluster := buildReadyCluster(clusterID, "4.15.0")

	tr := mock.New()
	tr.StatusOverrides[mcName+"/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionFalse, Reason: "ApplyFailed", LastTransitionTime: metav1.Now()},
		},
	}

	r, _ := buildReconciler(t, cluster, nil, tr, nil)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, 15*time.Second, result.RequeueAfter)
}

// TestReconcile_ApplyConditions_Idempotent verifies that when the cluster's conditions
// already match the current MW status, applyStatusConditions returns false and
// Status.Update is not called.
func TestReconcile_ApplyConditions_Idempotent(t *testing.T) {
	clusterID := "cluster-abc"
	mwName := clusterID + "-hc-adapter"
	mcName := "mc-cluster-1"

	cluster := buildReadyCluster(clusterID, "4.15.0")
	// Pre-populate conditions to exactly match what applyStatusConditions would set.
	cluster.Status.Conditions = []metav1.Condition{
		{Type: "ManifestWorkApplied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully", Message: ""},
		{Type: "HostedClusterAvailable", Status: metav1.ConditionFalse, Reason: "HostedClusterAvailable", Message: ""},
	}

	tr := mock.New()
	tr.StatusOverrides[mcName+"/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		// No ResourceStatuses → no HC feedback → availableStatus stays "False"
	}

	r, storeClient := buildReconciler(t, cluster, nil, tr, nil)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, result.RequeueAfter)
	require.False(t, storeClient.statusWriter.called, "Status.Update should not be called when conditions are unchanged")
}

// TestReconcile_StatusUpdateConflict_ReturnsNoError verifies that a conflict error on
// Status.Update after applyStatusConditions is silently swallowed.
func TestReconcile_StatusUpdateConflict_ReturnsNoError(t *testing.T) {
	clusterID := "cluster-abc"
	mwName := clusterID + "-hc-adapter"
	mcName := "mc-cluster-1"

	cluster := buildReadyCluster(clusterID, "4.15.0")
	// No prior conditions → applyStatusConditions will add them → returns true → Update called.

	tr := mock.New()
	tr.StatusOverrides[mcName+"/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
	}

	r, storeClient := buildReconciler(t, cluster, nil, tr, conflictErr())

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter) // conflict → returns immediately
	require.True(t, storeClient.statusWriter.called)
}

// TestReconcile_WithServiceAccountsRef verifies that WIF service account emails are
// extracted from ServiceAccountsRef and passed through to the manifest build.
func TestReconcile_WithServiceAccountsRef(t *testing.T) {
	clusterID := "cluster-wif"
	mwName := clusterID + "-hc-adapter"
	mcName := "mc-cluster-1"

	cluster := buildReadyCluster(clusterID, "4.15.0")
	cluster.Spec.Platform.GCP.WorkloadIdentity = privatev1.WorkloadIdentitySpec{
		ProjectNumber: "123456789",
		PoolID:        "my-pool",
		ProviderID:    "my-provider",
		ServiceAccountsRef: &privatev1.ServiceAccountsRef{
			NodePoolEmail:        "nodepool@project.iam.gserviceaccount.com",
			ControlPlaneEmail:    "cp@project.iam.gserviceaccount.com",
			CloudControllerEmail: "cc@project.iam.gserviceaccount.com",
			StorageEmail:         "storage@project.iam.gserviceaccount.com",
			ImageRegistryEmail:   "registry@project.iam.gserviceaccount.com",
			NetworkEmail:         "network@project.iam.gserviceaccount.com",
		},
	}

	tr := mock.New()
	tr.StatusOverrides[mcName+"/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
	}

	r, storeClient := buildReconciler(t, cluster, nil, tr, nil)

	result, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, result.RequeueAfter) // ManifestWorkApplied=True → requeueStable
	require.Len(t, tr.ApplyCalls, 1)
	require.True(t, storeClient.statusWriter.called)
}

// TestReconcile_StatusUpdateError_ReturnsError verifies that a non-conflict error on
// Status.Update after applyStatusConditions is propagated.
func TestReconcile_StatusUpdateError_ReturnsError(t *testing.T) {
	clusterID := "cluster-abc"
	mwName := clusterID + "-hc-adapter"
	mcName := "mc-cluster-1"

	cluster := buildReadyCluster(clusterID, "4.15.0")

	tr := mock.New()
	tr.StatusOverrides[mcName+"/"+mwName] = &transport.ManifestWorkStatus{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
	}

	r, storeClient := buildReconciler(t, cluster, nil, tr, fmt.Errorf("server error"))

	_, err := r.Reconcile(context.Background(), clusterReq(clusterID))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update cluster status")
	require.True(t, storeClient.statusWriter.called)
}
