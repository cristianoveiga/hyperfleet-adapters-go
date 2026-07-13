package hyperfleetstore

import (
	"k8s.io/apimachinery/pkg/runtime"

	storectrl "github.com/patjlm/storectrl"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/common/hyperfleetapi"
)

// HyperFleetCluster represents a HyperFleet cluster as a store resource.
// ObjectMeta.Name = clusterID, ObjectMeta.Namespace = "hyperfleet" (fixed).
type HyperFleetCluster struct {
	storectrl.BaseObject `json:",inline"`
	// Top-level ClusterDetail fields that are not part of Spec or Status.
	// DisplayName is the human-readable cluster name (ClusterDetail.Name).
	DisplayName string `json:"displayName,omitempty"`
	// HFGeneration is the HyperFleet API generation counter (ClusterDetail.Generation),
	// distinct from ObjectMeta.Generation which tracks store resource versions.
	HFGeneration int64 `json:"hfGeneration"`
	// CreatedBy is the identity that created the cluster (ClusterDetail.CreatedBy).
	CreatedBy string `json:"createdBy,omitempty"`
	Spec      hyperfleetapi.ClusterSpec   `json:"spec"`
	Status    hyperfleetapi.ClusterStatus `json:"status"`
	// AdapterStatuses is pre-populated by the polling loop from GET /clusters/{id}/statuses.
	AdapterStatuses hyperfleetapi.AdapterStatuses `json:"adapterStatuses,omitempty"`
}

// DeepCopyObject implements runtime.Object.
func (c *HyperFleetCluster) DeepCopyObject() runtime.Object {
	cp := *c
	c.BaseObject.DeepCopyInto(&cp.BaseObject)
	return &cp
}

// HyperFleetClusterList is a list of HyperFleetCluster objects.
type HyperFleetClusterList struct {
	storectrl.BaseList `json:",inline"`
	Items              []HyperFleetCluster `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (l *HyperFleetClusterList) DeepCopyObject() runtime.Object {
	cp := *l
	l.BaseList.DeepCopyInto(&cp.BaseList)
	items := make([]HyperFleetCluster, len(l.Items))
	for i := range l.Items {
		l.Items[i].DeepCopyObject() // validate interface; actual copy below
		item := l.Items[i]
		var base storectrl.BaseObject
		l.Items[i].BaseObject.DeepCopyInto(&base)
		item.BaseObject = base
		items[i] = item
	}
	cp.Items = items
	return &cp
}

// HyperFleetNodePool represents a HyperFleet node pool as a store resource.
// ObjectMeta.Name = nodepoolID, ObjectMeta.Namespace = clusterID.
type HyperFleetNodePool struct {
	storectrl.BaseObject `json:",inline"`
	// ClusterID is the parent cluster's ID, duplicated here for convenience.
	ClusterID string `json:"clusterId"`
	// DisplayName is the human-readable node pool name (NodePoolDetail.Name).
	DisplayName string `json:"displayName,omitempty"`
	// HFGeneration is the HyperFleet API generation counter (NodePoolDetail.Generation).
	HFGeneration int64                       `json:"hfGeneration"`
	Spec         hyperfleetapi.NodePoolSpec  `json:"spec"`
	Status       hyperfleetapi.NodePoolStatus `json:"status"`
	// AdapterStatuses is pre-populated by the polling loop from GET /clusters/{id}/nodepools/{id}/statuses.
	AdapterStatuses hyperfleetapi.AdapterStatuses `json:"adapterStatuses,omitempty"`
}

// DeepCopyObject implements runtime.Object.
func (n *HyperFleetNodePool) DeepCopyObject() runtime.Object {
	cp := *n
	n.BaseObject.DeepCopyInto(&cp.BaseObject)
	return &cp
}

// HyperFleetNodePoolList is a list of HyperFleetNodePool objects.
type HyperFleetNodePoolList struct {
	storectrl.BaseList `json:",inline"`
	Items              []HyperFleetNodePool `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (l *HyperFleetNodePoolList) DeepCopyObject() runtime.Object {
	cp := *l
	l.BaseList.DeepCopyInto(&cp.BaseList)
	items := make([]HyperFleetNodePool, len(l.Items))
	for i := range l.Items {
		item := l.Items[i]
		var base storectrl.BaseObject
		l.Items[i].BaseObject.DeepCopyInto(&base)
		item.BaseObject = base
		items[i] = item
	}
	cp.Items = items
	return &cp
}
