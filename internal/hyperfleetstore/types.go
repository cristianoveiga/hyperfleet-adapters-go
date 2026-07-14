package hyperfleetstore

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/common/hyperfleetapi"
)

// HyperFleetCluster represents a HyperFleet cluster resource.
//
// Placeholder: field shape will be replaced once the orlop API schema is finalised.
// Name = clusterID, Namespace = ClusterNamespace ("hyperfleet").
type HyperFleetCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// DisplayName is the human-readable cluster name.
	DisplayName string `json:"displayName,omitempty"`
	// HFGeneration is the HyperFleet-side generation counter, distinct from
	// ObjectMeta.Generation which is managed by the API server.
	HFGeneration int64  `json:"hfGeneration"`
	CreatedBy    string `json:"createdBy,omitempty"`

	Spec            hyperfleetapi.ClusterSpec     `json:"spec"`
	Status          hyperfleetapi.ClusterStatus   `json:"status"`
	AdapterStatuses hyperfleetapi.AdapterStatuses `json:"adapterStatuses,omitempty"`
}

// DeepCopyObject implements runtime.Object.
func (c *HyperFleetCluster) DeepCopyObject() runtime.Object {
	out := &HyperFleetCluster{}
	*out = *c
	out.ObjectMeta = *c.ObjectMeta.DeepCopy()
	if c.AdapterStatuses != nil {
		out.AdapterStatuses = make(hyperfleetapi.AdapterStatuses, len(c.AdapterStatuses))
		copy(out.AdapterStatuses, c.AdapterStatuses)
	}
	return out
}

// HyperFleetClusterList is a list of HyperFleetCluster objects.
type HyperFleetClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HyperFleetCluster `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (l *HyperFleetClusterList) DeepCopyObject() runtime.Object {
	out := &HyperFleetClusterList{
		TypeMeta: l.TypeMeta,
		ListMeta: l.ListMeta,
	}
	out.Items = make([]HyperFleetCluster, len(l.Items))
	for i := range l.Items {
		cp := l.Items[i]
		cp.ObjectMeta = *l.Items[i].ObjectMeta.DeepCopy()
		if l.Items[i].AdapterStatuses != nil {
			cp.AdapterStatuses = make(hyperfleetapi.AdapterStatuses, len(l.Items[i].AdapterStatuses))
			copy(cp.AdapterStatuses, l.Items[i].AdapterStatuses)
		}
		out.Items[i] = cp
	}
	return out
}

// HyperFleetNodePool represents a HyperFleet node pool resource.
//
// Placeholder: field shape will be replaced once the orlop API schema is finalised.
// Name = nodepoolID, Namespace = clusterID.
type HyperFleetNodePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// ClusterID is the parent cluster's ID (= Namespace), duplicated for convenience.
	ClusterID   string `json:"clusterId"`
	DisplayName string `json:"displayName,omitempty"`
	// HFGeneration is the HyperFleet-side generation counter.
	HFGeneration int64 `json:"hfGeneration"`

	Spec            hyperfleetapi.NodePoolSpec     `json:"spec"`
	Status          hyperfleetapi.NodePoolStatus   `json:"status"`
	AdapterStatuses hyperfleetapi.AdapterStatuses  `json:"adapterStatuses,omitempty"`
}

// DeepCopyObject implements runtime.Object.
func (n *HyperFleetNodePool) DeepCopyObject() runtime.Object {
	out := &HyperFleetNodePool{}
	*out = *n
	out.ObjectMeta = *n.ObjectMeta.DeepCopy()
	if n.AdapterStatuses != nil {
		out.AdapterStatuses = make(hyperfleetapi.AdapterStatuses, len(n.AdapterStatuses))
		copy(out.AdapterStatuses, n.AdapterStatuses)
	}
	return out
}

// HyperFleetNodePoolList is a list of HyperFleetNodePool objects.
type HyperFleetNodePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HyperFleetNodePool `json:"items"`
}

// DeepCopyObject implements runtime.Object.
func (l *HyperFleetNodePoolList) DeepCopyObject() runtime.Object {
	out := &HyperFleetNodePoolList{
		TypeMeta: l.TypeMeta,
		ListMeta: l.ListMeta,
	}
	out.Items = make([]HyperFleetNodePool, len(l.Items))
	for i := range l.Items {
		cp := l.Items[i]
		cp.ObjectMeta = *l.Items[i].ObjectMeta.DeepCopy()
		if l.Items[i].AdapterStatuses != nil {
			cp.AdapterStatuses = make(hyperfleetapi.AdapterStatuses, len(l.Items[i].AdapterStatuses))
			copy(cp.AdapterStatuses, l.Items[i].AdapterStatuses)
		}
		out.Items[i] = cp
	}
	return out
}
