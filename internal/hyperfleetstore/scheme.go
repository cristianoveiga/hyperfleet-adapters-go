package hyperfleetstore

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// SchemeGroupVersion is the group/version for HyperFleet store types.
var SchemeGroupVersion = schema.GroupVersion{Group: "hyperfleet.io", Version: "v1alpha1"}

// AddToScheme registers HyperFleet store types with the given scheme.
func AddToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(SchemeGroupVersion,
		&HyperFleetCluster{},
		&HyperFleetClusterList{},
		&HyperFleetNodePool{},
		&HyperFleetNodePoolList{},
	)
	metav1.AddToGroupVersion(s, SchemeGroupVersion)
	return nil
}
