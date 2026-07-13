package hyperfleetstore

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	storectrl "github.com/patjlm/storectrl"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/common/hyperfleetapi"
)

// ClusterNamespace is the fixed Kubernetes namespace for all HyperFleetCluster objects.
const ClusterNamespace = "hyperfleet"

// clusterNamespace is an internal alias kept for convert.go readability.
const clusterNamespace = ClusterNamespace

// clusterFromAPI converts a ClusterDetail and its AdapterStatuses into a
// HyperFleetCluster with the given resource version string.
func clusterFromAPI(detail *hyperfleetapi.ClusterDetail, statuses hyperfleetapi.AdapterStatuses, rv string) *HyperFleetCluster {
	c := &HyperFleetCluster{
		BaseObject: storectrl.BaseObject{
			TypeMeta: metav1.TypeMeta{
				APIVersion: SchemeGroupVersion.String(),
				Kind:       "HyperFleetCluster",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:            detail.ID,
				Namespace:       clusterNamespace,
				ResourceVersion: rv,
			},
		},
		DisplayName:     detail.Name,
		HFGeneration:    detail.Generation,
		CreatedBy:       detail.CreatedBy,
		Spec:            detail.Spec,
		Status:          detail.Status,
		AdapterStatuses: statuses,
	}
	return c
}

// nodepoolFromAPI converts a NodePoolDetail and its AdapterStatuses into a
// HyperFleetNodePool with the given resource version string.
func nodepoolFromAPI(detail *hyperfleetapi.NodePoolDetail, statuses hyperfleetapi.AdapterStatuses, rv string) *HyperFleetNodePool {
	n := &HyperFleetNodePool{
		BaseObject: storectrl.BaseObject{
			TypeMeta: metav1.TypeMeta{
				APIVersion: SchemeGroupVersion.String(),
				Kind:       "HyperFleetNodePool",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:            detail.ID,
				Namespace:       detail.ClusterID,
				ResourceVersion: rv,
			},
		},
		ClusterID:       detail.ClusterID,
		DisplayName:     detail.Name,
		HFGeneration:    detail.Generation,
		Spec:            detail.Spec,
		Status:          detail.Status,
		AdapterStatuses: statuses,
	}
	return n
}
