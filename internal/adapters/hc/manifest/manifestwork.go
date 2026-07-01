// Package manifest provides the ManifestWork builder for the hc-adapter.
package manifest

import (
	"encoding/json"
	"fmt"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	workv1 "open-cluster-management.io/api/work/v1"
)

// hostedClusterManifestIndex is the position of the HostedCluster in the manifest list.
const hostedClusterManifestIndex = 3

// Input holds all parameters needed to build the HC ManifestWork.
type Input struct {
	ClusterID                    string
	ClusterName                  string
	Generation                   int64
	CreatedBy                    string
	InfraID                      string
	IssuerURL                    string
	ClusterIDUUID                string // spec.clusterID (RFC4122 UUID)
	GCPProjectID                 string
	GCPRegion                    string
	GCPNetwork                   string
	GCPSubnet                    string
	GCPEndpointAccess            string // default: "Private"
	WIFProjectNumber             string
	WIFPoolID                    string
	WIFProviderID                string
	NodePoolEmail                string
	ControlPlaneEmail            string
	CloudControllerEmail         string
	StorageEmail                 string
	ImageRegistryEmail           string
	NetworkEmail                 string
	ReleaseImage                 string
	ReleaseChannel               string
	BaseDomain                   string
	PullSecretStoreName          string // default: "gcp-secret-manager"
	PullSecretGCPKey             string // default: "default-openshift-pull-secret"
	ControllerAvailabilityPolicy string // default: "HighlyAvailable"
	CPOImage                     string // optional — set CPO annotation if non-empty
	CAPGImage                    string // optional — set CAPG annotation if non-empty
	Slug                         string // default: "user" (username slug for DNS names)
}

// Build constructs a *workv1.ManifestWork from the given input.
// Returns an error if required fields are missing.
func Build(input Input) (*workv1.ManifestWork, error) {
	if err := validate(input); err != nil {
		return nil, err
	}

	// Apply defaults.
	if input.GCPEndpointAccess == "" {
		input.GCPEndpointAccess = "Private"
	}
	if input.PullSecretStoreName == "" {
		input.PullSecretStoreName = "gcp-secret-manager"
	}
	if input.PullSecretGCPKey == "" {
		input.PullSecretGCPKey = "default-openshift-pull-secret"
	}
	if input.ControllerAvailabilityPolicy == "" {
		input.ControllerAvailabilityPolicy = "HighlyAvailable"
	}
	if input.Slug == "" {
		input.Slug = "user"
	}

	genStr := strconv.FormatInt(input.Generation, 10)
	clusterNS := fmt.Sprintf("clusters-%s", input.ClusterID)
	hcNS := fmt.Sprintf("clusters-%s-%s", input.ClusterID, input.ClusterName)

	manifests, err := buildManifests(input, clusterNS, hcNS)
	if err != nil {
		return nil, fmt.Errorf("hc manifest: build manifests: %w", err)
	}

	mw := &workv1.ManifestWork{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "work.open-cluster-management.io/v1",
			Kind:       "ManifestWork",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-hc-adapter", input.ClusterID),
			Labels: map[string]string{
				"hyperfleet.io/cluster-id": input.ClusterID,
				"hyperfleet.io/adapter":    "hc-adapter",
				"hyperfleet.io/component":  "hosted-cluster",
			},
			Annotations: map[string]string{
				"hyperfleet.io/generation": genStr,
			},
		},
		Spec: workv1.ManifestWorkSpec{
			Workload: workv1.ManifestsTemplate{
				Manifests: manifests,
			},
			DeleteOption: &workv1.DeleteOption{
				PropagationPolicy: workv1.DeletePropagationPolicyTypeForeground,
			},
			ManifestConfigs: buildManifestConfigs(input.ClusterID, input.ClusterName, clusterNS, hcNS),
		},
	}

	return mw, nil
}

func validate(input Input) error {
	required := map[string]string{
		"ClusterID":    input.ClusterID,
		"ClusterName":  input.ClusterName,
		"ReleaseImage": input.ReleaseImage,
		"BaseDomain":   input.BaseDomain,
		"GCPProjectID": input.GCPProjectID,
		"GCPRegion":    input.GCPRegion,
	}
	for field, val := range required {
		if val == "" {
			return fmt.Errorf("hc manifest: required field %s is empty", field)
		}
	}
	if input.Generation <= 0 {
		return fmt.Errorf("hc manifest: Generation must be > 0")
	}
	return nil
}

func buildManifests(input Input, clusterNS, hcNS string) ([]workv1.Manifest, error) {
	ns, err := buildNamespace(input, clusterNS)
	if err != nil {
		return nil, err
	}
	es, err := buildExternalSecret(input, clusterNS)
	if err != nil {
		return nil, err
	}
	cert, err := buildCertificate(input, clusterNS)
	if err != nil {
		return nil, err
	}
	hc, err := buildHostedCluster(input, clusterNS)
	if err != nil {
		return nil, err
	}
	job, err := buildRBACJob(input, hcNS)
	if err != nil {
		return nil, err
	}

	return []workv1.Manifest{ns, es, cert, hc, job}, nil
}

func toManifest(obj map[string]any) (workv1.Manifest, error) {
	raw, err := json.Marshal(obj)
	if err != nil {
		return workv1.Manifest{}, fmt.Errorf("hc manifest: marshal resource: %w", err)
	}
	return workv1.Manifest{RawExtension: runtime.RawExtension{Raw: raw}}, nil
}

func buildNamespace(input Input, clusterNS string) (workv1.Manifest, error) {
	genStr := strconv.FormatInt(input.Generation, 10)
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name": clusterNS,
			"labels": map[string]any{
				"hyperfleet.io/cluster-id": input.ClusterID,
				"hyperfleet.io/adapter":    "hc-adapter",
				"hyperfleet.io/component":  "hosted-cluster",
			},
			"annotations": map[string]any{
				"hyperfleet.io/generation": genStr,
			},
		},
	}
	return toManifest(obj)
}

func buildExternalSecret(input Input, clusterNS string) (workv1.Manifest, error) {
	genStr := strconv.FormatInt(input.Generation, 10)
	obj := map[string]any{
		"apiVersion": "external-secrets.io/v1",
		"kind":       "ExternalSecret",
		"metadata": map[string]any{
			"name":      "pull-secret",
			"namespace": clusterNS,
			"annotations": map[string]any{
				"hyperfleet.io/generation": genStr,
			},
		},
		"spec": map[string]any{
			"refreshInterval": "1h",
			"secretStoreRef": map[string]any{
				"name": input.PullSecretStoreName,
				"kind": "ClusterSecretStore",
			},
			"target": map[string]any{
				"name": "pull-secret",
				"template": map[string]any{
					"type": "kubernetes.io/dockerconfigjson",
				},
			},
			"dataFrom": []any{
				map[string]any{
					"extract": map[string]any{
						"key": input.PullSecretGCPKey,
					},
				},
			},
		},
	}
	return toManifest(obj)
}

func buildCertificate(input Input, clusterNS string) (workv1.Manifest, error) {
	genStr := strconv.FormatInt(input.Generation, 10)
	dnsName := fmt.Sprintf("*.%s-%s.%s", input.ClusterName, input.Slug, input.BaseDomain)
	obj := map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata": map[string]any{
			"name":      "external-api-cert",
			"namespace": clusterNS,
			"annotations": map[string]any{
				"hyperfleet.io/generation": genStr,
			},
		},
		"spec": map[string]any{
			"secretName": "external-api-cert",
			"dnsNames": []any{
				dnsName,
			},
			"issuerRef": map[string]any{
				"name": "letsencrypt",
				"kind": "ClusterIssuer",
			},
		},
	}
	return toManifest(obj)
}

func buildHostedCluster(input Input, clusterNS string) (workv1.Manifest, error) {
	genStr := strconv.FormatInt(input.Generation, 10)
	apiHostname := fmt.Sprintf("api.%s-%s.%s", input.ClusterName, input.Slug, input.BaseDomain)
	oauthHostname := fmt.Sprintf("oauth.%s-%s.%s", input.ClusterName, input.Slug, input.BaseDomain)

	annotations := map[string]any{
		"hyperfleet.io/generation": genStr,
	}
	if input.CPOImage != "" {
		annotations["hypershift.openshift.io/control-plane-operator-image"] = input.CPOImage
	}
	if input.CAPGImage != "" {
		annotations["hypershift.openshift.io/cluster-api-provider-gcp-image"] = input.CAPGImage
	}

	obj := map[string]any{
		"apiVersion": "hypershift.openshift.io/v1beta1",
		"kind":       "HostedCluster",
		"metadata": map[string]any{
			"name":      input.ClusterName,
			"namespace": clusterNS,
			"labels": map[string]any{
				"hyperfleet.io/cluster-id": input.ClusterID,
			},
			"annotations": annotations,
		},
		"spec": map[string]any{
			"clusterID":   input.ClusterIDUUID,
			"infraID":     input.InfraID,
			"issuerURL":   input.IssuerURL,
			"releaseImage": input.ReleaseImage,
			"controllerAvailabilityPolicy": input.ControllerAvailabilityPolicy,
			"pullSecret": map[string]any{
				"name": "pull-secret",
			},
			"networking": map[string]any{
				"clusterNetwork": []any{
					map[string]any{"cidr": "10.132.0.0/14"},
				},
				"serviceNetwork": []any{
					map[string]any{"cidr": "172.31.0.0/16"},
				},
				"networkType": "OVNKubernetes",
			},
			"platform": map[string]any{
				"type": "GCP",
				"gcp": map[string]any{
					"projectID":      input.GCPProjectID,
					"region":         input.GCPRegion,
					"network":        input.GCPNetwork,
					"subnet":         input.GCPSubnet,
					"endpointAccess": input.GCPEndpointAccess,
					"workloadIdentity": map[string]any{
						"projectNumber": input.WIFProjectNumber,
						"poolID":        input.WIFPoolID,
						"providerID":    input.WIFProviderID,
						"serviceAccountsRef": map[string]any{
							"nodePoolEmail":        input.NodePoolEmail,
							"controlPlaneEmail":    input.ControlPlaneEmail,
							"cloudControllerEmail": input.CloudControllerEmail,
							"storageEmail":         input.StorageEmail,
							"imageRegistryEmail":   input.ImageRegistryEmail,
							"networkEmail":         input.NetworkEmail,
						},
					},
				},
			},
			"services": []any{
				map[string]any{
					"service": "APIServer",
					"servicePublishingStrategy": map[string]any{
						"type": "Route",
						"route": map[string]any{
							"hostname": apiHostname,
						},
					},
				},
				map[string]any{
					"service": "OAuthServer",
					"servicePublishingStrategy": map[string]any{
						"type": "Route",
						"route": map[string]any{
							"hostname": oauthHostname,
						},
					},
				},
				map[string]any{
					"service": "Konnectivity",
					"servicePublishingStrategy": map[string]any{
						"type": "Route",
					},
				},
				map[string]any{
					"service": "Ignition",
					"servicePublishingStrategy": map[string]any{
						"type": "Route",
					},
				},
			},
			"capabilities": map[string]any{
				"disabled": []any{
					"ImageRegistry",
					"Console",
					"Ingress",
				},
			},
			"configuration": map[string]any{
				"apiServer": map[string]any{
					"servingCerts": map[string]any{
						"namedCertificates": []any{
							map[string]any{
								"names": []any{apiHostname},
								"servingCertificate": map[string]any{
									"name": "external-api-cert",
								},
							},
						},
					},
				},
				"authentication": map[string]any{
					"type": "OIDC",
					"oidcProviders": []any{
						map[string]any{
							"name": "google",
							"issuer": map[string]any{
								"issuerURL": "https://accounts.google.com",
								"audiences": []any{
									"32555940559.apps.googleusercontent.com",
								},
							},
							"claimMappings": map[string]any{
								"username": map[string]any{
									"claim": "email",
								},
								"groups": map[string]any{
									"claim":  "hd",
									"prefix": "",
								},
							},
						},
					},
				},
			},
		},
	}
	return toManifest(obj)
}

func buildRBACJob(input Input, hcNS string) (workv1.Manifest, error) {
	genStr := strconv.FormatInt(input.Generation, 10)
	jobName := fmt.Sprintf("rbac-setup-gen-%d", input.Generation)
	ttl := int64(300)
	backoffLimit := int32(10)
	activeDeadline := int64(1800)
	clusterAdminScript := fmt.Sprintf(`set -euo pipefail
# Wait for API server (up to 25 minutes)
for i in $(seq 1 150); do
  if oc --kubeconfig /etc/kubeconfig/kubeconfig get namespace 2>/dev/null; then
    break
  fi
  echo "Waiting for API server... attempt $i"
  sleep 10
done

oc --kubeconfig /etc/kubeconfig/kubeconfig create clusterrolebinding hyperfleet-admins \
  --clusterrole=cluster-admin \
  --group=redhat.com \
  --user=%s || true
`, input.CreatedBy)

	obj := map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]any{
			"name":      jobName,
			"namespace": hcNS,
			"annotations": map[string]any{
				"hyperfleet.io/generation": genStr,
			},
		},
		"spec": map[string]any{
			"ttlSecondsAfterFinished": ttl,
			"backoffLimit":            backoffLimit,
			"activeDeadlineSeconds":   activeDeadline,
			"template": map[string]any{
				"spec": map[string]any{
					"serviceAccountName": "default",
					"restartPolicy":      "OnFailure",
					"containers": []any{
						map[string]any{
							"name":    "rbac-setup",
							"image":   "quay.io/openshift/origin-cli:4.20",
							"command": []any{"/bin/bash", "-c", clusterAdminScript},
							"volumeMounts": []any{
								map[string]any{
									"name":      "kubeconfig",
									"mountPath": "/etc/kubeconfig",
									"readOnly":  true,
								},
							},
						},
					},
					"volumes": []any{
						map[string]any{
							"name": "kubeconfig",
							"secret": map[string]any{
								"secretName": "service-network-admin-kubeconfig",
							},
						},
					},
				},
			},
		},
	}
	return toManifest(obj)
}

func buildManifestConfigs(clusterID, clusterName, clusterNS, hcNS string) []workv1.ManifestConfigOption {
	mwName := fmt.Sprintf("%s-hc-adapter", clusterID)
	_ = mwName
	_ = hcNS

	ssaStrategy := &workv1.UpdateStrategy{
		Type: workv1.UpdateStrategyTypeServerSideApply,
	}

	createOnlyStrategy := &workv1.UpdateStrategy{
		Type: workv1.UpdateStrategyTypeCreateOnly,
	}

	return []workv1.ManifestConfigOption{
		// Namespace: ServerSideApply + phase feedback
		{
			ResourceIdentifier: workv1.ResourceIdentifier{
				Group:     "",
				Resource:  "namespaces",
				Name:      clusterNS,
				Namespace: "",
			},
			UpdateStrategy: ssaStrategy,
			FeedbackRules: []workv1.FeedbackRule{
				{
					Type: workv1.JSONPathsType,
					JsonPaths: []workv1.JsonPath{
						{Name: "phase", Path: ".status.phase"},
					},
				},
			},
		},
		// Job: CreateOnly (immutable fields)
		{
			ResourceIdentifier: workv1.ResourceIdentifier{
				Group:     "batch",
				Resource:  "jobs",
				Name:      fmt.Sprintf("rbac-setup-gen-%d", 0), // placeholder; actual name varies
				Namespace: hcNS,
			},
			UpdateStrategy: createOnlyStrategy,
		},
		// HostedCluster: ServerSideApply + status feedback
		{
			ResourceIdentifier: workv1.ResourceIdentifier{
				Group:     "hypershift.openshift.io",
				Resource:  "hostedclusters",
				Name:      clusterName,
				Namespace: clusterNS,
			},
			UpdateStrategy: ssaStrategy,
			FeedbackRules: []workv1.FeedbackRule{
				{
					Type: workv1.JSONPathsType,
					JsonPaths: []workv1.JsonPath{
						{Name: "availableCondition", Path: ".status.conditions[?(@.type==\"Available\")].status"},
						{Name: "degradedCondition", Path: ".status.conditions[?(@.type==\"Degraded\")].status"},
						{Name: "controlPlaneEndpoint", Path: ".status.controlPlaneEndpoint.host"},
						{Name: "version", Path: ".status.version.desired.image"},
					},
				},
			},
		},
	}
}
