package manifest_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	workv1 "open-cluster-management.io/api/work/v1"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/manifest"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/constants"
)

// ─── test helpers ─────────────────────────────────────────────────────────────

// metaWithGen builds ObjectMeta carrying only the generation annotation.
func metaWithGen(genStr string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Annotations: map[string]string{constants.AnnotationGeneration: genStr},
	}
}

// objWithGen returns an Unstructured with the generation annotation set.
// apiVersion and kind are included so the object survives UnmarshalJSON validation.
func objWithGen(name, genStr string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
	}}
	obj.SetName(name)
	obj.SetNamespace("default")
	if genStr != "" {
		obj.SetAnnotations(map[string]string{constants.AnnotationGeneration: genStr})
	}
	return obj
}

// toManifest JSON-encodes an Unstructured into a workv1.Manifest.
func toManifest(t *testing.T, obj *unstructured.Unstructured) workv1.Manifest {
	t.Helper()
	b, err := json.Marshal(obj.Object)
	require.NoError(t, err)
	return workv1.Manifest{RawExtension: runtime.RawExtension{Raw: b}}
}

// makeWork builds a ManifestWork with the given MW-level generation annotation and manifests.
func makeWork(mwGenStr string, manifests ...workv1.Manifest) *workv1.ManifestWork {
	w := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{Name: "test-work"},
		Spec: workv1.ManifestWorkSpec{
			Workload: workv1.ManifestsTemplate{Manifests: manifests},
		},
	}
	if mwGenStr != "" {
		w.Annotations = map[string]string{constants.AnnotationGeneration: mwGenStr}
	}
	return w
}

// parentWithManifests builds an Unstructured whose spec.workload.manifests contains
// the objects' raw maps — matching the structure DiscoverNestedManifest expects.
func parentWithManifests(objs ...*unstructured.Unstructured) *unstructured.Unstructured {
	manifests := make([]interface{}, 0, len(objs))
	for _, o := range objs {
		manifests = append(manifests, o.Object)
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"spec": map[string]interface{}{
				"workload": map[string]interface{}{
					"manifests": manifests,
				},
			},
		},
	}
}

// parentWithStatus builds a parent containing a status.resourceStatus.manifests entry
// that correlates to the given name/namespace, with optional statusFeedback and conditions.
func parentWithStatus(name, namespace string, statusFeedback, conditions interface{}) *unstructured.Unstructured {
	entry := map[string]interface{}{
		"resourceMeta": map[string]interface{}{
			"name":      name,
			"namespace": namespace,
		},
	}
	if statusFeedback != nil {
		entry["statusFeedback"] = statusFeedback
	}
	if conditions != nil {
		entry["conditions"] = conditions
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"status": map[string]interface{}{
				"resourceStatus": map[string]interface{}{
					"manifests": []interface{}{entry},
				},
			},
		},
	}
}

// ─── CompareGenerations ────────────────────────────────────────────────────────

func TestCompareGenerations_Operations(t *testing.T) {
	tests := []struct {
		name            string
		newGen          int64
		existingGen     int64
		exists          bool
		wantOp          manifest.Operation
		wantNewGen      int64
		wantExistingGen int64
	}{
		{
			name:            "resource does not exist → create",
			newGen: 5, existingGen: 0, exists: false,
			wantOp: manifest.OperationCreate, wantNewGen: 5, wantExistingGen: 0,
		},
		{
			name:            "exists=false with non-zero existing arg → existing zeroed in decision",
			newGen: 3, existingGen: 7, exists: false,
			wantOp: manifest.OperationCreate, wantNewGen: 3, wantExistingGen: 0,
		},
		{
			name:            "generations match → skip",
			newGen: 4, existingGen: 4, exists: true,
			wantOp: manifest.OperationSkip, wantNewGen: 4, wantExistingGen: 4,
		},
		{
			name:            "both zero → skip (equal)",
			newGen: 0, existingGen: 0, exists: true,
			wantOp: manifest.OperationSkip, wantNewGen: 0, wantExistingGen: 0,
		},
		{
			name:            "new generation higher → update",
			newGen: 5, existingGen: 3, exists: true,
			wantOp: manifest.OperationUpdate, wantNewGen: 5, wantExistingGen: 3,
		},
		{
			name:            "new generation lower (rollback) → update",
			newGen: 2, existingGen: 5, exists: true,
			wantOp: manifest.OperationUpdate, wantNewGen: 2, wantExistingGen: 5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := manifest.CompareGenerations(tc.newGen, tc.existingGen, tc.exists)
			assert.Equal(t, tc.wantOp, d.Operation)
			assert.Equal(t, tc.wantNewGen, d.NewGeneration)
			assert.Equal(t, tc.wantExistingGen, d.ExistingGeneration)
			assert.NotEmpty(t, d.Reason, "Reason must always be set")
		})
	}
}

func TestCompareGenerations_ReasonContent(t *testing.T) {
	t.Run("create reason mentions resource not found", func(t *testing.T) {
		d := manifest.CompareGenerations(1, 0, false)
		assert.Contains(t, d.Reason, "not found")
	})

	t.Run("skip reason includes the matching generation number", func(t *testing.T) {
		d := manifest.CompareGenerations(7, 7, true)
		assert.Contains(t, d.Reason, "7")
	})

	t.Run("update reason shows both old and new generations", func(t *testing.T) {
		d := manifest.CompareGenerations(10, 3, true)
		assert.Contains(t, d.Reason, "3")
		assert.Contains(t, d.Reason, "10")
	})
}

// ─── GetGeneration ─────────────────────────────────────────────────────────────

func TestGetGeneration(t *testing.T) {
	tests := []struct {
		name    string
		meta    metav1.ObjectMeta
		wantGen int64
	}{
		{"nil annotations → 0", metav1.ObjectMeta{Annotations: nil}, 0},
		{"annotation absent → 0", metav1.ObjectMeta{Annotations: map[string]string{"other": "val"}}, 0},
		{"empty annotation value → 0", metaWithGen(""), 0},
		{"non-numeric annotation → 0", metaWithGen("not-a-number"), 0},
		{"annotation overflow (out of int64 range) → 0", metaWithGen("99999999999999999999"), 0},
		{"zero value → 0", metaWithGen("0"), 0},
		{"valid positive → parsed", metaWithGen("42"), 42},
		{"generation 1 → 1", metaWithGen("1"), 1},
		{"negative value → parsed as-is (no validation here)", metaWithGen("-5"), -5},
		{"max int64 → parsed correctly", metaWithGen("9223372036854775807"), 9223372036854775807},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantGen, manifest.GetGeneration(tc.meta))
		})
	}
}

// ─── GetGenerationFromUnstructured ────────────────────────────────────────────

func TestGetGenerationFromUnstructured(t *testing.T) {
	t.Run("nil object → 0", func(t *testing.T) {
		assert.Equal(t, int64(0), manifest.GetGenerationFromUnstructured(nil))
	})

	t.Run("no annotations field → 0", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		assert.Equal(t, int64(0), manifest.GetGenerationFromUnstructured(obj))
	})

	t.Run("annotation key absent → 0", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		obj.SetAnnotations(map[string]string{"other": "val"})
		assert.Equal(t, int64(0), manifest.GetGenerationFromUnstructured(obj))
	})

	t.Run("empty annotation value → 0", func(t *testing.T) {
		assert.Equal(t, int64(0), manifest.GetGenerationFromUnstructured(objWithGen("x", "")))
	})

	t.Run("non-numeric annotation → 0", func(t *testing.T) {
		obj := objWithGen("x", "")
		obj.SetAnnotations(map[string]string{constants.AnnotationGeneration: "abc"})
		assert.Equal(t, int64(0), manifest.GetGenerationFromUnstructured(obj))
	})

	t.Run("valid generation → parsed", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		obj.SetAnnotations(map[string]string{constants.AnnotationGeneration: "7"})
		assert.Equal(t, int64(7), manifest.GetGenerationFromUnstructured(obj))
	})
}

// ─── ValidateGeneration ────────────────────────────────────────────────────────

func TestValidateGeneration(t *testing.T) {
	t.Run("nil annotations → error mentioning annotation key", func(t *testing.T) {
		err := manifest.ValidateGeneration(metav1.ObjectMeta{Annotations: nil})
		require.Error(t, err)
		assert.Contains(t, err.Error(), constants.AnnotationGeneration)
	})

	t.Run("annotation key absent → error", func(t *testing.T) {
		err := manifest.ValidateGeneration(metav1.ObjectMeta{
			Annotations: map[string]string{"other": "val"},
		})
		require.Error(t, err)
	})

	t.Run("empty annotation value → error mentioning 'empty'", func(t *testing.T) {
		err := manifest.ValidateGeneration(metaWithGen(""))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})

	t.Run("non-numeric value → error mentioning 'invalid'", func(t *testing.T) {
		err := manifest.ValidateGeneration(metaWithGen("not-a-number"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid")
	})

	t.Run("zero → error (generation must be > 0)", func(t *testing.T) {
		err := manifest.ValidateGeneration(metaWithGen("0"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "> 0")
	})

	t.Run("negative → error", func(t *testing.T) {
		require.Error(t, manifest.ValidateGeneration(metaWithGen("-1")))
	})

	t.Run("valid positive generation → no error", func(t *testing.T) {
		require.NoError(t, manifest.ValidateGeneration(metaWithGen("1")))
	})

	t.Run("large valid generation → no error", func(t *testing.T) {
		require.NoError(t, manifest.ValidateGeneration(metaWithGen("9999")))
	})
}

// ─── ValidateGenerationFromUnstructured ───────────────────────────────────────

func TestValidateGenerationFromUnstructured(t *testing.T) {
	t.Run("nil object → error mentioning 'nil'", func(t *testing.T) {
		err := manifest.ValidateGenerationFromUnstructured(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("nil annotations → error", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		err := manifest.ValidateGenerationFromUnstructured(obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), constants.AnnotationGeneration)
	})

	t.Run("annotation key absent → error", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		obj.SetAnnotations(map[string]string{"other": "val"})
		require.Error(t, manifest.ValidateGenerationFromUnstructured(obj))
	})

	t.Run("empty annotation value → error", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		obj.SetAnnotations(map[string]string{constants.AnnotationGeneration: ""})
		require.Error(t, manifest.ValidateGenerationFromUnstructured(obj))
	})

	t.Run("non-numeric value → error", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		obj.SetAnnotations(map[string]string{constants.AnnotationGeneration: "bad"})
		require.Error(t, manifest.ValidateGenerationFromUnstructured(obj))
	})

	t.Run("zero → error", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		obj.SetAnnotations(map[string]string{constants.AnnotationGeneration: "0"})
		require.Error(t, manifest.ValidateGenerationFromUnstructured(obj))
	})

	t.Run("negative → error", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		obj.SetAnnotations(map[string]string{constants.AnnotationGeneration: "-3"})
		require.Error(t, manifest.ValidateGenerationFromUnstructured(obj))
	})

	t.Run("valid positive generation → no error", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		obj.SetAnnotations(map[string]string{constants.AnnotationGeneration: "5"})
		require.NoError(t, manifest.ValidateGenerationFromUnstructured(obj))
	})
}

// ─── ValidateManifestWorkGeneration ───────────────────────────────────────────

func TestValidateManifestWorkGeneration(t *testing.T) {
	t.Run("nil ManifestWork → error mentioning 'nil'", func(t *testing.T) {
		err := manifest.ValidateManifestWorkGeneration(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("MW without generation annotation → error mentioning MW name", func(t *testing.T) {
		work := makeWork("") // no annotation
		err := manifest.ValidateManifestWorkGeneration(work)
		require.Error(t, err)
		assert.Contains(t, err.Error(), work.Name)
	})

	t.Run("MW with invalid (non-numeric) generation annotation → error", func(t *testing.T) {
		require.Error(t, manifest.ValidateManifestWorkGeneration(makeWork("not-a-number")))
	})

	t.Run("valid MW with no manifests → no error", func(t *testing.T) {
		require.NoError(t, manifest.ValidateManifestWorkGeneration(makeWork("1")))
	})

	t.Run("manifest with invalid JSON → error", func(t *testing.T) {
		work := makeWork("1", workv1.Manifest{RawExtension: runtime.RawExtension{Raw: []byte("not-json")}})
		require.Error(t, manifest.ValidateManifestWorkGeneration(work))
	})

	t.Run("manifest missing generation annotation → error mentioning manifest index", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]interface{}{"name": "cm", "namespace": "default"},
		}}
		work := makeWork("1", toManifest(t, obj))
		err := manifest.ValidateManifestWorkGeneration(work)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "manifest[0]")
	})

	t.Run("second manifest missing generation → error mentions correct index", func(t *testing.T) {
		obj1 := objWithGen("cm1", "1")
		obj2 := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]interface{}{"name": "cm2", "namespace": "default"},
		}} // no generation

		work := makeWork("1", toManifest(t, obj1), toManifest(t, obj2))
		err := manifest.ValidateManifestWorkGeneration(work)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "manifest[1]")
	})

	t.Run("all manifests carry valid generation → no error", func(t *testing.T) {
		obj1 := objWithGen("cm1", "2")
		obj2 := objWithGen("cm2", "2")

		work := makeWork("2", toManifest(t, obj1), toManifest(t, obj2))
		require.NoError(t, manifest.ValidateManifestWorkGeneration(work))
	})
}

// ─── GetLatestGenerationFromList ──────────────────────────────────────────────

func TestGetLatestGenerationFromList(t *testing.T) {
	t.Run("nil list → nil", func(t *testing.T) {
		assert.Nil(t, manifest.GetLatestGenerationFromList(nil))
	})

	t.Run("empty list → nil", func(t *testing.T) {
		assert.Nil(t, manifest.GetLatestGenerationFromList(&unstructured.UnstructuredList{}))
	})

	t.Run("single item → returned regardless of generation", func(t *testing.T) {
		list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{*objWithGen("only", "3")}}
		got := manifest.GetLatestGenerationFromList(list)
		require.NotNil(t, got)
		assert.Equal(t, "only", got.GetName())
	})

	t.Run("picks item with highest generation annotation", func(t *testing.T) {
		list := &unstructured.UnstructuredList{
			Items: []unstructured.Unstructured{
				*objWithGen("a", "1"),
				*objWithGen("b", "5"),
				*objWithGen("c", "3"),
			},
		}
		got := manifest.GetLatestGenerationFromList(list)
		require.NotNil(t, got)
		assert.Equal(t, "b", got.GetName())
	})

	t.Run("tie in generation → lexicographically first name wins (deterministic)", func(t *testing.T) {
		list := &unstructured.UnstructuredList{
			Items: []unstructured.Unstructured{
				*objWithGen("zebra", "5"),
				*objWithGen("alpha", "5"),
				*objWithGen("mango", "5"),
			},
		}
		got := manifest.GetLatestGenerationFromList(list)
		require.NotNil(t, got)
		assert.Equal(t, "alpha", got.GetName())
	})

	t.Run("items without generation annotation are treated as 0", func(t *testing.T) {
		noGen := &unstructured.Unstructured{Object: map[string]interface{}{}}
		noGen.SetName("no-gen")
		list := &unstructured.UnstructuredList{
			Items: []unstructured.Unstructured{
				*noGen,
				*objWithGen("with-gen", "2"),
			},
		}
		got := manifest.GetLatestGenerationFromList(list)
		require.NotNil(t, got)
		assert.Equal(t, "with-gen", got.GetName())
	})

	t.Run("does not reorder the original list", func(t *testing.T) {
		list := &unstructured.UnstructuredList{
			Items: []unstructured.Unstructured{
				*objWithGen("first", "1"),
				*objWithGen("second", "10"),
			},
		}
		_ = manifest.GetLatestGenerationFromList(list)
		assert.Equal(t, "first", list.Items[0].GetName(), "original list must not be mutated")
		assert.Equal(t, "second", list.Items[1].GetName())
	})
}

// ─── BuildLabelSelector ───────────────────────────────────────────────────────

func TestBuildLabelSelector(t *testing.T) {
	t.Run("nil map → empty string", func(t *testing.T) {
		assert.Equal(t, "", manifest.BuildLabelSelector(nil))
	})

	t.Run("empty map → empty string", func(t *testing.T) {
		assert.Equal(t, "", manifest.BuildLabelSelector(map[string]string{}))
	})

	t.Run("single label → key=value", func(t *testing.T) {
		assert.Equal(t, "env=prod", manifest.BuildLabelSelector(map[string]string{"env": "prod"}))
	})

	t.Run("multiple labels → sorted alphabetically by key", func(t *testing.T) {
		labels := map[string]string{"env": "prod", "app": "myapp", "team": "platform"}
		assert.Equal(t, "app=myapp,env=prod,team=platform", manifest.BuildLabelSelector(labels))
	})

	t.Run("output is deterministic across repeated calls", func(t *testing.T) {
		labels := map[string]string{"z": "last", "a": "first", "m": "middle"}
		assert.Equal(t, manifest.BuildLabelSelector(labels), manifest.BuildLabelSelector(labels))
	})
}

// ─── MatchesLabels ────────────────────────────────────────────────────────────

func TestMatchesLabels(t *testing.T) {
	objWithLabels := func(labels map[string]string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		obj.SetLabels(labels)
		return obj
	}

	t.Run("empty selector → matches any object (including unlabeled)", func(t *testing.T) {
		assert.True(t, manifest.MatchesLabels(objWithLabels(nil), ""))
		assert.True(t, manifest.MatchesLabels(objWithLabels(map[string]string{"app": "x"}), ""))
	})

	t.Run("object with no labels does not match a non-empty selector", func(t *testing.T) {
		assert.False(t, manifest.MatchesLabels(objWithLabels(nil), "app=myapp"))
	})

	t.Run("object labels satisfy all selector pairs → true", func(t *testing.T) {
		obj := objWithLabels(map[string]string{"app": "myapp", "env": "prod"})
		assert.True(t, manifest.MatchesLabels(obj, "app=myapp,env=prod"))
	})

	t.Run("extra labels on object beyond selector → still matches", func(t *testing.T) {
		obj := objWithLabels(map[string]string{"app": "myapp", "env": "prod", "extra": "label"})
		assert.True(t, manifest.MatchesLabels(obj, "app=myapp"))
	})

	t.Run("object missing a required selector label → false", func(t *testing.T) {
		obj := objWithLabels(map[string]string{"app": "myapp"})
		assert.False(t, manifest.MatchesLabels(obj, "app=myapp,env=prod"))
	})

	t.Run("label value mismatch → false", func(t *testing.T) {
		obj := objWithLabels(map[string]string{"app": "other"})
		assert.False(t, manifest.MatchesLabels(obj, "app=myapp"))
	})

	t.Run("malformed selector pair without '=' is silently skipped", func(t *testing.T) {
		// "noeq" has no '=' so it's skipped; "app=myapp" is checked and passes.
		obj := objWithLabels(map[string]string{"app": "myapp"})
		assert.True(t, manifest.MatchesLabels(obj, "noeq,app=myapp"))
	})
}

// ─── DiscoveryConfig ──────────────────────────────────────────────────────────

func TestDiscoveryConfig(t *testing.T) {
	t.Run("GetNamespace returns Namespace field", func(t *testing.T) {
		d := &manifest.DiscoveryConfig{Namespace: "kube-system"}
		assert.Equal(t, "kube-system", d.GetNamespace())
	})

	t.Run("GetName returns ByName field", func(t *testing.T) {
		d := &manifest.DiscoveryConfig{ByName: "my-resource"}
		assert.Equal(t, "my-resource", d.GetName())
	})

	t.Run("GetLabelSelector returns LabelSelector field", func(t *testing.T) {
		d := &manifest.DiscoveryConfig{LabelSelector: "app=foo"}
		assert.Equal(t, "app=foo", d.GetLabelSelector())
	})

	t.Run("IsSingleResource is true when ByName is set", func(t *testing.T) {
		assert.True(t, (&manifest.DiscoveryConfig{ByName: "something"}).IsSingleResource())
	})

	t.Run("IsSingleResource is false when ByName is empty", func(t *testing.T) {
		assert.False(t, (&manifest.DiscoveryConfig{}).IsSingleResource())
	})
}

// ─── MatchesDiscoveryCriteria ─────────────────────────────────────────────────

func TestMatchesDiscoveryCriteria(t *testing.T) {
	makeObj := func(name, namespace string, labels map[string]string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		obj.SetName(name)
		obj.SetNamespace(namespace)
		if labels != nil {
			obj.SetLabels(labels)
		}
		return obj
	}

	t.Run("no constraints → matches any object", func(t *testing.T) {
		obj := makeObj("anything", "any-ns", nil)
		assert.True(t, manifest.MatchesDiscoveryCriteria(obj, &manifest.DiscoveryConfig{}))
	})

	t.Run("namespace filter: same namespace → true", func(t *testing.T) {
		obj := makeObj("x", "target-ns", nil)
		assert.True(t, manifest.MatchesDiscoveryCriteria(obj, &manifest.DiscoveryConfig{Namespace: "target-ns"}))
	})

	t.Run("namespace filter: wrong namespace → false", func(t *testing.T) {
		obj := makeObj("x", "other-ns", nil)
		assert.False(t, manifest.MatchesDiscoveryCriteria(obj, &manifest.DiscoveryConfig{Namespace: "target-ns"}))
	})

	t.Run("single-resource by name: exact match → true", func(t *testing.T) {
		obj := makeObj("my-resource", "ns", nil)
		assert.True(t, manifest.MatchesDiscoveryCriteria(obj, &manifest.DiscoveryConfig{ByName: "my-resource"}))
	})

	t.Run("single-resource by name: no match → false", func(t *testing.T) {
		obj := makeObj("other", "ns", nil)
		assert.False(t, manifest.MatchesDiscoveryCriteria(obj, &manifest.DiscoveryConfig{ByName: "my-resource"}))
	})

	t.Run("label selector: labels match → true", func(t *testing.T) {
		obj := makeObj("x", "ns", map[string]string{"app": "myapp"})
		assert.True(t, manifest.MatchesDiscoveryCriteria(obj, &manifest.DiscoveryConfig{LabelSelector: "app=myapp"}))
	})

	t.Run("label selector: labels mismatch → false", func(t *testing.T) {
		obj := makeObj("x", "ns", map[string]string{"app": "other"})
		assert.False(t, manifest.MatchesDiscoveryCriteria(obj, &manifest.DiscoveryConfig{LabelSelector: "app=myapp"}))
	})

	t.Run("namespace check happens before name check: wrong namespace fails even if name matches", func(t *testing.T) {
		obj := makeObj("my-resource", "wrong-ns", nil)
		d := &manifest.DiscoveryConfig{Namespace: "right-ns", ByName: "my-resource"}
		assert.False(t, manifest.MatchesDiscoveryCriteria(obj, d))
	})
}

// ─── DiscoverNestedManifest ───────────────────────────────────────────────────

func TestDiscoverNestedManifest(t *testing.T) {
	t.Run("nil parent → empty list, no error", func(t *testing.T) {
		list, err := manifest.DiscoverNestedManifest(nil, &manifest.DiscoveryConfig{})
		require.NoError(t, err)
		assert.Empty(t, list.Items)
	})

	t.Run("nil discovery → empty list, no error", func(t *testing.T) {
		parent := parentWithManifests(objWithGen("x", "1"))
		list, err := manifest.DiscoverNestedManifest(parent, nil)
		require.NoError(t, err)
		assert.Empty(t, list.Items)
	})

	t.Run("parent without spec.workload.manifests → empty list", func(t *testing.T) {
		parent := &unstructured.Unstructured{Object: map[string]interface{}{}}
		list, err := manifest.DiscoverNestedManifest(parent, &manifest.DiscoveryConfig{ByName: "x"})
		require.NoError(t, err)
		assert.Empty(t, list.Items)
	})

	t.Run("discover by name: matching manifest returned", func(t *testing.T) {
		target := &unstructured.Unstructured{Object: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "target", "namespace": "ns"},
		}}
		other := &unstructured.Unstructured{Object: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "other", "namespace": "ns"},
		}}
		parent := parentWithManifests(target, other)

		list, err := manifest.DiscoverNestedManifest(parent, &manifest.DiscoveryConfig{ByName: "target"})
		require.NoError(t, err)
		require.Len(t, list.Items, 1)
		assert.Equal(t, "target", list.Items[0].GetName())
	})

	t.Run("discover by name: no match → empty list", func(t *testing.T) {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "other", "namespace": "ns"},
		}}
		list, err := manifest.DiscoverNestedManifest(parentWithManifests(obj), &manifest.DiscoveryConfig{ByName: "target"})
		require.NoError(t, err)
		assert.Empty(t, list.Items)
	})

	t.Run("discover by namespace: only manifests in that namespace returned", func(t *testing.T) {
		inNS := &unstructured.Unstructured{Object: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "in", "namespace": "target-ns"},
		}}
		outNS := &unstructured.Unstructured{Object: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "out", "namespace": "other-ns"},
		}}
		list, err := manifest.DiscoverNestedManifest(
			parentWithManifests(inNS, outNS),
			&manifest.DiscoveryConfig{Namespace: "target-ns"},
		)
		require.NoError(t, err)
		require.Len(t, list.Items, 1)
		assert.Equal(t, "in", list.Items[0].GetName())
	})

	t.Run("discover by label selector: all matching manifests returned", func(t *testing.T) {
		labeled := func(name string) *unstructured.Unstructured {
			return &unstructured.Unstructured{Object: map[string]interface{}{
				"metadata": map[string]interface{}{
					"name":      name,
					"namespace": "ns",
					"labels":    map[string]interface{}{"app": "myapp"},
				},
			}}
		}
		unlabeled := &unstructured.Unstructured{Object: map[string]interface{}{
			"metadata": map[string]interface{}{"name": "no-label", "namespace": "ns"},
		}}
		list, err := manifest.DiscoverNestedManifest(
			parentWithManifests(labeled("a"), unlabeled, labeled("b")),
			&manifest.DiscoveryConfig{LabelSelector: "app=myapp"},
		)
		require.NoError(t, err)
		assert.Len(t, list.Items, 2)
	})

	t.Run("non-map manifest entry → error", func(t *testing.T) {
		parent := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"metadata": map[string]interface{}{"name": "parent"},
				"spec": map[string]interface{}{
					"workload": map[string]interface{}{
						"manifests": []interface{}{"this-is-a-string-not-a-map"},
					},
				},
			},
		}
		_, err := manifest.DiscoverNestedManifest(parent, &manifest.DiscoveryConfig{})
		require.Error(t, err)
	})
}

// ─── EnrichWithResourceStatus ─────────────────────────────────────────────────

func TestEnrichWithResourceStatus(t *testing.T) {
	nestedObj := func(name, namespace string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
		obj.SetName(name)
		obj.SetNamespace(namespace)
		return obj
	}

	t.Run("nil parent → no-op, nested unchanged", func(t *testing.T) {
		nested := nestedObj("x", "ns")
		manifest.EnrichWithResourceStatus(nil, nested)
		_, hasStatus := nested.Object["statusFeedback"]
		assert.False(t, hasStatus)
	})

	t.Run("nil nested → no panic", func(t *testing.T) {
		parent := &unstructured.Unstructured{Object: map[string]interface{}{}}
		assert.NotPanics(t, func() {
			manifest.EnrichWithResourceStatus(parent, nil)
		})
	})

	t.Run("parent with no status → nested unchanged", func(t *testing.T) {
		parent := &unstructured.Unstructured{Object: map[string]interface{}{}}
		nested := nestedObj("x", "ns")
		manifest.EnrichWithResourceStatus(parent, nested)
		_, hasStatus := nested.Object["statusFeedback"]
		assert.False(t, hasStatus)
	})

	t.Run("matching entry → statusFeedback and conditions merged onto nested", func(t *testing.T) {
		sf := map[string]interface{}{"values": []interface{}{}}
		conds := []interface{}{map[string]interface{}{"type": "Applied", "status": "True"}}
		parent := parentWithStatus("my-hc", "hypershift", sf, conds)

		nested := nestedObj("my-hc", "hypershift")
		manifest.EnrichWithResourceStatus(parent, nested)

		assert.Equal(t, sf, nested.Object["statusFeedback"])
		assert.Equal(t, conds, nested.Object["conditions"])
	})

	t.Run("no entry matching nested name/namespace → nested unchanged", func(t *testing.T) {
		sf := map[string]interface{}{"values": []interface{}{}}
		parent := parentWithStatus("other-resource", "hypershift", sf, nil)

		nested := nestedObj("my-hc", "hypershift")
		manifest.EnrichWithResourceStatus(parent, nested)

		_, hasStatus := nested.Object["statusFeedback"]
		assert.False(t, hasStatus)
	})

	t.Run("entry has only statusFeedback (no conditions) → only statusFeedback merged", func(t *testing.T) {
		sf := map[string]interface{}{"values": []interface{}{}}
		parent := parentWithStatus("my-hc", "ns", sf, nil)
		nested := nestedObj("my-hc", "ns")

		manifest.EnrichWithResourceStatus(parent, nested)

		assert.Equal(t, sf, nested.Object["statusFeedback"])
		_, hasConds := nested.Object["conditions"]
		assert.False(t, hasConds, "conditions should not be set when not present in entry")
	})

	t.Run("entry has only conditions (no statusFeedback) → only conditions merged", func(t *testing.T) {
		conds := []interface{}{map[string]interface{}{"type": "Applied", "status": "True"}}
		parent := parentWithStatus("my-hc", "ns", nil, conds)
		nested := nestedObj("my-hc", "ns")

		manifest.EnrichWithResourceStatus(parent, nested)

		assert.Equal(t, conds, nested.Object["conditions"])
		_, hasSF := nested.Object["statusFeedback"]
		assert.False(t, hasSF, "statusFeedback should not be set when not present in entry")
	})
}