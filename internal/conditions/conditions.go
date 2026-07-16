// Package conditions provides helpers for managing metav1.Condition slices.
package conditions

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// IsTrue returns true when the named condition exists and its Status is metav1.ConditionTrue.
func IsTrue(conditions []metav1.Condition, condType string) bool {
	for _, c := range conditions {
		if c.Type == condType {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}

// Set upserts a condition into the slice. It returns true if the slice was
// modified (condition added or its Status, Reason, or Message changed), and
// false if the existing condition was identical — allowing callers to skip a
// Status().Update() call and avoid triggering spurious watch events.
func Set(conditions *[]metav1.Condition, c metav1.Condition) bool {
	for i, existing := range *conditions {
		if existing.Type != c.Type {
			continue
		}
		// Found — check whether anything meaningful changed.
		if existing.Status == c.Status && existing.Reason == c.Reason && existing.Message == c.Message {
			return false
		}
		// Preserve LastTransitionTime when only Reason/Message changed.
		if existing.Status != c.Status {
			c.LastTransitionTime = metav1.Now()
		} else {
			c.LastTransitionTime = existing.LastTransitionTime
		}
		(*conditions)[i] = c
		return true
	}
	// New condition.
	c.LastTransitionTime = metav1.Now()
	*conditions = append(*conditions, c)
	return true
}
