/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduling

import (
	"sync"

	resourcev1 "k8s.io/api/resource/v1"

	"sigs.k8s.io/karpenter/pkg/apis/v1alpha1"
)

// DRAResourceManager manages the mapping of instance types to their DRA resource slices.
// It maintains an in-memory cache built from NodeOverlay configurations.
type DRAResourceManager struct {
	mu sync.RWMutex
	// instanceTypeToSlices maps instance type name to its available resource slices
	instanceTypeToSlices map[string][]resourcev1.ResourceSliceSpec
}

// NewDRAResourceManager creates a new DRA resource manager
func NewDRAResourceManager() *DRAResourceManager {
	return &DRAResourceManager{
		instanceTypeToSlices: make(map[string][]resourcev1.ResourceSliceSpec),
	}
}

// BuildCache builds the cache from a list of NodeOverlays.
// This should be called when the scheduler is initialized or when NodeOverlays are updated.
func (m *DRAResourceManager) BuildCache(nodeOverlays []*v1alpha1.NodeOverlay) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clear existing cache
	m.instanceTypeToSlices = make(map[string][]resourcev1.ResourceSliceSpec)

	// Build cache from all NodeOverlays
	for _, overlay := range nodeOverlays {
		for _, draResource := range overlay.Spec.DRAResources {
			instanceType := draResource.InstanceType

			// Append resource slices for this instance type
			// Multiple overlays can contribute slices for the same instance type
			if _, exists := m.instanceTypeToSlices[instanceType]; !exists {
				m.instanceTypeToSlices[instanceType] = make([]resourcev1.ResourceSliceSpec, 0)
			}
			m.instanceTypeToSlices[instanceType] = append(
				m.instanceTypeToSlices[instanceType],
				draResource.ResourceSlices...,
			)
		}
	}
}

// GetResourceSlices returns the resource slices for a given instance type.
// Returns nil if the instance type has no DRA resources defined.
func (m *DRAResourceManager) GetResourceSlices(instanceType string) []resourcev1.ResourceSliceSpec {
	m.mu.RLock()
	defer m.mu.RUnlock()

	slices, exists := m.instanceTypeToSlices[instanceType]
	if !exists {
		return nil
	}

	// Return a copy to avoid external modifications
	result := make([]resourcev1.ResourceSliceSpec, len(slices))
	copy(result, slices)
	return result
}

// HasDRAResources checks if an instance type has any DRA resources defined
func (m *DRAResourceManager) HasDRAResources(instanceType string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	slices, exists := m.instanceTypeToSlices[instanceType]
	return exists && len(slices) > 0
}

// GetAllInstanceTypes returns all instance types that have DRA resources defined
func (m *DRAResourceManager) GetAllInstanceTypes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]string, 0, len(m.instanceTypeToSlices))
	for instanceType := range m.instanceTypeToSlices {
		result = append(result, instanceType)
	}
	return result
}

// UpdateInstanceType updates or adds DRA resources for a specific instance type.
// This can be used for incremental updates without rebuilding the entire cache.
func (m *DRAResourceManager) UpdateInstanceType(instanceType string, slices []resourcev1.ResourceSliceSpec) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(slices) == 0 {
		delete(m.instanceTypeToSlices, instanceType)
	} else {
		m.instanceTypeToSlices[instanceType] = slices
	}
}

// Clear clears the entire cache
func (m *DRAResourceManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.instanceTypeToSlices = make(map[string][]resourcev1.ResourceSliceSpec)
}
