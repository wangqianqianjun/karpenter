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
	"testing"

	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/karpenter/pkg/apis/v1alpha1"
)

func TestDRAResourceManager_BuildCache(t *testing.T) {
	manager := NewDRAResourceManager()

	overlays := []*v1alpha1.NodeOverlay{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "gpu-overlay-1"},
			Spec: v1alpha1.NodeOverlaySpec{
				DRAResources: []v1alpha1.DRAResource{
					{
						InstanceType: "g5.xlarge",
						ResourceSlices: []resourcev1.ResourceSliceSpec{
							{
								Driver: "gpu.nvidia.com",
								Pool: resourcev1.ResourcePool{
									Name:               "gpu-pool",
									Generation:         1,
									ResourceSliceCount: 1,
								},
								Devices: []resourcev1.Device{
									{Name: "gpu-0"},
									{Name: "gpu-1"},
								},
							},
						},
					},
					{
						InstanceType: "g5.2xlarge",
						ResourceSlices: []resourcev1.ResourceSliceSpec{
							{
								Driver: "gpu.nvidia.com",
								Pool: resourcev1.ResourcePool{
									Name:               "gpu-pool",
									Generation:         1,
									ResourceSliceCount: 1,
								},
								Devices: []resourcev1.Device{
									{Name: "gpu-0"},
									{Name: "gpu-1"},
									{Name: "gpu-2"},
									{Name: "gpu-3"},
								},
							},
						},
					},
				},
			},
		},
	}

	manager.BuildCache(overlays)

	// Test GetResourceSlices
	slices := manager.GetResourceSlices("g5.xlarge")
	if len(slices) != 1 {
		t.Errorf("expected 1 slice for g5.xlarge, got %d", len(slices))
	}
	if len(slices[0].Devices) != 2 {
		t.Errorf("expected 2 devices for g5.xlarge, got %d", len(slices[0].Devices))
	}

	slices = manager.GetResourceSlices("g5.2xlarge")
	if len(slices) != 1 {
		t.Errorf("expected 1 slice for g5.2xlarge, got %d", len(slices))
	}
	if len(slices[0].Devices) != 4 {
		t.Errorf("expected 4 devices for g5.2xlarge, got %d", len(slices[0].Devices))
	}

	// Test HasDRAResources
	if !manager.HasDRAResources("g5.xlarge") {
		t.Error("expected g5.xlarge to have DRA resources")
	}
	if !manager.HasDRAResources("g5.2xlarge") {
		t.Error("expected g5.2xlarge to have DRA resources")
	}
	if manager.HasDRAResources("m5.large") {
		t.Error("expected m5.large to not have DRA resources")
	}
}

func TestDRAResourceManager_MultipleOverlays(t *testing.T) {
	manager := NewDRAResourceManager()

	// Multiple overlays can contribute slices to the same instance type
	overlays := []*v1alpha1.NodeOverlay{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "overlay-1"},
			Spec: v1alpha1.NodeOverlaySpec{
				DRAResources: []v1alpha1.DRAResource{
					{
						InstanceType: "g5.xlarge",
						ResourceSlices: []resourcev1.ResourceSliceSpec{
							{
								Driver: "gpu.nvidia.com",
								Pool: resourcev1.ResourcePool{
									Name:               "pool-1",
									Generation:         1,
									ResourceSliceCount: 1,
								},
								Devices: []resourcev1.Device{{Name: "gpu-0"}},
							},
						},
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "overlay-2"},
			Spec: v1alpha1.NodeOverlaySpec{
				DRAResources: []v1alpha1.DRAResource{
					{
						InstanceType: "g5.xlarge",
						ResourceSlices: []resourcev1.ResourceSliceSpec{
							{
								Driver: "gpu.nvidia.com",
								Pool: resourcev1.ResourcePool{
									Name:               "pool-2",
									Generation:         1,
									ResourceSliceCount: 1,
								},
								Devices: []resourcev1.Device{{Name: "gpu-1"}},
							},
						},
					},
				},
			},
		},
	}

	manager.BuildCache(overlays)

	slices := manager.GetResourceSlices("g5.xlarge")
	if len(slices) != 2 {
		t.Errorf("expected 2 slices for g5.xlarge from multiple overlays, got %d", len(slices))
	}
}

func TestDRAResourceManager_UpdateInstanceType(t *testing.T) {
	manager := NewDRAResourceManager()

	// Initial build
	overlays := []*v1alpha1.NodeOverlay{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "overlay-1"},
			Spec: v1alpha1.NodeOverlaySpec{
				DRAResources: []v1alpha1.DRAResource{
					{
						InstanceType: "g5.xlarge",
						ResourceSlices: []resourcev1.ResourceSliceSpec{
							{
								Driver: "gpu.nvidia.com",
								Pool: resourcev1.ResourcePool{
									Name:               "pool-1",
									Generation:         1,
									ResourceSliceCount: 1,
								},
							},
						},
					},
				},
			},
		},
	}
	manager.BuildCache(overlays)

	if !manager.HasDRAResources("g5.xlarge") {
		t.Error("expected g5.xlarge to have DRA resources initially")
	}

	// Update with new slices
	newSlices := []resourcev1.ResourceSliceSpec{
		{
			Driver: "gpu.nvidia.com",
			Pool: resourcev1.ResourcePool{
				Name:               "pool-2",
				Generation:         2,
				ResourceSliceCount: 1,
			},
			Devices: []resourcev1.Device{{Name: "gpu-0"}, {Name: "gpu-1"}},
		},
	}
	manager.UpdateInstanceType("g5.xlarge", newSlices)

	slices := manager.GetResourceSlices("g5.xlarge")
	if len(slices) != 1 {
		t.Errorf("expected 1 slice after update, got %d", len(slices))
	}
	if slices[0].Pool.Generation != 2 {
		t.Errorf("expected generation 2, got %d", slices[0].Pool.Generation)
	}
	if len(slices[0].Devices) != 2 {
		t.Errorf("expected 2 devices, got %d", len(slices[0].Devices))
	}

	// Update with empty slices should remove the instance type
	manager.UpdateInstanceType("g5.xlarge", nil)
	if manager.HasDRAResources("g5.xlarge") {
		t.Error("expected g5.xlarge to not have DRA resources after empty update")
	}
}

func TestDRAResourceManager_GetAllInstanceTypes(t *testing.T) {
	manager := NewDRAResourceManager()

	overlays := []*v1alpha1.NodeOverlay{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "overlay-1"},
			Spec: v1alpha1.NodeOverlaySpec{
				DRAResources: []v1alpha1.DRAResource{
					{InstanceType: "g5.xlarge", ResourceSlices: []resourcev1.ResourceSliceSpec{{}}},
					{InstanceType: "g5.2xlarge", ResourceSlices: []resourcev1.ResourceSliceSpec{{}}},
					{InstanceType: "p4d.24xlarge", ResourceSlices: []resourcev1.ResourceSliceSpec{{}}},
				},
			},
		},
	}

	manager.BuildCache(overlays)

	instanceTypes := manager.GetAllInstanceTypes()
	if len(instanceTypes) != 3 {
		t.Errorf("expected 3 instance types, got %d", len(instanceTypes))
	}

	// Check all instance types are present
	expected := map[string]bool{
		"g5.xlarge":     false,
		"g5.2xlarge":    false,
		"p4d.24xlarge":  false,
	}
	for _, it := range instanceTypes {
		if _, ok := expected[it]; ok {
			expected[it] = true
		}
	}
	for it, found := range expected {
		if !found {
			t.Errorf("expected instance type %s not found", it)
		}
	}
}

func TestDRAResourceManager_Clear(t *testing.T) {
	manager := NewDRAResourceManager()

	overlays := []*v1alpha1.NodeOverlay{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "overlay-1"},
			Spec: v1alpha1.NodeOverlaySpec{
				DRAResources: []v1alpha1.DRAResource{
					{InstanceType: "g5.xlarge", ResourceSlices: []resourcev1.ResourceSliceSpec{{}}},
				},
			},
		},
	}

	manager.BuildCache(overlays)
	if !manager.HasDRAResources("g5.xlarge") {
		t.Error("expected g5.xlarge to have DRA resources before clear")
	}

	manager.Clear()
	if manager.HasDRAResources("g5.xlarge") {
		t.Error("expected g5.xlarge to not have DRA resources after clear")
	}
	if len(manager.GetAllInstanceTypes()) != 0 {
		t.Error("expected no instance types after clear")
	}
}
