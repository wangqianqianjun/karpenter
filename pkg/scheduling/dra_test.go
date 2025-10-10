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
	"context"
	"testing"

	resourcev1 "k8s.io/api/resource/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDRAValidator_CanScheduleWithDRA_NoClaims(t *testing.T) {
	validator := NewDRAValidator(fake.NewClientBuilder().Build())

	canSchedule, _, err := validator.CanScheduleWithDRA(context.Background(), nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !canSchedule {
		t.Error("expected canSchedule=true when there are no claims")
	}
}

func TestDRAValidator_CanScheduleWithDRA_ClaimsButNoSlices(t *testing.T) {
	validator := NewDRAValidator(fake.NewClientBuilder().Build())

	claims := []*resourcev1.ResourceClaim{
		{
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{
					Requests: []resourcev1.DeviceRequest{
						{
							Name: "gpu",
							Exactly: &resourcev1.ExactDeviceRequest{
								DeviceClassName: "gpu-class",
								Count:           1,
							},
						},
					},
				},
			},
		},
	}

	canSchedule, _, err := validator.CanScheduleWithDRA(context.Background(), nil, nil, nil, claims, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if canSchedule {
		t.Error("expected canSchedule=false when claims exist but no resource slices")
	}
}

func TestDRAValidator_CanScheduleWithDRA_SimpleMatch(t *testing.T) {
	// Create a DeviceClass to satisfy the claim
	deviceClass := &resourcev1.DeviceClass{}
	deviceClass.Name = "gpu-class"

	// Build fake client with the DeviceClass
	fakeClient := fake.NewClientBuilder().
		WithObjects(deviceClass).
		Build()

	validator := NewDRAValidator(fakeClient)

	claims := []*resourcev1.ResourceClaim{
		{
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{
					Requests: []resourcev1.DeviceRequest{
						{
							Name: "gpu",
							Exactly: &resourcev1.ExactDeviceRequest{
								DeviceClassName: "gpu-class",
								AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
								Count:           2,
							},
						},
					},
				},
			},
		},
	}

	allNodes := true
	slices := []resourcev1.ResourceSliceSpec{
		{
			Driver: "gpu.example.com",
			Pool: resourcev1.ResourcePool{
				Name:               "gpu-pool",
				ResourceSliceCount: 1,
			},
			AllNodes: &allNodes,
			Devices: []resourcev1.Device{
				{Name: "gpu-0"},
				{Name: "gpu-1"},
				{Name: "gpu-2"},
			},
		},
	}

	canSchedule, _, err := validator.CanScheduleWithDRA(context.Background(), nil, nil, nil, claims, slices, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !canSchedule {
		t.Error("expected canSchedule=true when there are enough devices (3 available, requesting 2)")
	}
}

func TestDRAValidator_CanScheduleWithDRA_InsufficientDevices(t *testing.T) {
	// Create a DeviceClass to satisfy the claim
	deviceClass := &resourcev1.DeviceClass{}
	deviceClass.Name = "gpu-class"

	// Build fake client with the DeviceClass
	fakeClient := fake.NewClientBuilder().
		WithObjects(deviceClass).
		Build()

	validator := NewDRAValidator(fakeClient)

	claims := []*resourcev1.ResourceClaim{
		{
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{
					Requests: []resourcev1.DeviceRequest{
						{
							Name: "gpu",
							Exactly: &resourcev1.ExactDeviceRequest{
								DeviceClassName:  "gpu-class",
								AllocationMode:   resourcev1.DeviceAllocationModeExactCount,
								Count:            5,
							},
						},
					},
				},
			},
		},
	}

	allNodes := true
	slices := []resourcev1.ResourceSliceSpec{
		{
			Driver: "gpu.example.com",
			Pool: resourcev1.ResourcePool{
				Name:               "gpu-pool",
				ResourceSliceCount: 1,
			},
			AllNodes: &allNodes,
			Devices: []resourcev1.Device{
				{Name: "gpu-0"},
				{Name: "gpu-1"},
			},
		},
	}

	canSchedule, _, err := validator.CanScheduleWithDRA(context.Background(), nil, nil, nil, claims, slices, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if canSchedule {
		t.Error("expected canSchedule=false when there are insufficient devices")
	}
}

func TestDRAValidator_CanScheduleWithDRA_MissingDeviceClass(t *testing.T) {
	// Intentionally do NOT provide a DeviceClass to test configuration error handling
	validator := NewDRAValidator(fake.NewClientBuilder().Build())

	claims := []*resourcev1.ResourceClaim{
		{
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{
					Requests: []resourcev1.DeviceRequest{
						{
							Name: "gpu",
							Exactly: &resourcev1.ExactDeviceRequest{
								DeviceClassName: "missing-class",
								AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
								Count:           1,
							},
						},
					},
				},
			},
		},
	}

	allNodes := true
	slices := []resourcev1.ResourceSliceSpec{
		{
			Driver: "gpu.example.com",
			Pool: resourcev1.ResourcePool{
				Name:               "gpu-pool",
				ResourceSliceCount: 1,
			},
			AllNodes: &allNodes,
			Devices: []resourcev1.Device{
				{Name: "gpu-0"},
			},
		},
	}

	canSchedule, _, err := validator.CanScheduleWithDRA(context.Background(), nil, nil, nil, claims, slices, nil)
	if err == nil {
		t.Error("expected error when DeviceClass is missing, got nil")
	}
	if canSchedule {
		t.Error("expected canSchedule=false when there is a configuration error")
	}
}
