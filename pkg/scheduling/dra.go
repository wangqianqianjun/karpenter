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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	dracel "k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// DRAValidator provides an abstraction for validating DRA resource claims
// against available resource slices during scheduling simulation.
// It wraps the official Kubernetes DRA allocator to reuse the allocation logic.
type DRAValidator struct {
	kubeClient client.Client
	celCache   *dracel.Cache
}

// NewDRAValidator creates a new DRA validator
func NewDRAValidator(kubeClient client.Client) *DRAValidator {
	return &DRAValidator{
		kubeClient: kubeClient,
		celCache:   dracel.NewCache(100, dracel.Features{}), // Cache size 100, no special features
	}
}

// PodResourceClaims represents the resolved resource claims for a pod
type PodResourceClaims struct {
	Claims []*resourcev1.ResourceClaim
}

// GetPodResourceClaims extracts and resolves resource claims from a pod's spec.
// It handles both direct ResourceClaim references and ResourceClaimTemplate references.
func (v *DRAValidator) GetPodResourceClaims(ctx context.Context, pod *corev1.Pod) (*PodResourceClaims, error) {
	if len(pod.Spec.ResourceClaims) == 0 {
		// No DRA claims, return empty
		return &PodResourceClaims{}, nil
	}

	result := &PodResourceClaims{
		Claims: make([]*resourcev1.ResourceClaim, 0, len(pod.Spec.ResourceClaims)),
	}

	for _, podClaim := range pod.Spec.ResourceClaims {
		var claim *resourcev1.ResourceClaim

		if podClaim.ResourceClaimName != nil {
			// Direct reference to ResourceClaim
			claim = &resourcev1.ResourceClaim{}
			if err := v.kubeClient.Get(ctx, client.ObjectKey{
				Namespace: pod.Namespace,
				Name:      *podClaim.ResourceClaimName,
			}, claim); err != nil {
				log.FromContext(ctx).WithValues("Pod", klog.KObj(pod), "claimName", *podClaim.ResourceClaimName).V(1).Info(fmt.Sprintf("failed to get ResourceClaim, %s", err))
				return nil, fmt.Errorf("failed to get ResourceClaim %s/%s: %w", pod.Namespace, *podClaim.ResourceClaimName, err)
			}
		} else if podClaim.ResourceClaimTemplateName != nil {
			// Reference to ResourceClaimTemplate - need to resolve the template
			template := &resourcev1.ResourceClaimTemplate{}
			if err := v.kubeClient.Get(ctx, client.ObjectKey{
				Namespace: pod.Namespace,
				Name:      *podClaim.ResourceClaimTemplateName,
			}, template); err != nil {
				log.FromContext(ctx).WithValues("Pod", klog.KObj(pod), "templateName", *podClaim.ResourceClaimTemplateName).V(1).Info(fmt.Sprintf("failed to get ResourceClaimTemplate, %s", err))
				return nil, fmt.Errorf("failed to get ResourceClaimTemplate %s/%s: %w", pod.Namespace, *podClaim.ResourceClaimTemplateName, err)
			}

			// Create a synthetic ResourceClaim from the template
			claim = &resourcev1.ResourceClaim{
				Spec: template.Spec.Spec,
			}
			// Set synthetic metadata for identification
			claim.Name = fmt.Sprintf("%s-%s", pod.Name, podClaim.Name)
			claim.Namespace = pod.Namespace
			claim.UID = metav1.UID(fmt.Sprintf("synthetic-%s-%s", pod.UID, podClaim.Name))
		}

		if claim != nil {
			result.Claims = append(result.Claims, claim)
		}
	}

	log.FromContext(ctx).WithValues("Pod", klog.KObj(pod), "claimsCount", len(result.Claims)).V(2).Info("retrieved pod resource claims")

	return result, nil
}

// CanScheduleWithDRA validates whether a pod with resource claims can be scheduled
// on a node with the given resource slices using the official DRA allocator.
//
// This method creates a DRA allocator with the available resource slices and attempts
// to allocate the claims. If allocation succeeds, the pod can be scheduled.
//
// Parameters:
//   - existingClaims: resource claims already allocated on the node
//   - newClaims: resource claims from the pod being scheduled
//   - resourceSlices: available device capacity on the instance type
//   - node: simulated node (can be nil for new nodes, will create a placeholder)
func (v *DRAValidator) CanScheduleWithDRA(
	ctx context.Context,
	existingClaims []*resourcev1.ResourceClaim,
	newClaims []*resourcev1.ResourceClaim,
	resourceSlices []resourcev1.ResourceSliceSpec,
	node *corev1.Node,
) (bool, error) {
	// If no new claims, always succeed
	if len(newClaims) == 0 {
		return true, nil
	}

	// If no resource slices defined but claims exist, cannot schedule
	if len(resourceSlices) == 0 {
		return false, nil
	}

	// Create a placeholder node if not provided
	if node == nil {
		node = &corev1.Node{}
		node.Name = "placeholder-node"
	}

	// Convert ResourceSliceSpecs to ResourceSlices
	slices := make([]*resourcev1.ResourceSlice, 0, len(resourceSlices))
	for i, spec := range resourceSlices {
		slice := &resourcev1.ResourceSlice{
			Spec: spec,
		}
		// Set a synthetic name for the slice
		slice.Name = fmt.Sprintf("slice-%d", i)
		slices = append(slices, slice)
	}

	// Build allocated state from existing claims
	allocatedState := buildAllocatedState(existingClaims)

	// Create a DeviceClassLister that fetches from the API server
	classLister := &kubeDeviceClassLister{
		client: v.kubeClient,
		ctx:    ctx,
	}

	// Use all supported features for maximum compatibility
	features := structured.Features{
		AdminAccess:          true,
		PrioritizedList:      true,
		PartitionableDevices: true,
		DeviceTaints:         true,
		DeviceBinding:        true,
		DeviceStatus:         true,
		ConsumableCapacity:   true,
	}

	// Create allocator with official DRA library
	allocator, err := structured.NewAllocator(
		ctx,
		features,
		allocatedState,
		classLister,
		slices,
		v.celCache,
	)
	if err != nil {
		return false, fmt.Errorf("failed to create DRA allocator: %w", err)
	}

	// Try to allocate all new claims
	results, err := allocator.Allocate(ctx, node, newClaims)
	if err != nil {
		// Allocation failed due to error - propagate the error for visibility
		// This could be due to configuration issues (CEL errors, missing DeviceClass, etc.)
		log.FromContext(ctx).WithValues("node", node.Name, "newClaimsCount", len(newClaims), "existingClaimsCount", len(existingClaims), "resourceSlicesCount", len(resourceSlices)).V(1).Info(fmt.Sprintf("DRA allocator failed, %s", err))
		return false, fmt.Errorf("DRA allocator failed: %w", err)
	}

	// Check if all claims were allocated
	if len(results) != len(newClaims) {
		// Some claims could not be allocated - insufficient device capacity
		log.FromContext(ctx).WithValues("node", node.Name, "requestedClaims", len(newClaims), "allocatedClaims", len(results), "existingClaims", len(existingClaims)).V(1).Info("DRA allocation incomplete")
		return false, nil
	}

	// Log successful allocation for debugging
	log.FromContext(ctx).WithValues("node", node.Name, "allocatedClaims", len(results), "existingClaims", len(existingClaims)).V(2).Info("DRA allocation successful")

	return true, nil
}

// ValidateNodeForPod is a high-level helper that encapsulates the full DRA validation workflow
// for scheduling a pod to an existing node. It collects existing claims, retrieves resource slices,
// and performs DRA validation.
//
// Parameters:
//   - ctx: context for API calls and logging
//   - newPod: the pod being scheduled
//   - existingPods: pods already scheduled on the node
//   - instanceType: the instance type of the node
//   - node: the actual node object (can be nil for simulation)
//   - draManager: manager for looking up ResourceSlices by instance type
//
// Returns:
//   - error if validation fails or if there's a configuration issue
func (v *DRAValidator) ValidateNodeForPod(
	ctx context.Context,
	newPod *corev1.Pod,
	existingPods []*corev1.Pod,
	instanceType string,
	node *corev1.Node,
	draManager interface {
		GetResourceSlices(instanceType string) []resourcev1.ResourceSliceSpec
	},
) error {
	// Check if pod has resource claims
	if len(newPod.Spec.ResourceClaims) == 0 {
		return nil
	}

	// Get resource claims for the new pod
	newPodClaims, err := v.GetPodResourceClaims(ctx, newPod)
	if err != nil {
		return fmt.Errorf("failed to get resource claims for pod: %w", err)
	}

	// Get existing resource claims from all pods already scheduled on this node
	var existingClaims []*resourcev1.ResourceClaim
	for _, existingPod := range existingPods {
		podClaims, err := v.GetPodResourceClaims(ctx, existingPod)
		if err != nil {
			return fmt.Errorf("failed to get resource claims for existing pod %s: %w", existingPod.Name, err)
		}
		existingClaims = append(existingClaims, podClaims.Claims...)
	}

	// Get ResourceSlices for this instance type
	resourceSlices := draManager.GetResourceSlices(instanceType)

	// Validate if this node can accommodate both existing and new claims
	canSchedule, err := v.CanScheduleWithDRA(
		ctx,
		existingClaims,
		newPodClaims.Claims,
		resourceSlices,
		node,
	)
	if err != nil {
		return fmt.Errorf("DRA validation failed: %w", err)
	}

	if !canSchedule {
		return fmt.Errorf("node does not satisfy DRA requirements")
	}

	return nil
}

// buildAllocatedState creates an AllocatedState from existing claims
func buildAllocatedState(existingClaims []*resourcev1.ResourceClaim) structured.AllocatedState {
	// Create sets to track allocated devices
	allocatedDevices := make(map[structured.DeviceID]struct{})
	allocatedSharedDeviceIDs := make(map[structured.SharedDeviceID]struct{})

	for _, claim := range existingClaims {
		if claim.Status.Allocation != nil {
			shareID := claim.UID

			for _, deviceResult := range claim.Status.Allocation.Devices.Results {
				deviceID := structured.MakeDeviceID(deviceResult.Driver, deviceResult.Pool, deviceResult.Device)
				allocatedDevices[deviceID] = struct{}{}

				sharedDeviceID := structured.MakeSharedDeviceID(deviceID, &shareID)
				allocatedSharedDeviceIDs[sharedDeviceID] = struct{}{}
			}
		}
	}

	// Build consumed capacity collection
	aggregatedCapacity := structured.NewConsumedCapacityCollection()

	for _, claim := range existingClaims {
		if claim.Status.Allocation != nil {
			for _, deviceResult := range claim.Status.Allocation.Devices.Results {
				deviceID := structured.MakeDeviceID(deviceResult.Driver, deviceResult.Pool, deviceResult.Device)

				// Find the request in claim spec to get capacity
				if deviceResult.Request != "" {
					for _, req := range claim.Spec.Devices.Requests {
						if req.Name == deviceResult.Request {
							if req.Exactly != nil && req.Exactly.Capacity != nil && req.Exactly.Capacity.Requests != nil {
								capacityMap := make(map[resourcev1.QualifiedName]resource.Quantity)
								for capName, capQty := range req.Exactly.Capacity.Requests {
									capacityMap[capName] = capQty
								}

								deviceConsumedCap := structured.NewDeviceConsumedCapacity(deviceID, capacityMap)

								// Insert to consumed capacity
								aggregatedCapacity.Insert(deviceConsumedCap)
							}
							break
						}
					}
				}
			}
		}
	}

	// Convert maps to sets
	allocatedDevicesSet := sets.New[structured.DeviceID]()
	for deviceID := range allocatedDevices {
		allocatedDevicesSet.Insert(deviceID)
	}

	allocatedSharedDeviceIDsSet := sets.New[structured.SharedDeviceID]()
	for sharedDeviceID := range allocatedSharedDeviceIDs {
		allocatedSharedDeviceIDsSet.Insert(sharedDeviceID)
	}

	// Create and return AllocatedState
	return structured.AllocatedState{
		AllocatedDevices:         allocatedDevicesSet,
		AllocatedSharedDeviceIDs: allocatedSharedDeviceIDsSet,
		AggregatedCapacity:       aggregatedCapacity,
	}
}

// kubeDeviceClassLister implements DeviceClassLister by querying the Kubernetes API server
type kubeDeviceClassLister struct {
	client client.Client
	ctx    context.Context
}

func (l *kubeDeviceClassLister) List() ([]*resourcev1.DeviceClass, error) {
	var classList resourcev1.DeviceClassList
	if err := l.client.List(l.ctx, &classList); err != nil {
		return nil, fmt.Errorf("failed to list device classes: %w", err)
	}

	result := make([]*resourcev1.DeviceClass, len(classList.Items))
	for i := range classList.Items {
		result[i] = &classList.Items[i]
	}
	return result, nil
}

func (l *kubeDeviceClassLister) Get(name string) (*resourcev1.DeviceClass, error) {
	var deviceClass resourcev1.DeviceClass
	if err := l.client.Get(l.ctx, client.ObjectKey{Name: name}, &deviceClass); err != nil {
		return nil, fmt.Errorf("failed to get device class %s: %w", name, err)
	}
	return &deviceClass, nil
}
