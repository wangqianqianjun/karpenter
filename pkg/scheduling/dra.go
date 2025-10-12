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

// KubeClient returns the Kubernetes client for API access
func (v *DRAValidator) KubeClient() client.Client {
	return v.kubeClient
}

// CELCache returns the CEL expression cache
func (v *DRAValidator) CELCache() *dracel.Cache {
	return v.celCache
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

// allocateWithState is the core DRA allocation logic that validates whether new claims can be scheduled
// given a pre-computed AllocatedState. This is a private helper used by both legacy and cache-based paths.
func (v *DRAValidator) allocateWithState(
	ctx context.Context,
	allocatedState structured.AllocatedState,
	newClaims []*resourcev1.ResourceClaim,
	resourceSlices []resourcev1.ResourceSliceSpec,
	node *corev1.Node,
) (bool, []resourcev1.AllocationResult, error) {
	// If no new claims, always succeed
	if len(newClaims) == 0 {
		return true, nil, nil
	}

	// If no resource slices defined but claims exist, cannot schedule
	if len(resourceSlices) == 0 {
		return false, nil, nil
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
		slice.Name = fmt.Sprintf("slice-%d", i)
		slices = append(slices, slice)
	}

	// Create a DeviceClassLister
	classLister := &KubeDeviceClassLister{
		Client: v.kubeClient,
		Ctx:    ctx,
	}

	// Use all supported features
	features := GetDRAFeatures()

	// Create allocator with the provided AllocatedState
	allocator, err := structured.NewAllocator(
		ctx,
		features,
		allocatedState,
		classLister,
		slices,
		v.celCache,
	)
	if err != nil {
		return false, nil, fmt.Errorf("failed to create DRA allocator: %w", err)
	}

	// Try to allocate all new claims
	results, err := allocator.Allocate(ctx, node, newClaims)
	if err != nil {
		log.FromContext(ctx).WithValues("node", node.Name, "newClaimsCount", len(newClaims)).V(1).Info(fmt.Sprintf("DRA allocator failed, %s", err))
		return false, nil, fmt.Errorf("DRA allocator failed: %w", err)
	}

	// Check if all claims were allocated
	if len(results) != len(newClaims) {
		log.FromContext(ctx).WithValues("node", node.Name, "requestedClaims", len(newClaims), "allocatedClaims", len(results)).V(1).Info("DRA allocation incomplete")
		return false, nil, nil
	}

	log.FromContext(ctx).WithValues("node", node.Name, "allocatedClaims", len(results)).V(2).Info("DRA allocation successful")
	return true, results, nil
}

// CanScheduleWithDRA validates whether a pod with resource claims can be scheduled
// on a node with the given resource slices using the official DRA allocator.
//
// This unified method handles both existing nodes and inflight NodeClaims:
// 1. For existing nodes: build AllocatedState from ResourceClaim.Status.Allocation
// 2. For inflight NodeClaims: build AllocatedState from cached allocation results
// 3. Merge both sources if both are provided
//
// Parameters:
//   - existingClaims: resource claims already allocated on the node (from Status.Allocation)
//   - cachedResults: map of pod -> cached allocation results (for inflight NodeClaims)
//   - existingPods: pods already scheduled (needed when using cachedResults)
//   - newClaims: resource claims from the pod being scheduled
//   - resourceSlices: available device capacity on the instance type
//   - node: simulated node (can be nil for new nodes, will create a placeholder)
//
// Returns:
//   - canSchedule: whether the pod can be scheduled
//   - allocationResults: allocation results for the new pod (to be cached)
//   - error: any error encountered
func (v *DRAValidator) CanScheduleWithDRA(
	ctx context.Context,
	existingClaims []*resourcev1.ResourceClaim,
	cachedResults map[*corev1.Pod][]resourcev1.AllocationResult,
	existingPods []*corev1.Pod,
	newClaims []*resourcev1.ResourceClaim,
	resourceSlices []resourcev1.ResourceSliceSpec,
	node *corev1.Node,
) (bool, []resourcev1.AllocationResult, error) {
	// Start with AllocatedState from existing claims (for existing nodes)
	// This will be empty if existingClaims is nil/empty (for inflight NodeClaims)
	allocatedState := buildAllocatedState(existingClaims)

	// If we have cached results (inflight NodeClaim scenario), merge them into AllocatedState
	// This ensures allocation stability - cached results take precedence
	if cachedResults != nil && len(existingPods) > 0 {
		for _, existingPod := range existingPods {
			if results, ok := cachedResults[existingPod]; ok {
				// Use cached allocation results - DON'T re-allocate
				podClaims, err := v.GetPodResourceClaims(ctx, existingPod)
				if err != nil {
					return false, nil, fmt.Errorf("failed to get resource claims for existing pod %s: %w", existingPod.Name, err)
				}

				if len(podClaims.Claims) > 0 {
					// Update allocated state with cached results
					allocatedState = UpdateAllocatedStateFromResults(
						allocatedState,
						results,
						podClaims.Claims,
					)
				}
			}
		}
	}

	// Use the unified allocation logic
	return v.allocateWithState(ctx, allocatedState, newClaims, resourceSlices, node)
}

// SchedulePodWithDRA is a high-level helper that encapsulates the full DRA validation workflow
// for scheduling a pod to either an existing node or an inflight NodeClaim.
//
// This unified method supports two scenarios:
// 1. Existing nodes: Uses existingClaims from ResourceClaim.Status.Allocation + REAL ResourceSlices
// 2. Inflight NodeClaims: Uses cachedResults from previous allocations + VIRTUAL ResourceSlices
//
// Parameters:
//   - ctx: context for API calls and logging
//   - newPod: the pod being scheduled
//   - existingPods: pods already scheduled on the node/NodeClaim
//   - cachedResults: cached allocation results (for inflight NodeClaims, nil for existing nodes)
//   - resourceSlices: available ResourceSlices (REAL for existing nodes, VIRTUAL for inflight NodeClaims)
//   - node: the actual node object (can be nil for simulation)
//
// Returns:
//   - canSchedule: true if the pod can be scheduled
//   - allocationResults: the allocation results for the new pod (for caching)
//   - error: if there's a configuration issue or API failure
func (v *DRAValidator) SchedulePodWithDRA(
	ctx context.Context,
	newPod *corev1.Pod,
	existingPods []*corev1.Pod,
	cachedResults map[*corev1.Pod][]resourcev1.AllocationResult,
	resourceSlices []resourcev1.ResourceSliceSpec,
	node *corev1.Node,
) (bool, []resourcev1.AllocationResult, error) {
	// Check if pod has resource claims
	if len(newPod.Spec.ResourceClaims) == 0 {
		return true, nil, nil
	}

	// Get resource claims for the new pod
	newPodClaims, err := v.GetPodResourceClaims(ctx, newPod)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get resource claims for pod: %w", err)
	}

	// For existing nodes: collect existingClaims from Status.Allocation
	// For inflight NodeClaims: existingClaims will be nil, use cachedResults instead
	var existingClaims []*resourcev1.ResourceClaim
	if cachedResults == nil {
		// Existing node scenario: collect claims from all existing pods
		for _, existingPod := range existingPods {
			podClaims, err := v.GetPodResourceClaims(ctx, existingPod)
			if err != nil {
				return false, nil, fmt.Errorf("failed to get resource claims for existing pod %s: %w", existingPod.Name, err)
			}
			existingClaims = append(existingClaims, podClaims.Claims...)
		}
	}

	// Validate if this node can accommodate both existing and new claims
	canSchedule, allocationResults, err := v.CanScheduleWithDRA(
		ctx,
		existingClaims, // nil for inflight NodeClaims, populated for existing nodes
		cachedResults,  // nil for existing nodes, populated for inflight NodeClaims
		existingPods,   // used when cachedResults is not nil
		newPodClaims.Claims,
		resourceSlices,
		node,
	)
	if err != nil {
		return false, nil, fmt.Errorf("DRA validation failed: %w", err)
	}

	return canSchedule, allocationResults, nil
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

// EmptyAllocatedState creates an empty AllocatedState for initialization
func EmptyAllocatedState() structured.AllocatedState {
	return structured.AllocatedState{
		AllocatedDevices:         sets.New[structured.DeviceID](),
		AllocatedSharedDeviceIDs: sets.New[structured.SharedDeviceID](),
		AggregatedCapacity:       structured.NewConsumedCapacityCollection(),
	}
}

// UpdateAllocatedStateFromResults updates AllocatedState by adding allocation results.
// This is used to maintain state during sequential allocation simulation for inflight NodeClaims.
func UpdateAllocatedStateFromResults(
	current structured.AllocatedState,
	results []resourcev1.AllocationResult,
	claims []*resourcev1.ResourceClaim,
) structured.AllocatedState {
	// Clone current state
	newAllocatedDevices := current.AllocatedDevices.Clone()
	newSharedDevices := current.AllocatedSharedDeviceIDs.Clone()
	newCapacity := cloneConsumedCapacityCollection(current.AggregatedCapacity)

	for i, result := range results {
		claim := claims[i]
		shareID := claim.UID

		for _, deviceResult := range result.Devices.Results {
			deviceID := structured.MakeDeviceID(
				deviceResult.Driver,
				deviceResult.Pool,
				deviceResult.Device,
			)

			// Add to allocated devices
			newAllocatedDevices.Insert(deviceID)

			// Add to shared device IDs if we have a UID
			if shareID != "" {
				sharedDeviceID := structured.MakeSharedDeviceID(deviceID, &shareID)
				newSharedDevices.Insert(sharedDeviceID)
			}

			// Add consumed capacity if present
			if deviceResult.ConsumedCapacity != nil {
				capacityMap := make(map[resourcev1.QualifiedName]resource.Quantity)
				for capName, capQty := range deviceResult.ConsumedCapacity {
					capacityMap[capName] = capQty.DeepCopy()
				}
				deviceCap := structured.NewDeviceConsumedCapacity(deviceID, capacityMap)
				newCapacity.Insert(deviceCap)
			}
		}
	}

	return structured.AllocatedState{
		AllocatedDevices:         newAllocatedDevices,
		AllocatedSharedDeviceIDs: newSharedDevices,
		AggregatedCapacity:       newCapacity,
	}
}

// cloneConsumedCapacityCollection creates a deep copy of ConsumedCapacityCollection
func cloneConsumedCapacityCollection(
	collection structured.ConsumedCapacityCollection,
) structured.ConsumedCapacityCollection {
	newCollection := structured.NewConsumedCapacityCollection()
	for deviceID, capacity := range collection {
		// Clone the capacity map (note: ConsumedCapacity is map[QualifiedName]*Quantity)
		newCapacityMap := make(map[resourcev1.QualifiedName]*resource.Quantity)
		for key, qtyPtr := range capacity {
			if qtyPtr != nil {
				qtyCopy := qtyPtr.DeepCopy()
				newCapacityMap[key] = &qtyCopy
			}
		}
		newCollection[deviceID] = newCapacityMap
	}
	return newCollection
}

// KubeDeviceClassLister implements DeviceClassLister by querying the Kubernetes API server.
// This is exported so it can be used by other packages like nodeclaim.
type KubeDeviceClassLister struct {
	Client client.Client
	Ctx    context.Context
}

func (l *KubeDeviceClassLister) List() ([]*resourcev1.DeviceClass, error) {
	var classList resourcev1.DeviceClassList
	if err := l.Client.List(l.Ctx, &classList); err != nil {
		return nil, fmt.Errorf("failed to list device classes: %w", err)
	}

	result := make([]*resourcev1.DeviceClass, len(classList.Items))
	for i := range classList.Items {
		result[i] = &classList.Items[i]
	}
	return result, nil
}

func (l *KubeDeviceClassLister) Get(name string) (*resourcev1.DeviceClass, error) {
	var deviceClass resourcev1.DeviceClass
	if err := l.Client.Get(l.Ctx, client.ObjectKey{Name: name}, &deviceClass); err != nil {
		return nil, fmt.Errorf("failed to get device class %s: %w", name, err)
	}
	return &deviceClass, nil
}

// GetNodeResourceSlices queries real ResourceSlices from the cluster for a specific node.
// This is used for existing nodes where we need to match against real device allocations.
//
// IMPORTANT: This method requires a field index on ResourceSlice.spec.nodeName to be set up
// during operator initialization. Without the index, this query will be inefficient.
//
// Returns:
//   - ResourceSliceSpecs for the node
//   - error if query fails
func (v *DRAValidator) GetNodeResourceSlices(ctx context.Context, nodeName string) ([]resourcev1.ResourceSliceSpec, error) {
	var sliceList resourcev1.ResourceSliceList

	// Query ResourceSlices where spec.nodeName matches the node name
	// Note: This requires the field index "spec.nodeName" to be set up
	if err := v.kubeClient.List(ctx, &sliceList, client.MatchingFields{"spec.nodeName": nodeName}); err != nil {
		return nil, fmt.Errorf("failed to list ResourceSlices for node %s: %w", nodeName, err)
	}

	if len(sliceList.Items) == 0 {
		// Node has no ResourceSlices - this is normal for nodes without DRA resources
		log.FromContext(ctx).WithValues("node", nodeName).V(2).Info("node has no ResourceSlices")
		return nil, nil
	}

	// Convert to specs
	specs := make([]resourcev1.ResourceSliceSpec, len(sliceList.Items))
	for i, slice := range sliceList.Items {
		specs[i] = slice.Spec
	}

	log.FromContext(ctx).WithValues("node", nodeName, "sliceCount", len(specs)).V(2).Info("retrieved ResourceSlices for node")
	return specs, nil
}

// GetDRAFeatures returns the structured.Features to use for DRA allocation.
// This enables all supported DRA features for maximum compatibility.
func GetDRAFeatures() structured.Features {
	return structured.Features{
		AdminAccess:          true,
		PrioritizedList:      true,
		PartitionableDevices: true,
		DeviceTaints:         true,
		DeviceBinding:        true,
		DeviceStatus:         true,
		ConsumableCapacity:   true,
	}
}

// NewDRAAllocator creates a new DRA allocator using the official Kubernetes library.
// This is a convenience wrapper to simplify allocator creation.
func NewDRAAllocator(
	ctx context.Context,
	features structured.Features,
	allocatedState structured.AllocatedState,
	classLister interface {
		List() ([]*resourcev1.DeviceClass, error)
		Get(name string) (*resourcev1.DeviceClass, error)
	},
	slices []*resourcev1.ResourceSlice,
	celCache *dracel.Cache,
) (structured.Allocator, error) {
	return structured.NewAllocator(
		ctx,
		features,
		allocatedState,
		classLister,
		slices,
		celCache,
	)
}

// BuildCachedResultsForInstanceType extracts cached allocation results for a specific instance type
// from a two-dimensional cache structure (pod -> instance type -> results).
//
// This is used in NodeClaim scheduling where we need to track allocation results per pod per instance type,
// since multiple instance types may be compatible and we don't know which one will be selected until later.
//
// Parameters:
//   - allCachedResults: two-dimensional map [pod][instanceType] -> AllocationResults
//   - instanceTypeName: the specific instance type to extract results for
//
// Returns:
//   - A flat map [pod] -> AllocationResults containing only the results for the specified instance type
func BuildCachedResultsForInstanceType(
	allCachedResults map[*corev1.Pod]map[string][]resourcev1.AllocationResult,
	instanceTypeName string,
) map[*corev1.Pod][]resourcev1.AllocationResult {
	if allCachedResults == nil {
		return nil
	}

	result := make(map[*corev1.Pod][]resourcev1.AllocationResult)
	for pod, perInstanceType := range allCachedResults {
		if results, exists := perInstanceType[instanceTypeName]; exists {
			result[pod] = results
		}
	}

	if len(result) == 0 {
		return nil
	}

	return result
}
