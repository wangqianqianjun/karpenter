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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	opts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// NodeClaim is a set of constraints, compatible pods, and possible instance types that could fulfill these constraints. This
// will be turned into one or more actual node instances within the cluster after bin packing.
type NodeClaim struct {
	NodeClaimTemplate

	Pods               []*corev1.Pod
	reservationManager *ReservationManager
	topology           *Topology
	hostPortUsage      *scheduling.HostPortUsage
	daemonResources    corev1.ResourceList
	hostname           string

	// We store the reserved offerings rather than appending reservation ID labels for two reasons:
	// - We need to release any reservations that were made in previous iterations and are no longer compatible with the
	//   NodeClaim.
	// - Since other NodeClaims may have released reservations which are compatible with this NodeClaim since the last
	//   time a pod was scheduled, it's possible for the set of reserved offerings to expand as well as contract over
	//   multiple iterations. This has the benefit of maximizing the flexibility of an in-flight NodeClaim, maximizing
	//   the scheduler's binpacking efficiency. Tightening the NodeClaim's requirements before finalization would prevent
	//   this expansion.
	reservedOfferings    cloudprovider.Offerings
	reservedOfferingMode ReservedOfferingMode

	// DRA (Dynamic Resource Allocation) support
	draManager   *DRAResourceManager
	draValidator *scheduling.DRAValidator

	// draAllocationResults caches allocation results per pod per instance type.
	// This ensures allocation stability - once a pod's devices are allocated for a specific
	// instance type, they won't change even if the allocator is called again.
	//
	// We need TWO dimensions because:
	// 1. Multiple instance types may satisfy a pod's DRA requirements
	// 2. Each instance type may have different device pools/allocations
	// 3. We don't know which instance type will be selected until later
	//
	// Structure:
	//   First key: pod pointer (stable within a single scheduling cycle)
	//   Second key: instance type name (e.g., "gpu-small", "gpu-medium")
	//   Value: allocation results for that pod on that instance type
	//
	// Example:
	//   draAllocationResults[pod1]["gpu-small"] = [device-0, device-1]
	//   draAllocationResults[pod1]["gpu-medium"] = [device-3]
	//
	// Lifecycle: Created with NodeClaim, garbage collected when NodeClaim is discarded.
	// No manual cleanup needed as NodeClaim is a single-use object per scheduling cycle.
	draAllocationResults map[*corev1.Pod]map[string][]resourcev1.AllocationResult
}

// ReservedOfferingError indicates a NodeClaim couldn't be created or a pod couldn't be added to an exxisting NodeClaim
// due to
type ReservedOfferingError struct {
	error
}

func NewReservedOfferingError(err error) ReservedOfferingError {
	return ReservedOfferingError{error: err}
}

func IsReservedOfferingError(err error) bool {
	roe := &ReservedOfferingError{}
	return errors.As(err, roe)
}

func (e ReservedOfferingError) Unwrap() error {
	return e.error
}

var nodeID int64

func NewNodeClaim(
	nodeClaimTemplate *NodeClaimTemplate,
	topology *Topology,
	daemonResources corev1.ResourceList,
	hostPortUsage *scheduling.HostPortUsage,
	instanceTypes []*cloudprovider.InstanceType,
	reservationManager *ReservationManager,
	reservedOfferingMode ReservedOfferingMode,
	draManager *DRAResourceManager,
	draValidator *scheduling.DRAValidator,
) *NodeClaim {
	hostname := fmt.Sprintf("hostname-placeholder-%04d", atomic.AddInt64(&nodeID, 1))
	template := *nodeClaimTemplate
	template.Requirements = scheduling.NewRequirements()
	template.Requirements.Add(nodeClaimTemplate.Requirements.Values()...)
	template.Requirements.Add(scheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpIn, hostname))
	template.InstanceTypeOptions = instanceTypes
	template.Spec.Resources.Requests = daemonResources
	return &NodeClaim{
		NodeClaimTemplate:    template,
		hostPortUsage:        hostPortUsage,
		topology:             topology,
		daemonResources:      daemonResources,
		hostname:             hostname,
		reservedOfferings:    cloudprovider.Offerings{},
		reservationManager:   reservationManager,
		reservedOfferingMode: reservedOfferingMode,
		draManager:           draManager,
		draValidator:         draValidator,
		draAllocationResults: make(map[*corev1.Pod]map[string][]resourcev1.AllocationResult),
	}
}

// createPlaceholderNode creates a temporary node object for DRA validation simulation.
// This placeholder node is used to validate DRA requirements against instance types that don't
// have actual nodes yet. It includes the instance type label and copies relevant labels from
// the NodeClaimTemplate to ensure proper DRA validation (e.g., NodeSelector matching).
func (n *NodeClaim) createPlaceholderNode(instanceType *cloudprovider.InstanceType) *corev1.Node {
	labels := make(map[string]string)

	// Add instance type label (required for DRA validation)
	labels[corev1.LabelInstanceTypeStable] = instanceType.Name

	// Copy labels from NodeClaimTemplate that might be relevant for DRA NodeSelector matching
	// This includes zone, region, architecture, OS, and any custom labels
	for k, v := range n.Labels {
		labels[k] = v
	}

	// Copy requirements that can be represented as labels
	// This ensures NodeSelector matching works correctly in DRA validation
	for _, req := range n.Requirements.Values() {
		if req.Operator() == corev1.NodeSelectorOpIn && req.Len() == 1 {
			// Only add single-value In requirements as labels
			values := req.Values()
			if len(values) > 0 {
				labels[req.Key] = values[0]
			}
		}
	}

	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   fmt.Sprintf("karpenter-placeholder-%s-%s", instanceType.Name, n.hostname),
			Labels: labels,
		},
	}
}

// filterInstanceTypesByDRA filters instance types based on DRA (Dynamic Resource Allocation) requirements.
// It checks if each instance type has sufficient device resources to satisfy the pod's ResourceClaims,
// considering the resources already allocated to existing pods on the NodeClaim.
//
// This method uses cached allocation results to ensure allocation stability - once a pod is allocated
// devices, it always gets the same devices even across multiple CanAdd calls.
func (n *NodeClaim) filterInstanceTypesByDRA(
	ctx context.Context,
	instanceTypes []*cloudprovider.InstanceType,
	pod *corev1.Pod,
) ([]*cloudprovider.InstanceType, error) {
	// If no DRA components or pod has no resource claims, return all instance types
	if n.draManager == nil || n.draValidator == nil || len(pod.Spec.ResourceClaims) == 0 {
		return instanceTypes, nil
	}

	// Filter instance types by DRA capability using cached allocation results
	compatible := make([]*cloudprovider.InstanceType, 0, len(instanceTypes))
	for _, it := range instanceTypes {
		// Create a placeholder node for DRA validation simulation
		placeholderNode := n.createPlaceholderNode(it)

		// Build cached results for this specific instance type
		// This extracts only the allocation results relevant to this instance type
		// from the two-dimensional cache [pod][instanceType] -> results
		cachedResultsForIT := scheduling.BuildCachedResultsForInstanceType(n.draAllocationResults, it.Name)

		// Get VIRTUAL ResourceSlices for this instance type from NodeOverlay
		// For inflight NodeClaims, we use virtual ResourceSlices since the node doesn't exist yet
		resourceSlices := n.draManager.GetResourceSlices(it.Name)

		// Use the high-level SchedulePodWithDRA method
		// For inflight NodeClaims, we pass instance-type-specific cached results
		canSchedule, allocationResults, err := n.draValidator.SchedulePodWithDRA(
			ctx,
			pod,                // newPod: the pod being scheduled
			n.Pods,             // existingPods: pods already on this NodeClaim
			cachedResultsForIT, // cachedResults: cached results for THIS instance type only
			resourceSlices,     // resourceSlices: VIRTUAL ResourceSlices from NodeOverlay
			placeholderNode,    // node: placeholder node for simulation
		)
		if err != nil {
			return nil, fmt.Errorf("DRA validation failed for instance type %s: %w", it.Name, err)
		}

		if canSchedule {
			// Cache the allocation results for this pod + instance type combination
			// This ensures that if this pod is added to the NodeClaim with this instance type,
			// future CanAdd calls will use these same allocation results instead of re-allocating
			if n.draAllocationResults[pod] == nil {
				n.draAllocationResults[pod] = make(map[string][]resourcev1.AllocationResult)
			}
			n.draAllocationResults[pod][it.Name] = allocationResults
			compatible = append(compatible, it)
		}
	}

	return compatible, nil
}

// CanAdd returns whether the pod can be added to the NodeClaim
// based on the taints/tolerations, host port compatibility,
// requirements, resources, reserved capacity reservations, and topology requirements
func (n *NodeClaim) CanAdd(ctx context.Context, pod *corev1.Pod, podData *PodData, relaxMinValues bool) (updatedRequirements scheduling.Requirements, updatedInstanceTypes []*cloudprovider.InstanceType, offeringsToReserve []*cloudprovider.Offering, err error) {
	// Check Taints
	if err := scheduling.Taints(n.Spec.Taints).ToleratesPod(pod); err != nil {
		return nil, nil, nil, err
	}

	// exposed host ports on the node
	hostPorts := scheduling.GetHostPorts(pod)
	if err := n.hostPortUsage.Conflicts(pod, hostPorts); err != nil {
		return nil, nil, nil, fmt.Errorf("checking host port usage, %w", err)
	}
	nodeClaimRequirements := scheduling.NewRequirements(n.Requirements.Values()...)

	// Check NodeClaim Affinity Requirements
	if err := nodeClaimRequirements.Compatible(podData.Requirements, scheduling.AllowUndefinedWellKnownLabels); err != nil {
		return nil, nil, nil, fmt.Errorf("incompatible requirements, %w", err)
	}
	nodeClaimRequirements.Add(podData.Requirements.Values()...)

	// Check Topology Requirements
	topologyRequirements, err := n.topology.AddRequirements(pod, n.NodeClaimTemplate.Spec.Taints, podData.StrictRequirements, nodeClaimRequirements, scheduling.AllowUndefinedWellKnownLabels)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = nodeClaimRequirements.Compatible(topologyRequirements, scheduling.AllowUndefinedWellKnownLabels); err != nil {
		return nil, nil, nil, err
	}
	nodeClaimRequirements.Add(topologyRequirements.Values()...)

	// Check instance type combinations
	requests := resources.Merge(n.Spec.Resources.Requests, podData.Requests)

	remaining, unsatisfiableKeys, err := filterInstanceTypesByRequirements(n.InstanceTypeOptions, nodeClaimRequirements, podData.Requests, n.daemonResources, requests, relaxMinValues)
	if relaxMinValues {
		// Update min values on the requirements if they are relaxed
		for key, minValues := range unsatisfiableKeys {
			nodeClaimRequirements.Get(key).MinValues = lo.ToPtr(minValues)
		}
	}
	if err != nil {
		// We avoid wrapping this err because calling String() on InstanceTypeFilterError is an expensive operation
		// due to calls to resources.Merge and stringifying the nodeClaimRequirements
		return nil, nil, nil, err
	}

	// Filter by DRA (Dynamic Resource Allocation) requirements if pod has resource claims
	if len(pod.Spec.ResourceClaims) > 0 {
		remaining, err = n.filterInstanceTypesByDRA(ctx, remaining, pod)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("filtering instance types by DRA, %w", err)
		}
		if len(remaining) == 0 {
			return nil, nil, nil, fmt.Errorf("no instance types satisfy DRA requirements")
		}
	}

	ofs, err := n.offeringsToReserve(ctx, remaining, nodeClaimRequirements)
	if err != nil {
		return nil, nil, nil, err
	}
	return nodeClaimRequirements, remaining, ofs, nil
}

// Add updates the NodeClaim to schedule the pod to this NodeClaim, updating
// the NodeClaim with new requirements, instance types, and offerings to reserve
// based on the pod scheduling
func (n *NodeClaim) Add(pod *corev1.Pod, podData *PodData, nodeClaimRequirements scheduling.Requirements, instanceTypes []*cloudprovider.InstanceType, offeringsToReserve []*cloudprovider.Offering) {
	// Update node
	n.Pods = append(n.Pods, pod)
	n.InstanceTypeOptions = instanceTypes
	n.Spec.Resources.Requests = resources.Merge(n.Spec.Resources.Requests, podData.Requests)
	n.Requirements = nodeClaimRequirements
	n.topology.Register(corev1.LabelHostname, n.hostname)
	n.topology.Record(pod, n.NodeClaim.Spec.Taints, nodeClaimRequirements, scheduling.AllowUndefinedWellKnownLabels)
	n.hostPortUsage.Add(pod, scheduling.GetHostPorts(pod))
	n.reservationManager.Reserve(n.hostname, offeringsToReserve...)
	n.releaseReservedOfferings(n.reservedOfferings, offeringsToReserve)
	n.reservedOfferings = offeringsToReserve
}

// releaseReservedOfferings releases all offerings which are present in the current reserved offerings, but are not
// present in the updated reserved offerings.
func (n *NodeClaim) releaseReservedOfferings(current, updated cloudprovider.Offerings) {
	updatedIDs := sets.New[string]()
	for _, o := range updated {
		updatedIDs.Insert(o.ReservationID())
	}
	for _, o := range current {
		if !updatedIDs.Has(o.ReservationID()) {
			n.reservationManager.Release(n.hostname, o)
		}
	}
}

// reserveOfferings handles the reservation of `karpenter.sh/capacity-type: reserved` offerings, returning the set of
// reserved offerings. If the ReservedOfferingMode is set to strict, this function may also return an error if it failed
// to reserve compatible offerings when some were available.
//
//nolint:gocyclo
func (n *NodeClaim) offeringsToReserve(
	ctx context.Context,
	instanceTypes []*cloudprovider.InstanceType,
	nodeClaimRequirements scheduling.Requirements,
) (cloudprovider.Offerings, error) {
	if !opts.FromContext(ctx).FeatureGates.ReservedCapacity {
		return nil, nil
	}

	hasCompatibleOffering := false
	var reservedOfferings cloudprovider.Offerings
	for _, it := range instanceTypes {
		for _, o := range it.Offerings {
			if o.CapacityType() != v1.CapacityTypeReserved || !o.Available {
				continue
			}
			// Track every incompatible reserved offering for release. Since releasing a reservation is a no-op when there is no
			// reservation for the given host, there's no need to check that a reservation actually exists for the offering.
			if !nodeClaimRequirements.IsCompatible(o.Requirements, scheduling.AllowUndefinedWellKnownLabels) {
				continue
			}
			hasCompatibleOffering = true
			// Note that reservation is an idempotent operation - if we have previously successfully reserved an offering for
			// this host, this operation is guaranteed to succeed. We may also succeed to make reservations for offerings which
			// failed in previous iterations if other NodeClaims have released them since the last attempt.
			if n.reservationManager.CanReserve(n.hostname, o) {
				reservedOfferings = append(reservedOfferings, o)
			}
		}
	}

	if n.reservedOfferingMode == ReservedOfferingModeStrict {
		// If an instance type with a compatible reserved offering exists, but we failed to make any reservations, we should
		// fail. This could occur when all of the capacity for compatible instances has been reserved by previously created
		// nodeclaims. Since we reserve offering pessimistically, i.e. we will reserve any offering that the instance could
		// be launched with, we should fall back and attempt to schedule this pod in a subsequent scheduling simulation once
		// reservation capacity is available again.
		if hasCompatibleOffering && len(reservedOfferings) == 0 {
			return nil, NewReservedOfferingError(fmt.Errorf("one or more instance types with compatible reserved offerings are available, but could not be reserved"))
		}
		// If the nodeclaim previously had compatible reserved offerings, but the additional requirements filtered those out,
		// we should fail to add the pod to this nodeclaim.
		if len(n.reservedOfferings) != 0 && len(reservedOfferings) == 0 {
			return nil, NewReservedOfferingError(fmt.Errorf("satisfying updated nodeclaim constraints would remove all compatible reserved offering options"))
		}
	}
	return reservedOfferings, nil
}

// FinalizeScheduling is called once all scheduling has completed and allows the node to perform any cleanup
// necessary before its requirements are used for instance launching
func (n *NodeClaim) FinalizeScheduling() {
	// We need nodes to have hostnames for topology purposes, but we don't want to pass that node name on to consumers
	// of the node as it will be displayed in error messages
	delete(n.Requirements, corev1.LabelHostname)
	// If there are any reserved offerings tracked, inject those requirements onto the NodeClaim. This ensures that if
	// there are multiple reserved offerings for an instance type, we don't attempt to overlaunch into a single offering.
	if len(n.reservedOfferings) != 0 {
		// Tightening constraint to reserved ensures that we get automatic drift handling when the Node / NodeClaim's capacity
		// type label is dynamically updated by the cloudprovider.
		n.Requirements[v1.CapacityTypeLabelKey] = scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeReserved)
		n.Requirements.Add(scheduling.NewRequirement(
			cloudprovider.ReservationIDLabel,
			corev1.NodeSelectorOpIn,
			lo.Map(n.reservedOfferings, func(o *cloudprovider.Offering, _ int) string { return o.ReservationID() })...,
		))
	}
}

func (n *NodeClaim) RemoveInstanceTypeOptionsByPriceAndMinValues(reqs scheduling.Requirements, maxPrice float64) (*NodeClaim, error) {
	n.InstanceTypeOptions = lo.Filter(n.InstanceTypeOptions, func(it *cloudprovider.InstanceType, _ int) bool {
		launchPrice := it.Offerings.Available().WorstLaunchPrice(reqs)
		return launchPrice < maxPrice
	})
	if _, _, err := n.InstanceTypeOptions.SatisfiesMinValues(reqs); err != nil {
		return nil, err
	}
	return n, nil
}

func InstanceTypeList(instanceTypeOptions []*cloudprovider.InstanceType) string {
	var itSb strings.Builder
	for i, it := range instanceTypeOptions {
		// print the first 5 instance types only (indices 0-4)
		if i > 4 {
			fmt.Fprintf(&itSb, " and %d other(s)", len(instanceTypeOptions)-i)
			break
		} else if i > 0 {
			fmt.Fprint(&itSb, ", ")
		}
		fmt.Fprint(&itSb, it.Name)
	}
	return itSb.String()
}

type InstanceTypeFilterError struct {
	// Each of these three flags indicates if that particular criteria was met by at least one instance type
	requirementsMet bool
	fits            bool
	hasOffering     bool

	// requirementsAndFits indicates if a single instance type met the scheduling requirements and had enough resources
	requirementsAndFits bool
	// requirementsAndOffering indicates if a single instance type met the scheduling requirements and was a required offering
	requirementsAndOffering bool
	// fitsAndOffering indicates if a single instance type had enough resources and was a required offering
	fitsAndOffering          bool
	minValuesIncompatibleErr error

	// We capture requirements so that we can know what the requirements were when evaluating instance type compatibility
	requirements scheduling.Requirements
	// We capture podRequests here since when a pod can't schedule due to requests, it's because the pod
	// was on its own on the simulated Node and exceeded the available resources for any instance type for this NodePool
	podRequests corev1.ResourceList
	// We capture daemonRequests since this contributes to the resources that are required to schedule to this NodePool
	daemonRequests corev1.ResourceList
}

//nolint:gocyclo
func (e InstanceTypeFilterError) Error() string {
	// minValues is specified in the requirements and is not met
	if e.minValuesIncompatibleErr != nil {
		return fmt.Sprintf("%s, requirements=%s, resources=%s", e.minValuesIncompatibleErr.Error(), e.requirements, resources.String(resources.Merge(e.daemonRequests, e.podRequests)))
	}
	// no instance type met any of the three criteria, meaning each criteria was enough to completely prevent
	// this pod from scheduling
	if !e.requirementsMet && !e.fits && !e.hasOffering {
		return fmt.Sprintf("no instance type met the scheduling requirements or had enough resources or had a required offering, requirements=%s, resources=%s", e.requirements, resources.String(resources.Merge(e.daemonRequests, e.podRequests)))
	}
	// check the other pairwise criteria
	if !e.requirementsMet && !e.fits {
		return fmt.Sprintf("no instance type met the scheduling requirements or had enough resources, requirements=%s, resources=%s", e.requirements, resources.String(resources.Merge(e.daemonRequests, e.podRequests)))
	}
	if !e.requirementsMet && !e.hasOffering {
		return fmt.Sprintf("no instance type met the scheduling requirements or had a required offering, requirements=%s, resources=%s", e.requirements, resources.String(resources.Merge(e.daemonRequests, e.podRequests)))
	}
	if !e.fits && !e.hasOffering {
		return fmt.Sprintf("no instance type had enough resources or had a required offering, requirements=%s, resources=%s", e.requirements, resources.String(resources.Merge(e.daemonRequests, e.podRequests)))
	}
	// and then each individual criteria. These are sort of the same as above in that each one indicates that no
	// instance type matched that criteria at all, so it was enough to exclude all instance types.  I think it's
	// helpful to have these separate, since we can report the multiple excluding criteria above.
	if !e.requirementsMet {
		return fmt.Sprintf("no instance type met all requirements, requirements=%s, resources=%s", e.requirements, resources.String(resources.Merge(e.daemonRequests, e.podRequests)))
	}
	if !e.fits {
		msg := fmt.Sprintf("no instance type has enough resources, requirements=%s, resources=%s", e.requirements, resources.String(resources.Merge(e.daemonRequests, e.podRequests)))
		// special case for a user typo I saw reported once
		if e.podRequests.Cpu().Cmp(resource.MustParse("1M")) >= 0 {
			msg += " (CPU request >= 1 Million, m vs M typo?)"
		}
		return msg
	}
	if !e.hasOffering {
		return fmt.Sprintf("no instance type has the required offering, requirements=%s, resources=%s", e.requirements, resources.String(resources.Merge(e.daemonRequests, e.podRequests)))
	}
	// see if any pair of criteria was enough to exclude all instances
	if e.requirementsAndFits {
		return fmt.Sprintf("no instance type which met the scheduling requirements and had enough resources, had a required offering, requirements=%s, resources=%s", e.requirements, resources.String(resources.Merge(e.daemonRequests, e.podRequests)))
	}
	if e.fitsAndOffering {
		return fmt.Sprintf("no instance type which had enough resources and the required offering met the scheduling requirements, requirements=%s, resources=%s", e.requirements, resources.String(resources.Merge(e.daemonRequests, e.podRequests)))
	}
	if e.requirementsAndOffering {
		return fmt.Sprintf("no instance type which met the scheduling requirements and the required offering had the required resources, requirements=%s, resources=%s", e.requirements, resources.String(resources.Merge(e.daemonRequests, e.podRequests)))
	}
	// finally all instances were filtered out, but we had at least one instance that met each criteria, and met each
	// pairwise set of criteria, so the only thing that remains is no instance which met all three criteria simultaneously
	return fmt.Sprintf("no instance type met the requirements/resources/offering tuple, requirements=%s, resources=%s", e.requirements, resources.String(resources.Merge(e.daemonRequests, e.podRequests)))
}

//nolint:gocyclo
func filterInstanceTypesByRequirements(instanceTypes []*cloudprovider.InstanceType, requirements scheduling.Requirements, podRequests, daemonRequests, totalRequests corev1.ResourceList, relaxMinValues bool) (cloudprovider.InstanceTypes, map[string]int, error) {
	unsatisfiableKeys := map[string]int{}
	// We hold the results of our scheduling simulation inside of this InstanceTypeFilterError struct
	// to reduce the CPU load of having to generate the error string for a failed scheduling simulation
	err := InstanceTypeFilterError{
		requirementsMet: false,
		fits:            false,
		hasOffering:     false,

		requirementsAndFits:     false,
		requirementsAndOffering: false,
		fitsAndOffering:         false,

		requirements:   requirements,
		podRequests:    podRequests,
		daemonRequests: daemonRequests,
	}
	remaining := cloudprovider.InstanceTypes{}

	for _, it := range instanceTypes {
		// the tradeoff to not short-circuiting on the filtering is that we can report much better error messages
		// about why scheduling failed
		itCompat := compatible(it, requirements)
		itFits := fits(it, totalRequests)

		// By using this iterative approach vs. the Available() function it prevents allocations
		// which have to be garbage collected and slow down Karpenter's scheduling algorithm
		itHasOffering := false
		for _, of := range it.Offerings {
			if of.Available && requirements.IsCompatible(of.Requirements, scheduling.AllowUndefinedWellKnownLabels) {
				itHasOffering = true
				break
			}
		}

		// track if any single instance type met a single criteria
		err.requirementsMet = err.requirementsMet || itCompat
		err.fits = err.fits || itFits
		err.hasOffering = err.hasOffering || itHasOffering

		// track if any single instance type met the three pairs of criteria
		err.requirementsAndFits = err.requirementsAndFits || (itCompat && itFits && !itHasOffering)
		err.requirementsAndOffering = err.requirementsAndOffering || (itCompat && itHasOffering && !itFits)
		err.fitsAndOffering = err.fitsAndOffering || (itFits && itHasOffering && !itCompat)

		// and if it met all criteria, we keep the instance type and continue filtering.  We now won't be reporting
		// any errors.
		if itCompat && itFits && itHasOffering {
			remaining = append(remaining, it)
		}
	}

	if requirements.HasMinValues() {
		// We don't care about the minimum number of instance types that meet our requirements here, we only care if they meet our requirements.
		_, unsatisfiableKeys, err.minValuesIncompatibleErr = remaining.SatisfiesMinValues(requirements)
		if err.minValuesIncompatibleErr != nil {
			if !relaxMinValues {
				// If MinValuesPolicy is set to Strict, return empty InstanceTypeOptions as we cannot launch with the remaining InstanceTypes when min values is violated.
				remaining = nil
			} else {
				err.minValuesIncompatibleErr = nil
			}
		}
	}
	if len(remaining) == 0 {
		return nil, unsatisfiableKeys, err
	}
	return remaining, unsatisfiableKeys, nil
}

func compatible(instanceType *cloudprovider.InstanceType, requirements scheduling.Requirements) bool {
	return instanceType.Requirements.Intersects(requirements) == nil
}

func fits(instanceType *cloudprovider.InstanceType, requests corev1.ResourceList) bool {
	return resources.Fits(requests, instanceType.Allocatable())
}
