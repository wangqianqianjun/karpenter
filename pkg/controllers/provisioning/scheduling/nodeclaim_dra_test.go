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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func TestNodeClaim_createPlaceholderNode(t *testing.T) {
	tests := []struct {
		name         string
		nodeTemplate NodeClaimTemplate
		instanceType *cloudprovider.InstanceType
		wantLabels   map[string]string
	}{
		{
			name: "basic placeholder with instance type",
			nodeTemplate: NodeClaimTemplate{
				NodeClaim: v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"custom-label": "custom-value",
						},
					},
				},
			},
			instanceType: &cloudprovider.InstanceType{
				Name: "p4d.24xlarge",
			},
			wantLabels: map[string]string{
				corev1.LabelInstanceTypeStable: "p4d.24xlarge",
				"custom-label":                 "custom-value",
			},
		},
		{
			name: "placeholder with zone and region labels",
			nodeTemplate: NodeClaimTemplate{
				NodeClaim: v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							corev1.LabelTopologyZone:   "us-east-1a",
							corev1.LabelTopologyRegion: "us-east-1",
							corev1.LabelArchStable:     "amd64",
							corev1.LabelOSStable:       "linux",
						},
					},
				},
			},
			instanceType: &cloudprovider.InstanceType{
				Name: "m5.large",
			},
			wantLabels: map[string]string{
				corev1.LabelInstanceTypeStable: "m5.large",
				corev1.LabelTopologyZone:       "us-east-1a",
				corev1.LabelTopologyRegion:     "us-east-1",
				corev1.LabelArchStable:         "amd64",
				corev1.LabelOSStable:           "linux",
			},
		},
		{
			name: "placeholder with requirements as labels",
			nodeTemplate: NodeClaimTemplate{
				Requirements: scheduling.NewRequirements(
					scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "us-west-2a"),
					scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, "amd64"),
				),
			},
			instanceType: &cloudprovider.InstanceType{
				Name: "t3.medium",
			},
			wantLabels: map[string]string{
				corev1.LabelInstanceTypeStable: "t3.medium",
				corev1.LabelTopologyZone:       "us-west-2a",
				corev1.LabelArchStable:         "amd64",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := &NodeClaim{
				NodeClaimTemplate: tt.nodeTemplate,
				hostname:          "test-hostname",
			}

			node := nc.createPlaceholderNode(tt.instanceType)

			// Check node name format
			expectedPrefix := "karpenter-placeholder-" + tt.instanceType.Name
			if len(node.Name) <= len(expectedPrefix) {
				t.Errorf("placeholder node name too short: %s", node.Name)
			}
			if node.Name[:len(expectedPrefix)] != expectedPrefix {
				t.Errorf("placeholder node name = %s, want prefix %s", node.Name, expectedPrefix)
			}

			// Check labels
			for k, v := range tt.wantLabels {
				if got := node.Labels[k]; got != v {
					t.Errorf("label %s = %s, want %s", k, got, v)
				}
			}

			// Ensure instance type label exists
			if node.Labels[corev1.LabelInstanceTypeStable] != tt.instanceType.Name {
				t.Errorf("instance type label = %s, want %s",
					node.Labels[corev1.LabelInstanceTypeStable], tt.instanceType.Name)
			}
		})
	}
}

func TestNodeClaim_createPlaceholderNode_MultiValueRequirements(t *testing.T) {
	// Test that multi-value requirements are NOT added as labels
	nc := &NodeClaim{
		NodeClaimTemplate: NodeClaimTemplate{
			Requirements: scheduling.NewRequirements(
				// Single value - should be added
				scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, "amd64"),
				// Multiple values - should NOT be added
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "us-west-2a", "us-west-2b"),
				// NotIn operator - should NOT be added
				scheduling.NewRequirement("custom-key", corev1.NodeSelectorOpNotIn, "value"),
			),
		},
		hostname: "test-hostname",
	}

	it := &cloudprovider.InstanceType{Name: "test-instance"}
	node := nc.createPlaceholderNode(it)

	// Single value requirement should be present
	if node.Labels[corev1.LabelArchStable] != "amd64" {
		t.Errorf("expected arch label to be 'amd64', got %s", node.Labels[corev1.LabelArchStable])
	}

	// Multi-value requirement should NOT be present
	if _, exists := node.Labels[corev1.LabelTopologyZone]; exists {
		t.Errorf("multi-value requirement should not be added as label")
	}

	// NotIn requirement should NOT be present
	if _, exists := node.Labels["custom-key"]; exists {
		t.Errorf("NotIn requirement should not be added as label")
	}
}
