/*
Copyright The Ratify Authors.
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

package ratify

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/notaryproject/ratify-go/internal/set"
)

const allRule = 0 // All nested rules must succeed for this rule to be allowed.

// thresholdPolicyDecision is the decision made by threshold policy enforcer.
type thresholdPolicyDecision int

const (
	// thresholdPolicyDecisionUndetermined is the initial or a temporary state
	// of the policy decision.
	thresholdPolicyDecisionUndetermined thresholdPolicyDecision = iota
	// thresholdPolicyDecisionDeny is the final state when the policy evaluation
	// failed.
	thresholdPolicyDecisionDeny
	// thresholdPolicyDecisionAllow is the final state when the policy
	// evaluation succeeded.
	thresholdPolicyDecisionAllow
)

// ThresholdPolicyRule defines the policy rule for [ThresholdPolicyEnforcer].
type ThresholdPolicyRule struct {
	// Verifier is the Verifier to be used for this rule.
	// If set, the rule requires the Verifier to verify the corresponding
	// artifact. Either Verifier or Rules must be set.
	// Optional.
	Verifier string

	// Threshold is the required number of satisfied nested rules defined in
	// this rule. If not set or set to 0, all nested rules must be satisfied.
	// Optional.
	Threshold int

	// Rules hold nested rules that could be applied to referrer artifacts.
	// Optional.
	Rules []*ThresholdPolicyRule

	// verifiers hold the verifiers that are available in the nested rules with
	// Verifier set. It's auto generated during the compilation of the policy.
	verifiers set.Set[string]

	// ruleID is the unique ID of the rule in the policy tree. It's auto
	// generatd during the compilation of the policy.
	ruleID int
}

// deepCopy creates a deep copy of the ThresholdPolicyRule with all exposed
// fields.
func (r *ThresholdPolicyRule) deepCopy() *ThresholdPolicyRule {
	if r == nil {
		return nil
	}

	rule := &ThresholdPolicyRule{
		Verifier:  r.Verifier,
		Threshold: r.Threshold,
		Rules:     make([]*ThresholdPolicyRule, len(r.Rules)),
	}
	for i, childRule := range r.Rules {
		rule.Rules[i] = childRule.deepCopy()
	}
	return rule
}

// compile compiles the policy rule by assigning unique IDs and aggregating
// verifiers from nested rules recursively.
func (r *ThresholdPolicyRule) compile(ruleID int) (set.Set[string], int) {
	r.ruleID = ruleID
	ruleID++

	verifiers := set.New[string]()
	for _, childRule := range r.Rules {
		var childVerifiers set.Set[string]
		childVerifiers, ruleID = childRule.compile(ruleID)
		verifiers.Union(childVerifiers)
	}
	r.verifiers = verifiers

	if r.Verifier != "" {
		return set.New(r.Verifier), ruleID
	}
	return verifiers, ruleID
}

// evaluationNode holds the statistics for a policy rule during evaluation.
// An evaluationNode is regarded as a virtual node if it corresponds to a rule
// without Verifier set. A virtual node will set artifactDigest to empty since
// it doesn't correspond to a specific artifact.
type evaluationNode struct {
	// rule is the policy rule for this node.
	rule *ThresholdPolicyRule

	// subjectNode is the parent node of this node.
	subjectNode *evaluationNode

	// artifactDigest is the digest of the artifact being verified against in
	// this node.
	artifactDigest string

	// ruleDecision indicates the decision made by the nested rules and the
	// threshold. If there is no nested rules, this field is set to Allow by
	// default.
	ruleDecision thresholdPolicyDecision

	// childVirtualNodes is the index of the immediate virtual evaluation
	// nodes by rule ID.
	childVirtualNodes map[int]*evaluationNode

	// childNodes is the index of the immediate evaluation nodes with Verifers
	// by rule ID.
	childNodes map[int][]*evaluationNode

	// committed indicates whether more child (virtual) nodes will be added to
	// this node. If set to true, no more child nodes will be added to it.
	committed bool
}

func (n *evaluationNode) addChildNode(rule *ThresholdPolicyRule, artifactDigest string) *evaluationNode {
	node := &evaluationNode{
		rule:           rule,
		subjectNode:    n,
		artifactDigest: artifactDigest,
	}
	if len(rule.Rules) == 0 {
		node.ruleDecision = thresholdPolicyDecisionAllow
	} else {
		node.childNodes = make(map[int][]*evaluationNode)
		node.childVirtualNodes = make(map[int]*evaluationNode)
	}

	// A nested rule with verifier set could match multiple artifacts of the
	// same artifactType.
	n.childNodes[rule.ruleID] = append(n.childNodes[rule.ruleID], node)
	return node
}

func (n *evaluationNode) addChildVirtualNode(rule *ThresholdPolicyRule) *evaluationNode {
	node := &evaluationNode{
		rule:        rule,
		subjectNode: n,
	}
	node.childNodes = make(map[int][]*evaluationNode)
	node.childVirtualNodes = make(map[int]*evaluationNode)

	n.childVirtualNodes[rule.ruleID] = node
	return node
}

// refreshDecision refreshes the decision for the node and its ancestors.
func (n *evaluationNode) refreshDecision() {
	// 1. Calculate rule decision.
	if n.calculateDecision() == thresholdPolicyDecisionAllow {
		// 2. If the rule decision is allow, propagate it to ancestors.
		n.refreshAncestorsDecision()
	}
}

// calculateDecision calculates the decision for the node based on the gathered
// results.
func (n *evaluationNode) calculateDecision() thresholdPolicyDecision {
	if n.ruleDecision != thresholdPolicyDecisionUndetermined {
		return n.ruleDecision
	}

	successfulRuleCount := 0
	undeterminedRuleCount := 0
	for _, nodes := range n.childNodes {
		// validate each rule
		successfuleRuleFound := false
		undeterminedRuleFound := false
		for _, node := range nodes {
			if node.ruleDecision == thresholdPolicyDecisionAllow {
				successfuleRuleFound = true
			} else if node.ruleDecision == thresholdPolicyDecisionUndetermined {
				undeterminedRuleFound = true
			}
		}
		if successfuleRuleFound {
			successfulRuleCount++
		}
		if undeterminedRuleFound {
			undeterminedRuleCount++
		}
	}
	for _, node := range n.childVirtualNodes {
		if node.ruleDecision == thresholdPolicyDecisionAllow {
			successfulRuleCount++
		} else if node.ruleDecision == thresholdPolicyDecisionUndetermined {
			undeterminedRuleCount++
		}
	}

	threshold := n.rule.Threshold
	if threshold == allRule {
		threshold = len(n.rule.Rules)
	}
	if successfulRuleCount >= threshold {
		n.ruleDecision = thresholdPolicyDecisionAllow
	} else if n.committed && undeterminedRuleCount == 0 {
		n.ruleDecision = thresholdPolicyDecisionDeny
	}
	return n.ruleDecision
}

// refreshAncestorsDecision refreshes the decision for all ancestors.
func (n *evaluationNode) refreshAncestorsDecision() {
	for node := n.subjectNode; node != nil; node = node.subjectNode {
		if node.ruleDecision != thresholdPolicyDecisionUndetermined {
			break
		}
		// only propagate allow decision to ancestors.
		if node.calculateDecision() != thresholdPolicyDecisionAllow {
			break
		}
	}
}

// A node is finalized if either it's determined or any of its ancestors is
// determined.
func (n *evaluationNode) finalized() bool {
	node := n
	for node != nil {
		if node.ruleDecision != thresholdPolicyDecisionUndetermined {
			return true
		}
		node = node.subjectNode
	}
	return false
}

// verifiable checks if the verifier is required in the rule.
func (n *evaluationNode) verifiable(verifier string) bool {
	return n.rule.verifiers.Contains(verifier)
}

// thresholdEvaluator represents the state of the threshold policy during
// evaluation.
type thresholdEvaluator struct {
	// evalGraph is the root node of the evaluation graph.
	evalGraph *evaluationNode

	// subjectIndex is the index of the evaluation nodes by subject digest.
	subjectIndex map[string][]*evaluationNode

	// verifierIndex is the index of the evaluation nodes by concatenating subject
	// digest, artifact digest and verifier.
	verifierIndex map[string][]*evaluationNode

	mu sync.RWMutex
}

// verifierIndexKey generates the key for the verifier index.
func verifierIndexKey(subjectDigest, artifactDigest, verifier string) string {
	return strings.Join([]string{subjectDigest, artifactDigest, verifier}, ":")
}

// Pruned checks if whether the verifier is required to verify the subject
// against the artifact.
func (e *thresholdEvaluator) Pruned(ctx context.Context, subjectDigest, artifactDigest, verifier string) (PrunedState, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if _, ok := e.verifierIndex[verifierIndexKey(subjectDigest, artifactDigest, verifier)]; ok {
		return PrunedStateVerifierPruned, nil
	}
	nodes, ok := e.subjectIndex[subjectDigest]
	if !ok {
		return PrunedStateSubjectPruned, nil
	}

	// Return PrunedStateSubjectPruned if all nodes are finalized.
	// Otherwise, return PrunedStateVerifierPruned if all nodes don't require
	// the verifier.
	// Otherwise, return PrunedStateNone.
	subjectPruned := true
	verifierPruned := true
	for _, node := range nodes {
		if node.finalized() {
			continue
		}
		subjectPruned = false
		if node.verifiable(verifier) {
			verifierPruned = false
			break
		}
	}
	if subjectPruned {
		return PrunedStateSubjectPruned, nil
	}
	if verifierPruned {
		return PrunedStateVerifierPruned, nil
	}
	return PrunedStateNone, nil
}

// AddResult adds the successful verification result of the subject against the
// artifact to the evaluator for further evaluation.
func (e *thresholdEvaluator) AddResult(ctx context.Context, subjectDigest, artifactDigest string, artifactResult *VerificationResult) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if artifactResult.Err != nil {
		// Only add successful verification result to the evaluator.
		return nil
	}

	nodes, err := e.createEvaluationNodes(subjectDigest, artifactDigest, artifactResult.Verifier.Name())
	if err != nil {
		return err
	}

	for _, node := range nodes {
		node.refreshDecision()
	}
	return nil
}

// createEvaluationNodes creates new evaluation nodes for the given subject,
// artifact and verifier.
// Note that multiple nodes may be created in terms of the policy rules.
func (e *thresholdEvaluator) createEvaluationNodes(subjectDigest, artifactDigest, verifier string) ([]*evaluationNode, error) {
	// Find subject nodes for the given subject digest.
	subjectNodes, ok := e.subjectIndex[subjectDigest]
	if !ok {
		return nil, fmt.Errorf("cannot find evaluation node for subject %s", subjectDigest)
	}

	// create new nodes for the combination of subject, artifact and verifier.
	var nodes []*evaluationNode
	for _, subjectNode := range subjectNodes {
		newNodes := e.createEvaluationNodesForSubject(subjectDigest, artifactDigest, verifier, subjectNode)
		if len(newNodes) > 0 {
			nodes = append(nodes, newNodes...)
		}
	}
	return nodes, nil
}

func (e *thresholdEvaluator) createEvaluationNodesForSubject(subjectDigest, artifactDigest, verifier string, subjectNode *evaluationNode) []*evaluationNode {
	var nodes []*evaluationNode
	for _, rule := range subjectNode.rule.Rules {
		if rule.Verifier != "" {
			nodes = e.createEvaluationNode(rule, subjectDigest, artifactDigest, verifier, subjectNode, nodes)
		} else {
			nodes = e.createVirtualEvaluationNode(rule, subjectDigest, artifactDigest, verifier, subjectNode, nodes)
		}
	}
	return nodes
}

func (e *thresholdEvaluator) createEvaluationNode(rule *ThresholdPolicyRule, subjectDigest, artifactDigest, verifier string, subjectNode *evaluationNode, nodes []*evaluationNode) []*evaluationNode {
	if verifier != rule.Verifier {
		return nodes
	}

	node := subjectNode.addChildNode(rule, artifactDigest)
	nodes = append(nodes, node)

	verifierIndexKey := verifierIndexKey(subjectDigest, artifactDigest, verifier)
	e.verifierIndex[verifierIndexKey] = append(e.verifierIndex[verifierIndexKey], node)
	e.subjectIndex[artifactDigest] = append(e.subjectIndex[artifactDigest], node)

	return nodes
}

func (e *thresholdEvaluator) createVirtualEvaluationNode(rule *ThresholdPolicyRule, subjectDigest, artifactDigest, verifier string, subjectNode *evaluationNode, nodes []*evaluationNode) []*evaluationNode {
	if !rule.verifiers.Contains(verifier) {
		return nodes
	}

	node, ok := subjectNode.childVirtualNodes[rule.ruleID]
	if !ok {
		node = subjectNode.addChildVirtualNode(rule)
	}

	newNodes := e.createEvaluationNodesForSubject(subjectDigest, artifactDigest, verifier, node)
	if len(newNodes) > 0 {
		nodes = append(nodes, newNodes...)
	}
	return nodes
}

// Commit marks related nodes as commited.
// In single goroutine mode, refresh commited nodes to calculate the decision.
// In multi goroutine mode, commited node cannot be refreshed as it may need
// more verification results to make a decision.
func (e *thresholdEvaluator) Commit(ctx context.Context, subjectDigest string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, node := range e.subjectIndex[subjectDigest] {
		node.committed = true
		node.refreshDecision()
	}
	return nil
}

// Evaluate makes the final decision based on aggregated evaluation graph.
func (e *thresholdEvaluator) Evaluate(ctx context.Context) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Refresh the decision for the root node.
	e.evalGraph.refreshDecision()
	return e.evalGraph.ruleDecision == thresholdPolicyDecisionAllow, nil
}

// ThresholdPolicyEnforcer is an implementation of the PolicyEnforcer interface.
type ThresholdPolicyEnforcer struct {
	policy *ThresholdPolicyRule
}

// NewThresholdPolicyEnforcer creates a new ThresholdPolicyEnforcer with the
// given policy rule. The policy rule must be non-nil.
func NewThresholdPolicyEnforcer(policy *ThresholdPolicyRule) (*ThresholdPolicyEnforcer, error) {
	if err := validatePolicy(policy); err != nil {
		return nil, err
	}

	copiedPolicy := policy.deepCopy()
	copiedPolicy.compile(1)

	return &ThresholdPolicyEnforcer{
		policy: copiedPolicy,
	}, nil
}

func validatePolicy(policy *ThresholdPolicyRule) error {
	if policy == nil {
		return fmt.Errorf("policy rule is nil")
	}
	if policy.Verifier != "" {
		return fmt.Errorf("the root rule must not have a verifier name")
	}

	return validateRule(policy)
}

func validateRule(rule *ThresholdPolicyRule) error {
	if rule == nil {
		return fmt.Errorf("rule is nil")
	}

	// Validate threshold value.
	if rule.Threshold < 0 {
		return fmt.Errorf("threshold must be greater than or equal to 0")
	}
	if rule.Threshold > len(rule.Rules) {
		return fmt.Errorf("threshold must be less than or equal to the number of rules")
	}

	if rule.Verifier == "" && len(rule.Rules) == 0 {
		return fmt.Errorf("rule must have at least one nested rule if no verifier name is set")
	}

	for _, childRule := range rule.Rules {
		if err := validateRule(childRule); err != nil {
			return err
		}
	}
	return nil
}

// Evaluator returns a thresholdEvaluator for the given subject digest.
func (e *ThresholdPolicyEnforcer) Evaluator(ctx context.Context, subjectDigest string) (Evaluator, error) {
	if e.policy == nil {
		return nil, fmt.Errorf("policy is nil")
	}

	rootNode := &evaluationNode{
		rule:              e.policy,
		artifactDigest:    subjectDigest,
		childNodes:        make(map[int][]*evaluationNode),
		childVirtualNodes: make(map[int]*evaluationNode),
	}

	return &thresholdEvaluator{
		subjectIndex: map[string][]*evaluationNode{
			subjectDigest: {rootNode},
		},
		verifierIndex: make(map[string][]*evaluationNode),
		evalGraph:     rootNode,
	}, nil
}
