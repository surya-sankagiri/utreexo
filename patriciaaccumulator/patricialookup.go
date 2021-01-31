package patriciaaccumulator

import (
	"fmt"
	"sort"

	"github.com/openconfig/goyang/pkg/indent"
	logrus "github.com/sirupsen/logrus"
)

type patriciaLookup struct {
	stateRoot Hash
	treeNodes *ramCacheTreeNodes
}

// String converts a patriciaLookup into a string as an indented list of nodes
func (t *patriciaLookup) String() string {

	if t.stateRoot != empty {
		return fmt.Sprint("Root is: ", t.stateRoot[:6], "\n", t.subtreeString(t.stateRoot), "\n")
	}
	return "empty tree"
}

// TODO ctrl-f for panic(err)

// subtreeString returns an indented list of nodes that appear under a given hash
func (t *patriciaLookup) subtreeString(nodeHash Hash) string {
	node, _ := t.treeNodes.read(nodeHash)

	out := ""

	if node.left == node.right {
		return fmt.Sprint("leafnode ", "hash ", nodeHash[:6], " at postion ", node.prefix.value(), " left ", node.left[:6], " right ", node.right[:6], "\n")
	}
	out += fmt.Sprint("hash ", nodeHash[:6], " prefix:", node.prefix.String(), " left ", node.left[:6], " right ", node.right[:6], "\n")
	out += indent.String(" ", t.subtreeString(node.left))
	out += indent.String(" ", t.subtreeString(node.right))

	return out
}

// RetrieveProof creates a proof for a single target against a state root
// The proof consists of:
//   The target we are proving for
// 	 all midpoints on the main branch from the root to the proved leaf
//   all hashes of nodes that are neighbors of nodes on the main branch, in order
func (t *patriciaLookup) RetrieveProof(target uint64) (PatriciaProof, error) {

	var proof PatriciaProof
	proof.target = target
	proof.prefixes = make([]prefixRange, 0, 64)
	proof.hashes = make([]Hash, 0, 64)
	reversedPrefixes := make([]prefixRange, 0, 64)
	reversedHashes := make([]Hash, 0, 64)
	var node patriciaNode
	var nodeHash Hash
	var neighborHash Hash

	var ok bool
	// Start at the root of the tree
	node, ok = t.treeNodes.read(t.stateRoot)
	if !ok {
		return proof, fmt.Errorf("state root %x not found", t.stateRoot)
	}
	// Discover the path to the leaf
	for {
		logrus.Debug(fmt.Sprintln("Descending to leaf - at node", node.prefix.min(), node.prefix.max(), target))
		reversedPrefixes = append(reversedPrefixes, node.prefix)
		if !node.prefix.contains(target) {
			// The target location is not in range; this is an error
			// The current system has no use for proof of non-existence, nor has the means to create one
			return proof, fmt.Errorf("target location %d not found", target)
		}

		// If the node prefix is a singleton, we are at a leaf
		if node.prefix.isSingleton() {
			// Perform a sanity check here
			if len(reversedPrefixes) != len(reversedHashes)+1 {
				panic("# prefixes not equal to # hashes")
			}
			if node.prefix.value() != target {
				panic("prefix value of leaf not equal to target")
			}
			logrus.Trace("Returning valid proof")
			for i := range reversedPrefixes {
				proof.prefixes = append(proof.prefixes, reversedPrefixes[len(reversedPrefixes)-1-i])
			}
			for i := range reversedHashes {
				proof.hashes = append(proof.hashes, reversedHashes[len(reversedHashes)-1-i])
			}
			return proof, nil
		}

		if node.prefix.left().contains(target) {
			logrus.Debug("Descending Left")
			nodeHash = node.left
			neighborHash = node.right
		} else if node.prefix.right().contains(target) {
			logrus.Debug("Descending Right")
			nodeHash = node.right
			neighborHash = node.left
		} else {
			return proof, fmt.Errorf("Neither side contains target %d", target)
		}

		// We are not yet at the leaf, so we descend the tree.
		node, ok = t.treeNodes.read(nodeHash)
		if !ok {
			return proof, fmt.Errorf("Patricia Node %x not found", nodeHash)
		}
		_, ok = t.treeNodes.read(neighborHash)
		if !ok {
			return proof, fmt.Errorf("Patricia Node %x not found", neighborHash)
		}
		reversedHashes = append(reversedHashes, neighborHash)
	}
}

// Parallelized version
// RetrieveListProofs creates a list of individual proofs for targets, in sorted order
func (t *patriciaLookup) RetrieveListProofs(targets []uint64) ([]PatriciaProof, error) {

	// A slice of proofs of individual elements
	individualProofs := make([]PatriciaProof, 0, 3000)

	ch := make(chan PatriciaProof)

	for _, target := range targets {

		go func() {
			proof, err := t.RetrieveProof(target)
			if err != nil {
				panic(err)
			}
			ch <- proof
		}()
	}

	for range targets {
		individualProofs = append(individualProofs, <-ch)
	}

	sort.Slice(individualProofs, func(i, j int) bool { return individualProofs[i].target < individualProofs[j].target })

	return individualProofs, nil

}

// RetrieveBatchProofShort creates a proof for a batch of targets against a state root
// The proof consists of:
//   The targets we are proving for (in left to right order) (aka ascending order)
// 	 all midpoints on the main branches from the root to the proved leaves, represented as widths (uint8), in DFS order. ()
//   all hashes of nodes that are neighbors of nodes on the main branches, but not on a main branch themselves (in DFS order with lower level nodes first, the order the hashes will be needed when the stateless node reconstructs the proof branches)
// NOTE: This version is meant to trim the data that's not needed,
// TODO abstract this into two functions, one that makes a long batch proof, one that shortens it
func (t *patriciaLookup) RetrieveBatchProof(targets []uint64) BatchProof {

	// If no targets, return empty batchproof
	if len(targets) == 0 {
		return BatchProof{targets, make([]Hash, 0, 0), make([]uint8, 0, 0)}
	}

	// A slice of proofs of individual elements
	// RetrieveListProofs creates a list of individual proofs for targets, **in sorted order**
	logrus.Trace("Calling RetrieveListProofs")
	individualProofs, _ := t.RetrieveListProofs(targets)
	// prefixesWidth is a slice that will go into the BatchProof; it will replace the slice of prefixes
	// TODO: convert prefixesWidth to []uint8
	var prefixLogWidths = make([]uint8, 0, 0)
	// prefixesSet is used to remove repeated entries
	var prefixesSet = make(map[prefixRange]bool)

	// Add prefixes, one individualProof at a time, to prefixesWidth, and use prefixesSet to remove redundancies
	for _, proof := range individualProofs {
		// prefixLogWidths = append(prefixLogWidths, 0)
		for _, prefix := range proof.prefixes {
			//check if this prefix has already been seen before
			if _, ok := prefixesSet[prefix]; !ok {
				prefixesSet[prefix] = true
				logWidth := prefix.logWidth()
				prefixLogWidths = append(prefixLogWidths, logWidth)
			}
		}
	}

	hashesToDelete := make(map[Hash]bool)

	allHashes := make([]Hash, len(individualProofs[0].hashes))
	copy(allHashes, individualProofs[0].hashes)

	// To compress this data into a BatchProof we must remove
	// the hash children of nodes which occur in two individual proofs but fork
	for i := 1; i < len(individualProofs); i++ {
		proofCurrent := individualProofs[i]
		proofPrev := individualProofs[i-1]
		// fmt.Println(proofCurrent)
		// fmt.Println(proofPrev)
		// Iterate through i and i-1 to find the fork
		for j, prefix := range proofCurrent.prefixes {
			if prefix != proofPrev.prefixes[j] {
				// Fork found
				// Delete the hashes at j-1 from both
				// filterDelete(hashes, proofCurrent.hashes[j-1])
				hashesToDelete[proofPrev.hashes[j-1]] = true
				// Now add the hashes from currentProof from after the fork
				allHashes = append(allHashes, proofCurrent.hashes[j:]...)
				break
			}
		}
	}

	// Delete deletable hashes
	hashes := make([]Hash, 0, 0)
	for _, hash := range allHashes {
		if !hashesToDelete[hash] {
			hashes = append(hashes, hash)
		}
	}

	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })
	return BatchProof{targets, hashes, prefixLogWidths}
}

// ProveBatch Returns a BatchProof proving a list of hashes against a forest
func (f *Forest) ProveBatch(hashes []Hash) (BatchProof, error) {

	logrus.Trace("Prove batch")
	f.lookup.treeNodes.clearHitTracker()

	targets := make([]uint64, len(hashes))

	for i, hash := range hashes {
		// targets[i], ok = f.lookup.leafLocations[hash.Mini()]
		dummyNode, ok := f.lookup.treeNodes.read(hash)
		if !ok {
			panic("ProveBatch: hash of leaf not found in leafLocations")
		}
		targets[i] = dummyNode.prefix.value()
	}
	logrus.Debug("Calling RetrieveBatchProof")

	f.lookup.treeNodes.printHitTracker()
	return f.lookup.RetrieveBatchProof(targets), nil
}

// Adds a hash at a particular location and returns the new state root
// This is not used currently, but might be useful in the future
func (t *patriciaLookup) addSingleHash(location uint64, toAdd Hash) error {

	// TODO: Should the proof branch be the input, so this can be called without a patriciaLookup by a stateless node
	// branch := t.RetrieveProof(toAdd, location)

	// Add the new leaf node
	newLeafNode := newLeafPatriciaNode(toAdd, location)
	t.treeNodes.write(newLeafNode.hash(), newLeafNode)
	// t.leafLocations[toAdd.Mini()] = location
	t.treeNodes.write(toAdd, patriciaNode{toAdd, empty, singletonRange(location)})

	logrus.Debug("Adding", toAdd[:6], "at", location, "on root", t.stateRoot[:6], t.treeNodes.size(), "nodes preexisting")

	if t.stateRoot == empty {
		// If the patriciaLookup is empty (has empty root), treeNodes should be empty
		if t.treeNodes.size() > 1 {
			return fmt.Errorf("stateRoot empty, but treeNodes was populated")
		}

		// The patriciaLookup is empty, we make a single root and return
		t.stateRoot = newLeafNode.hash()

		return nil

	}
	node, ok := t.treeNodes.read(t.stateRoot)
	logrus.Debug("starting root range is", node.prefix.min(), node.prefix.max())
	if !ok {
		return fmt.Errorf("state root %x not found", t.stateRoot)
	}

	var hash Hash
	var neighbor patriciaNode
	neighborBranch := make([]patriciaNode, 0, 64)
	mainBranch := make([]patriciaNode, 0, 64)

	// We descend the tree until we find a node which does not contain the location at which we are adding
	// This node should be the sibling of the new leaf
	for node.prefix.contains(location) {
		// mainBranch will consist of all the nodes to be replaced, from the root to the parent of the new sibling of the new leaf
		mainBranch = append(mainBranch, node)
		// If the min is the max, we are at a preexisting leaf
		// THIS SHOULD NOT HAPPEN. We never add on top of something already added in this code
		// (Block validity should already be checked at this point)
		if node.left == node.right {
			panic("If the min is the max, we are at a preexisting leaf. We should never add on top of something already added.")
		}

		// Determine if we descend left or right
		if node.prefix.left().contains(location) {
			neighbor, _ = t.treeNodes.read(node.right)
			hash = node.left
		} else {
			neighbor, _ = t.treeNodes.read(node.left)
			hash = node.right
		}
		// Add the other direction node to the branch
		neighborBranch = append(neighborBranch, neighbor)
		// Update node down the tree
		node, _ = t.treeNodes.read(hash)

	} // End loop

	// Combine leaf node with its new sibling
	nodeToAdd := newPatriciaNode(newLeafNode, node)
	t.treeNodes.write(nodeToAdd.hash(), nodeToAdd)

	// We travel up the branch, recombining nodes
	for len(neighborBranch) > 0 {
		// nodeToAdd must now replace the hash of the last node of mainBranch in the second-to-last node of mainBranch
		// Pop neighborNode off
		neighborNode := neighborBranch[len(neighborBranch)-1]
		neighborBranch = neighborBranch[:len(neighborBranch)-1]
		nodeToAdd = newPatriciaNode(neighborNode, nodeToAdd)
		t.treeNodes.write(nodeToAdd.hash(), nodeToAdd)
	}
	// Delete all nodes in the main branch, they have been replaced
	for _, node := range mainBranch {
		t.treeNodes.delete(node.hash())
	}

	// The new state root is the hash of the last node added
	t.stateRoot = nodeToAdd.hash()
	// fmt.Println("new root midpoint is", nodeToAdd.midpoint)

	return nil
}

// Based on add above: this code removes a location
// This is not used currently, but might be useful in the future
func (t *patriciaLookup) removeSingleLocation(location uint64) {

	//

	empty := Hash{}
	if t.stateRoot == empty {
		panic("State root is empty hash")
	}

	node, ok := t.treeNodes.read(t.stateRoot)
	if !ok {
		panic(fmt.Sprint("Could not find root ", t.stateRoot[:6], " in nodes"))
	}

	var hash Hash
	var neighbor patriciaNode
	neighborBranch := make([]patriciaNode, 0, 64)
	mainBranch := make([]patriciaNode, 0, 64)

	// We descend the tree until we find a leaf node
	// This node should be the sibling of the new leaf
	for node.left != node.right {
		// mainBranch will consist of all the nodes to be deleted, from the root to the removed leaf
		mainBranch = append(mainBranch, node)

		// Determine if we descend left or right
		if node.prefix.left().contains(location) {
			neighbor, _ = t.treeNodes.read(node.right)
			hash = node.left
		} else {
			neighbor, _ = t.treeNodes.read(node.left)
			hash = node.right
		}
		// Add the other direction node to the branch
		neighborBranch = append(neighborBranch, neighbor)
		// Update node down the tree
		node, _ = t.treeNodes.read(hash)
		// fmt.Printf("midpoint %d\n", node.midpoint)

	} // End loop
	mainBranch = append(mainBranch, node)

	// node is now the leaf node for the deleted entry
	// delete the hash from the leaf location map
	// loc, ok := t.leafLocations[node.left.Mini()]
	dummyNode, ok := t.treeNodes.read(node.left)
	loc := dummyNode.prefix.value()

	if !ok {
		panic("Didn't find location")
	}
	if loc != node.prefix.value() {
		panic("Location does not match")
	}
	// delete(t.leafLocations, node.left.Mini())
	t.treeNodes.delete(node.left)

	// Check that the leaf node we found has the right location
	if node.prefix.value() != location {
		panic(fmt.Sprintf("Found wrong location in remove"))
	}

	// Delete all nodes in the main branch, they are to be replaced
	for _, node := range mainBranch {
		t.treeNodes.delete(node.hash())
	}

	// If there are no elements in the neighbor nodes, the leaf we deleted was the root
	// The root becomes empty
	if len(neighborBranch) == 0 {
		t.stateRoot = empty
		logrus.Debug("new root hash is", empty[:6])

		return
	}

	// If there is one element in the neighbor nodes, the leaf we deleted was a child of the root
	// The neighbor becomes the new root.
	if len(neighborBranch) == 1 {
		t.stateRoot = neighborBranch[0].hash()
		logrus.Debug("new root hash is", t.stateRoot[:6])

		return
	}

	// Otherwise, the lowest new node is the combination of the sibling and uncle of the deleted leaf
	// That is, the last two nodes in the neighborBranch
	nodeToAdd := newPatriciaNode(neighborBranch[len(neighborBranch)-1], neighborBranch[len(neighborBranch)-2])
	neighborBranch = neighborBranch[:len(neighborBranch)-2]
	t.treeNodes.write(nodeToAdd.hash(), nodeToAdd)

	// We travel up the branch, recombining nodes
	for len(neighborBranch) > 0 {
		// nodeToAdd must now replace the hash of the last node of mainBranch in the second-to-last node of mainBranch
		neighborNode := neighborBranch[len(neighborBranch)-1]
		neighborBranch = neighborBranch[:len(neighborBranch)-1]
		nodeToAdd = newPatriciaNode(neighborNode, nodeToAdd)
		t.treeNodes.write(nodeToAdd.hash(), nodeToAdd)
	}

	// The new state root is the hash of the last node added
	t.stateRoot = nodeToAdd.hash()
	logrus.Debug("new root hash is", t.stateRoot[:6])

}

// removeFromSubtree takes a sorted list of locations, and deletes them from the tree
// returns the hash of the new parent node (to replace the input hash)
// also returns whether any elements remain in that subtree
func (t *patriciaLookup) removeFromSubtree(locations []uint64, hash Hash) (Hash, bool, error) {

	logrus.Trace("Starting recursive remove from subtree")

	// Check if anything to be removed
	if len(locations) == 0 {
		return hash, false, nil
	}

	node, ok := t.treeNodes.read(hash)
	if !ok {
		logrus.Debug(t.String())
		panic("Remove from subtree could not find node")
	}
	t.treeNodes.delete(hash)

	logrus.Debug("Removing", locations, node.prefix)

	// Check if leaf node
	if node.left == node.right {
		if len(locations) != 1 {
			panic(fmt.Sprintf("Not 1 element at leaf %d", node))
		}
		if locations[0] != node.prefix.value() {
			panic(fmt.Sprintf("Wrong leaf found in remove subtree location %d but value %d", locations[0], node.prefix.value()))
		}
		// delete(t.leafLocations, node.left.Mini())
		t.treeNodes.delete(node.left)
		return empty, true, nil
	}

	// Binary search to find the index of the first element contained in the right
	var minIdx, maxIdx, idx int
	minIdx = 0
	maxIdx = len(locations)
	rightRangeMin := node.prefix.right().min()
	if rightRangeMin <= locations[minIdx] {
		idx = 0
	} else if locations[maxIdx-1] < rightRangeMin {
		idx = maxIdx
	} else {
		for {
			if minIdx+1 == maxIdx {
				idx = maxIdx
				break
			}
			newIdx := (minIdx + maxIdx) / 2
			if rightRangeMin <= locations[newIdx] {
				maxIdx = newIdx
			} else {
				minIdx = newIdx
			}
		}
	}

	leftLocations := locations[:idx]
	rightLocations := locations[idx:]

	// Remove all locations from the left side of the tree
	newLeft, leftDeleted, err := t.removeFromSubtree(leftLocations, node.left)

	if err != nil {
		panic(err)
	}

	// Remove all locations from the right side
	newRight, rightDeleted, err := t.removeFromSubtree(rightLocations, node.right)

	if err != nil {
		panic(err)
	}

	if !leftDeleted && !rightDeleted {
		// Recompute the new tree node for the parent of both sides
		newNode := newInternalPatriciaNode(newLeft, newRight, node.prefix)
		newHash := newNode.hash()

		t.treeNodes.write(newHash, newNode)

		return newHash, false, nil
	}
	if leftDeleted && !rightDeleted {
		return newRight, false, nil
	}
	if !leftDeleted && rightDeleted {
		return newLeft, false, nil
	}
	if leftDeleted && rightDeleted {
		return empty, true, nil
	}

	return empty, true, nil
}

// Adds hashes to the tree under node hash with starting location
// Returns the new hash of the root to replace the at
// TODO pass node in when possible to avoid accessing treeNodes
// Optionally takes a node which is the hash's node, so you don't have to read it
func (t *patriciaLookup) recursiveAddRight(hash Hash, startLocation uint64, adds []Hash, hintNode *patriciaNode) (Hash, error) {

	logrus.Debug("Starting recursive add")

	// Nothing to add, do nothing and return your hash
	if len(adds) == 0 {
		return hash, nil
	}

	var node patriciaNode

	if hintNode != nil {
		node = *hintNode
		if node.hash() != hash {
			panic("wrong node passed")
		}
	} else {
		// Get the node we are adding to
		readNode, ok := t.treeNodes.read(hash)
		if !ok {
			panic("could not find node")
		}
		node = readNode
	}

	// Get the overarching prefix of node and all the adds after everything is done
	lastLocation := startLocation + uint64(len(adds)) - 1
	var overarchingPrefix prefixRange
	if node.prefix.contains(lastLocation) {
		overarchingPrefix = node.prefix
	} else {
		overarchingPrefix = commonPrefix(node.prefix, singletonRange(lastLocation))
	}

	// // At what index in the list of adds do we stop placing under the current node
	// var i uint64
	// i = overarchingPrefix.midpoint() - startLocation

	// There must exist at least one add on the right side of the overarchingPrefix
	if !overarchingPrefix.right().contains(lastLocation) {
		panic("At least one add should be on right side")
	}

	// We now must branch on two critical factors:
	// Are there subnodes of node on the right of the overarch (is node.prefix == overarch)?
	// Are there adds to the left of the overarching midpoint?

	// There are three possibilities:
	if node.prefix.subset(overarchingPrefix.left()) {
		if startLocation < overarchingPrefix.midpoint() {
			// 1. node is contained in the left half of overarch and there are adds on the left
			i := overarchingPrefix.midpoint() - startLocation
			leftAdds := adds[:i]
			rightAdds := adds[i:]
			// newLeftHash, err := t.recursiveAddRight(hash, startLocation, leftAdds, &node)
			newLeftHash, err := t.recursiveAddRight(hash, startLocation, leftAdds, nil)
			if err != nil {
				panic(err)
			}
			newRightHash, err := t.makeNewTree(startLocation+i, rightAdds)
			if err != nil {
				panic(err)
			}
			newNode := newInternalPatriciaNode(newLeftHash, newRightHash, overarchingPrefix)
			t.treeNodes.write(newNode.hash(), newNode)
			return newNode.hash(), nil
		}
		// 2. node is contained in the left half of overarch and there are only adds on the right

		newRightHash, err := t.makeNewTree(startLocation, adds)
		if err != nil {
			panic(err)
		}
		newNode := newInternalPatriciaNode(hash, newRightHash, overarchingPrefix)
		t.treeNodes.write(newNode.hash(), newNode)
		return newNode.hash(), nil

	}
	// 3. node hits both sides of overarch (so there are only adds on the right)

	if node.prefix != overarchingPrefix {
		panic("Error in prefix management")
	}

	// Since the set of leaves under the node prefix (the over arching prefix in this case)
	// changes, the node must die
	t.treeNodes.delete(hash)

	// The right hash is what changes
	newRightHash, err := t.recursiveAddRight(node.right, startLocation, adds, nil)
	if err != nil {
		panic(err)
	}
	// Since all the adds are on the right node.left remains unchanged

	newNode := newInternalPatriciaNode(node.left, newRightHash, overarchingPrefix)
	t.treeNodes.write(newNode.hash(), newNode)
	return newNode.hash(), nil

}

// makeNewTree makes a tree out of a bunch of adds, and adds all the nodes to t, including the node whose hash it returns
func (t *patriciaLookup) makeNewTree(startLocation uint64, adds []Hash) (Hash, error) {
	if len(adds) == 0 {
		panic("Cant make tree with no adds")
	}
	if len(adds) == 1 {
		node := newLeafPatriciaNode(adds[0], startLocation)
		t.treeNodes.write(node.hash(), node)
		t.treeNodes.write(adds[0], patriciaNode{adds[0], empty, singletonRange(startLocation)})
		return node.hash(), nil
	}
	topPrefix := commonPrefix(singletonRange(startLocation), singletonRange(startLocation+uint64(len(adds))-1))

	cutoff := topPrefix.midpoint()

	leftAdds := adds[:cutoff-startLocation]
	rightAdds := adds[cutoff-startLocation:]

	leftTreeNode, err := t.makeNewTree(startLocation, leftAdds)
	if err != nil {
		return empty, err
	}
	rightTreeNode, err := t.makeNewTree(cutoff, rightAdds)
	if err != nil {
		return empty, err
	}

	newNode := newInternalPatriciaNode(leftTreeNode, rightTreeNode, topPrefix)
	t.treeNodes.write(newNode.hash(), newNode)
	return newNode.hash(), nil

}
