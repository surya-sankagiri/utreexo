package patriciaaccumulator

import (
	"fmt"
	"sort"

	"github.com/openconfig/goyang/pkg/indent"
	logrus "github.com/sirupsen/logrus"
)

type patriciaLookup struct {
	stateRoot     Hash
	treeNodes     *ramCacheTreeNodes
	leafLocations map[MiniHash]uint64
}

// func NewPatriciaLookup() *patriciaLookup {
// 	t := new(patriciaLookup)
// 	t.treeNodes = make(map[Hash]patriciaNode)
// 	return t
// }

// func (t *patriciaLookup) merge(other *patriciaLookup) {
// 	for hash, node := range other.treeNodes {
// 		t.treeNodes[hash] = node
// 	}
// }

// For debugging
func (t *patriciaLookup) String() string {

	if t.stateRoot != empty {
		return fmt.Sprint("Root is: ", t.stateRoot[:6], "\n", t.subtreeString(t.stateRoot), "\n")
	}
	return "empty tree"
}

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

// I changed this since the other code assumes the root is first, we might change back at some point -Bolton
// func ConstructProof(target uint64, midpoints []uint64, neighborHashes []Hash) PatriciaProof {
// 	var proof PatriciaProof
// 	midpoints = midpoints[:len(midpoints)-1] // the last midpoint is the target
// 	//reverse the order of midpoints and neighborHashes so as to include trees first
// 	for i, j := 0, len(midpoints)-1; i < j; i, j = i+1, j-1 {
// 		midpoints[i], midpoints[j] = midpoints[j], midpoints[i]
// 		neighborHashes[i], neighborHashes[j] = neighborHashes[j], neighborHashes[i]
// 	}
// 	proof.target = target
// 	proof.midpoints = midpoints
// 	proof.hashes = neighborHashes
// 	return proof
// }

// RetrieveProof Creates a proof for a single target against a state root
// The proof consists of:
//   The target we are proving for
// 	 all midpoints on the main branch from the root to (Just before?) the proved leaf
//   all hashes of nodes that are neighbors of nodes on the main branch, in order
func (t *patriciaLookup) RetrieveProof(target uint64) (PatriciaProof, error) {

	var proof PatriciaProof
	proof.prefixes = make([]prefixRange, 0, 64)
	proof.hashes = make([]Hash, 0, 64)
	var node patriciaNode
	var nodeHash Hash
	var neighborHash Hash
	// var midpoints = make([]uint64, 0, 64)
	// var neighborHashes = make([]Hash, 0, 64)
	var ok bool
	// Start at the root of the tree
	node, ok = t.treeNodes.read(t.stateRoot)
	if !ok {
		return proof, fmt.Errorf("state root %x not found", t.stateRoot)
	}
	// Discover the path to the leaf
	for {
		logrus.Debug(fmt.Sprintln("Descending to leaf - at node", node.prefix.min(), node.prefix.max(), target))
		proof.prefixes = append(proof.prefixes, node.prefix)
		if !node.prefix.contains(target) {
			// The target location is not in range; this is an error
			// REMARK (SURYA): The current system has no use for proof of non-existence, nor has the means to create one
			return proof, fmt.Errorf("target location %d not found", target)
		}

		// If the min is the max, we are at a leaf
		// TODO do we want to include the midpoint of the leaf node?
		if node.left == node.right {
			// REMARK (SURYA): We do not check whether the hash corresponds to the hash of a leaf
			//perform a sanity check here
			if len(proof.prefixes) != len(proof.hashes)+1 {
				panic("# midpoints not equal to # hashes")
			}
			if node.prefix.value() != target {
				panic("midpoint of leaf not equal to target")
			}
			// proof = ConstructProof(target, midpoints, neighborHashes)
			logrus.Trace("Returning valid proof")
			return proof, nil
		}
		if !node.prefix.contains(target) {
			panic("Target not in range in RetrieveProof")
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
		proof.hashes = append(proof.hashes, neighborHash)
	}
}

// // RetrieveListProofs creates a list of individual proofs for targets, in sorted order
// func (t *patriciaLookup) RetrieveListProofs(targets []uint64) ([]PatriciaProof, error) {

// 	// A slice of proofs of individual elements
// 	individualProofs := make([]PatriciaProof, 0, 3000)

// 	// TODO: is sorting necessary? may already be in order
// 	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })

// 	logrus.Debug("Starting loop in RetrieveListProofs")
// 	// Note: a parallelized version is below
// 	for _, target := range targets {
// 		logrus.Debug("calling RetrieveProof")
// 		proof, err := t.RetrieveProof(target)
// 		if err != nil {
// 			panic(err)
// 		}
// 		rootNode, ok := t.treeNodes.read(t.stateRoot)
// 		if !ok {
// 			panic("Could not find Root Node")
// 		}

// 		if proof.midpoints[0] != rootNode.midpoint {
// 			panic("Wrong root midpoint")
// 		}

// 		individualProofs = append(individualProofs, proof)
// 	}

// 	return individualProofs, nil

// }

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

		// rootNode, _ := t.treeNodes.read(t.stateRoot)

		// if proof.midpoints[0] != rootNode.midpoint {
		// 	panic("Wrong root midpoint")
		// }
	}

	for range targets {
		individualProofs = append(individualProofs, <-ch)
	}

	sort.Slice(individualProofs, func(i, j int) bool { return individualProofs[i].target < individualProofs[j].target })

	return individualProofs, nil

}

// RetrieveBatchProof creates a proof for a batch of targets against a state root
// The proof consists of:
//   The targets we are proving for (in left to right order)
// 	 all midpoints on the main branches from the root to (Just before?) the proved leaves (in any order)
//   all hashes of nodes that are neighbors of nodes on the main branches, but not on a main branch themselves (in DFS order with lower level nodes first, the order the hashes will be needed when the stateless node reconstructs the proof branches)
// NOTE: This version is meant to trim the data that's not needed,
// func (t *patriciaLookup) RetrieveBatchProofLong(targets []uint64) LongBatchProof {

// 	// If no targets, return empty batchproof
// 	if len(targets) == 0 {
// 		return LongBatchProof{targets, make([]Hash, 0, 0), make([]uint64, 0, 0)}
// 	}

// 	// A slice of proofs of individual elements
// 	individualProofs, _ := t.RetrieveListProofs(targets)

// 	var midpoints = make([]uint64, 0, 0)
// 	var midpointsSet = make(map[uint64]bool)
// 	// Collect all midpoints in a set
// 	for _, proof := range individualProofs {
// 		for _, midpoint := range proof.midpoints {
// 			midpointsSet[midpoint] = true
// 		}
// 	}
// 	// Put set into list form
// 	for k := range midpointsSet {
// 		midpoints = append(midpoints, k)
// 	}

// 	sort.Slice(midpoints, func(i, j int) bool { return midpoints[i] < midpoints[j] })
// 	// TODO compress this list

// 	hashesToDelete := make(map[Hash]bool)

// 	allHashes := make([]Hash, len(individualProofs[0].hashes))
// 	copy(allHashes, individualProofs[0].hashes)

// 	// To compress this data into a BatchProof we must remove
// 	// the hash children of nodes which occur in two individual proofs but fork
// 	for i := 1; i < len(individualProofs); i++ {
// 		proofCurrent := individualProofs[i]
// 		proofPrev := individualProofs[i-1]
// 		// fmt.Println(proofCurrent)
// 		// fmt.Println(proofPrev)
// 		// Iterate through i and i-1 to find the fork
// 		for j, midpoint := range proofCurrent.midpoints {
// 			if midpoint != proofPrev.midpoints[j] {
// 				// Fork found
// 				// Delete the hashes at j-1 from both
// 				// filterDelete(hashes, proofCurrent.hashes[j-1])
// 				hashesToDelete[proofPrev.hashes[j-1]] = true
// 				// Now add the hashes from currentProof from after the fork
// 				allHashes = append(allHashes, proofCurrent.hashes[j:]...)
// 				break

// 			}
// 		}
// 	}

// 	// Delete deletable hashes
// 	hashes := make([]Hash, 0, 0)
// 	for _, hash := range allHashes {
// 		if !hashesToDelete[hash] {
// 			hashes = append(hashes, hash)
// 		}
// 	}

// 	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })
// 	return LongBatchProof{targets, hashes, midpoints}

// 	// TODO

// }

// RetrieveBatchProofShort creates a proof for a batch of targets against a state root
// The proof consists of:
//   The targets we are proving for (in left to right order) (aka ascending order)
// 	 all midpoints on the main branches from the root to the proved leaves, represented as widths (uint8), in DFS order. ()
//   all hashes of nodes that are neighbors of nodes on the main branches, but not on a main branch themselves (in DFS order with lower level nodes first, the order the hashes will be needed when the stateless node reconstructs the proof branches)
// NOTE: This version is meant to trim the data that's not needed,
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
		prefixLogWidths = append(prefixLogWidths, 0)
		for _, prefix := range proof.prefixes {
			//check if this prefix has already been seen before
			if _, ok := prefixesSet[prefix]; !ok {
				prefixesSet[prefix] = true
				logWidth := prefix.logWidth() + 1
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

// // Helper function that sorts a slice and removes duplicate
// func sortRemoveDuplicates(a []uint64) {
// 	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })

// 	for i := 0; i < len(a)-1; i++ {
// 		if a[i] == a[i+1] {
// 			// From https://yourbasic.org/golang/delete-element-slice/
// 			// Remove the element at index i from a.
// 			copy(a[i:], a[i+1:]) // Shift a[i+1:] left one index.
// 			// a[len(a)-1] = ""     // Erase last element (write zero value).
// 			a = a[:len(a)-1] // Truncate slice.

// 			i--
// 		}
// 	}
// }

// Helper function removes all copies of a specific value from a slice
func filterDelete(a []Hash, val Hash) {
	// TODO computational efficiency.

	for i := 0; i < len(a)-1; i++ {
		if a[i] == val {
			// From https://yourbasic.org/golang/delete-element-slice/
			// Remove the element at index i from a.
			copy(a[i:], a[i+1:]) // Shift a[i+1:] left one index.
			// a[len(a)-1] = ""     // Erase last element (write zero value).
			a = a[:len(a)-1] // Truncate slice.

			i--
		}
	}
}

// Adds a hash at a particular location and returns the new state root
func (t *patriciaLookup) add(location uint64, toAdd Hash) error {

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
func (t *patriciaLookup) remove(location uint64) {

	// TODO: should state root be a property of patriciaLookup, and we avoid a single lookup representing multiple roots?
	// TODO: Should the proof branch be the input, so this can be called without appatriciaLookup by a stateless node
	// branch := t.RetrieveProof(toAdd, location)

	// fmt.Println("Removing at", location, "on root", t.stateRoot[:6], len(t.treeNodes), "nodes preexisting. Tree is:")
	// fmt.Print(t)

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

	// logrus.Debug("Removing", locations, node.midpoint, node.left, node.right)

	// check all locations in the right range
	// if !node.prefix.contains(locations[0]) || !node.prefix.contains(locations[len(locations)-1]) {
	// 	logrus.Debug(locations, node.prefix.min(), node.prefix.max())
	// 	panic("Not in range, in removeFromSubtree")
	// }

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

	// Linear search
	// var idx int
	// idx = len(locations)
	// for i, location := range locations {
	// 	if node.prefix.right().contains(location) {
	// 		idx = i
	// 		break
	// 	}
	// }

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

// // Adds hashes to the tree under node hash with starting location
// // Returns the new hash of the root to replace the given node
// // Given node assumed to be deleted before this function is called
// func (t *patriciaLookup) recursiveAddRight(hash Hash, startLocation uint64, adds []Hash) (Hash, error) {

// 	logrus.Debug("Starting recursive add")

// 	if len(adds) == 0 {
// 		return hash, nil
// 		// panic("recursiveAddRight: Do not call if nothing to add")
// 	}

// 	node, ok := t.treeNodes.read(hash)
// 	if !ok {
// 		panic("Hash not found")
// 	}

// 	// Index of things to go beneath node
// 	var i uint64
// 	i = node.prefix.max() - startLocation

// 	if node.prefix.max() < startLocation {
// 		i = 0
// 	}

// 	if i > uint64(len(adds)) {
// 		i = uint64(len(adds))
// 	}

// 	inNode := adds[:i]
// 	outsideNode := adds[i:]

// 	var newNode patriciaNode
// 	// Add what must be added in the right child
// 	if i > 0 {
// 		rightNode, ok := t.treeNodes.read(node.right)
// 		if !ok {
// 			panic("could not find node")
// 		}
// 		t.treeNodes.delete(node.right)
// 		newRightHash, err := t.recursiveAddRight(rightNode, startLocation, inNode)

// 		if err != nil {
// 			panic(err)
// 		}
// 		newNode = newInternalPatriciaNode(node.left, newRightHash, node.prefix)
// 	} else {
// 		newNode = node
// 	}

// 	newNodeHash := newNode.hash()
// 	t.treeNodes.write(newNodeHash, newNode)

// 	// If there are no additional adds, we replace the right child with the new right child
// 	if len(outsideNode) == 0 {
// 		return newNodeHash, nil
// 	} else if len(outsideNode) >= 1 {
// 		// If there are elements not in the node,
// 		// they must go in a new subtree which joins with the old node to make a new node
// 		// Form the rest into their own subtree

// 		siblingNode := newLeafPatriciaNode(outsideNode[0], startLocation+i)
// 		siblingNodeHash := siblingNode.hash()
// 		t.treeNodes.write(siblingNodeHash, siblingNode)
// 		// t.leafLocations[outsideNode[0].Mini()] = startLocation + i
// 		t.treeNodes.write(outsideNode[0], patriciaNode{outsideNode[0], empty, singletonRange(startLocation + i)})
// 		combinedNode := newPatriciaNode(newNode, siblingNode)

// 		// t.treeNodes.write(combinedNodeHash, combinedNode)

// 		if len(outsideNode) > 1 {
// 			newNewNodeHash, err := t.recursiveAddRight(combinedNode, startLocation+i+1, outsideNode[1:])
// 			return newNewNodeHash, err
// 		} else {
// 			combinedNodeHash := combinedNode.hash()
// 			t.treeNodes.write(combinedNodeHash, combinedNode)
// 			return combinedNodeHash, nil
// 		}
// 	}
// 	panic("")
// }

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
		} else {
			// 2. node is contained in the left half of overarch and there are only adds on the right

			newRightHash, err := t.makeNewTree(startLocation, adds)
			if err != nil {
				panic(err)
			}
			newNode := newInternalPatriciaNode(hash, newRightHash, overarchingPrefix)
			t.treeNodes.write(newNode.hash(), newNode)
			return newNode.hash(), nil
		}

	} else {
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

	// // If there are adds to the left of the overarching midpoint
	// if overarchingPrefix.midpoint() < startLocation {
	// 	i = 0
	// }

	// if i > uint64(len(adds)) {
	// 	i = uint64(len(adds))
	// }

	// addsUnderNode := adds[:i]
	// addsOutsideNode := adds[i:]

	// if i > 0 {

	// 	t.treeNodes.delete(hash)

	// 	// Add what must be added in the right child
	// 	newRightHash, err := t.recursiveAddRight(node.right, startLocation, addsUnderNode)

	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	node = newInternalPatriciaNode(node.left, newRightHash, node.prefix)
	// 	hash = node.hash()
	// 	t.treeNodes.write(hash, node)

	// }

	// // If there are no additional adds, we replace the right child with the new right child
	// if len(addsOutsideNode) == 0 {

	// 	return hash, nil

	// } else if len(addsOutsideNode) >= 1 {
	// 	// If there are elements not in the node,

	// 	// Which elements go in the supernode of the current node?

	// 	supernodePrefix := node.prefix
	// 	for supernodePrefix.max() <= node.prefix.max() {
	// 		supernodePrefix = supernodePrefix.up()
	// 	}

	// 	j := supernodePrefix.max() - (startLocation + i)

	// 	if j > uint64(len(addsOutsideNode)) {
	// 		j = uint64(len(addsOutsideNode))
	// 	}
	// 	addsInsideSuper := addsOutsideNode[:j]
	// 	addsOutsideSuper := addsOutsideNode[j:]

	// 	insideSuperNode, err := t.makeNewTree(startLocation+i, addsInsideSuper)

	// 	if err != nil {
	// 		return empty, err
	// 	}

	// 	fmt.Println(startLocation, i, j, len(addsOutsideNode), len(addsOutsideSuper), len(addsInsideSuper))
	// 	// 1008 to 1024        1009 to 1010
	// 	newNode := newPatriciaNode(node, insideSuperNode)
	// 	newHash := newNode.hash()
	// 	t.treeNodes.write(newHash, newNode)

	// 	return t.recursiveAddRight(newHash, startLocation+i+j, addsOutsideSuper)

	// }

	panic("")

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
