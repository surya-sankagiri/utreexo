package patriciaaccumulator

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/openconfig/goyang/pkg/indent"
	logrus "github.com/sirupsen/logrus"
)

// A FullForest is the entire accumulator of the UTXO set. This is
// what the bridge node stores.  Everything is always full.

/*
The forest is structured in the space of a tree numbered from the bottom left,
taking up the space of a perfect tree that can contain the whole forest.
This means that in most cases there will be null nodes in the tree.
That's OK; it helps reduce renumbering nodes and makes it easier to think about
addressing.  It also might work well for on-disk serialization.
There might be a better / optimal way to do this but it seems OK for now.
*/

// Forest is the entire accumulator of the UTXO set as either a:
// 1) slice if the forest is stored in memory.
// 2) single file if the forest is stored in disk.
// A leaf represents a UTXO with additional data for verification.
// This leaf is numbered from bottom left to right.
// Example of a forest with 4 numLeaves:
//
//	06
//	|------\
//	04......05
//	|---\...|---\
//	00..01..02..03
//
// 04 is the concatenation and the hash of 00 and 01. 06 is the root
// This tree would have a row of 2.
type Forest struct {
	numLeaves uint64 // number of leaves in the forest (bottom row)
	maxLeaf   uint64 // the index of the largest leaf that has ever existed in the trie
	// I think this will be necessary in the add function
	// TODO make sure maxLeaf is updated correctly

	// "data" (not the best name but) is an interface to storing the forest
	// hashes.  There's ram based and disk based for now, maybe if one
	// is clearly better can go back to non-interface.
	data ForestData
	// moving to slice based forest.  more efficient, can be moved to
	// an on-disk file more easily (the subtree stuff should be changed
	// at that point to do runs of i/o).  Not sure about "deleting" as it
	// might not be needed at all with a slice.

	// It doesn't matter too much how much the forest data is structured, since we are focusing on improving proof sizes
	// positionMap map[MiniHash]uint64
	// Inverse of forestMap for leaves.

	lookup patriciaLookup

	/*
	 * below are just for testing / benchmarking
	 */

	// HistoricHashes represents how many hashes this forest has computed
	//
	// Meant for testing / benchmarking
	HistoricHashes uint64

	// TimeRem represents how long Remove() function took
	//
	// Meant for testing / benchmarking
	TimeRem time.Duration

	// TimeMST represents how long the moveSubTree() function took
	//
	// Meant for testing / benchmarking
	TimeMST time.Duration

	// TimeInHash represents how long the hash operations (reHash) took
	//
	// Meant for testing / benchmarking
	TimeInHash time.Duration

	// TimeInProve represents how long the Prove operations took
	//
	// Meant for testing / benchmarking
	TimeInProve time.Duration

	// TimeInVerify represents how long the verify operations took
	//
	// Meant for testing / benchmarking
	TimeInVerify time.Duration
}

// TODO: It seems capitalized structs are exported. Should this be the case for the structs we define?

// Typedef for midpoints to represent ranges?

func (f *Forest) DiskSlotsUsed() uint64 {

	return f.lookup.treeNodes.diskSize()

}

func (f *Forest) LeafLocationSize() uint64 {

	return uint64(len(f.lookup.leafLocations))

}

type patriciaLookup struct {
	stateRoot     Hash
	treeNodes     ForestData
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

	// CHeck if anything to be removed
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
	if !node.prefix.contains(locations[0]) || !node.prefix.contains(locations[len(locations)-1]) {
		logrus.Debug(locations, node.prefix.min(), node.prefix.max())
		panic("Not in range, in removeFromSubtree")
	}

	// Check if leaf node
	if node.left == node.right {
		if len(locations) != 1 {
			panic("Not 1 element at leaf")
		}
		if locations[0] != node.prefix.value() {
			panic("Wrong leaf found in remove subtree")
		}
		// delete(t.leafLocations, node.left.Mini())
		t.treeNodes.delete(node.left)
		return empty, true, nil
	}

	var idx int
	idx = len(locations)
	for i, location := range locations {
		if node.prefix.right().contains(location) {
			idx = i
			break
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

// Delete the leaves at the locations for the trie forest
func (f *Forest) removev5(locations []uint64) error {
	start := time.Now()
	if f.numLeaves < uint64(len(locations)) {
		panic(fmt.Sprintf("Attempting to delete %v nodes, only %v exist", len(locations), f.numLeaves))
	}
	nextNumLeaves := f.numLeaves - uint64(len(locations))
	logrus.Debug("Calling remove from subtree")
	newRoot, _, err := f.lookup.removeFromSubtree(locations, f.lookup.stateRoot)
	if err != nil {
		return err
	}
	f.lookup.stateRoot = newRoot
	// for _, location := range locations {

	// 	if location > f.maxLeaf {
	// 		// check that all locations are before the maxLeaf
	// 		return fmt.Errorf(
	// 			"Trying to delete leaf at %d, beyond max %d", location, f.maxLeaf)
	// 	}

	// 	f.lookup.remove(location)
	// }
	end := time.Now()
	logrus.Debug("Time to delete", len(locations), "UTXOs:", end.Sub(start))
	f.numLeaves = nextNumLeaves

	return nil

}

// NewForest Makes a new forest
func NewForest(forestFile *os.File, cached bool) *Forest {
	f := new(Forest)
	f.numLeaves = 0
	f.maxLeaf = 0

	if forestFile == nil {
		// for in-ram
		// f.data = new(ramForestData)
		panic("We aren't doing ram")
	} else {
		// panic("We cannot yet create a forest from cache or memory")

		if cached {
			panic("We don't do this either")
			// d := new(cacheForestData)
			// d.file = forestFile
			// d.cache = newDiskForestCache(20)
			// f.data = d
		} else {
			// for on-disk with cache
			// 2_000_000 lems in ram seems good, not really enough to cause ram issues, but enough to speed things up
			// With 2_000_000 it seems to only use 3 gigs at most, so 10million should be safe
			treeNodes := newRAMCacheTreeNodes(forestFile, 10000000)
			// for on disk
			// treeNodes := newSuperDiskTreeNodes(forestFile)
			// for bitcask/diskv
			// treeNodes := newdbTreeNodes()

			// TODO move this code to a test file
			// if treeNodes.size() != 0 {
			// 	panic("")
			// }

			// treeNodes.write(node.hash(), node)

			// if treeNodes.size() != 1 {
			// 	fmt.Println("", treeNodes.size(), treeNodes.filePopulatedTo, len(treeNodes.emptySlots))
			// 	panic("Size not 1 after write")
			// }

			// treeNodes.read(node.hash())

			// if treeNodes.size() != 1 {
			// 	panic("Size not 1 after write then read")
			// }

			// treeNodes.delete(node.hash())

			// if treeNodes.size() != 0 {
			// 	panic("")
			// }

			f.lookup = patriciaLookup{Hash{}, &treeNodes, make(map[MiniHash]uint64)}
			// Changed this to a reference
			// This code runs, but I don't understand why.
			// Shouldn't treeNodes be overwritten when it goes out of scope?
			// I guess it's actually idiomatic
			// https://stackoverflow.com/a/28485041/3854633

		}
	}

	// f.data.resize(1)
	// f.positionMap = make(map[MiniHash]uint64)
	return f
}

// END OF OUR CODE //

//BEGINNING OF UTREEXO CODE //

// NewForest : use ram if not given a file
// func NewForest(forestFile *os.File, cached bool) *Forest {
// 	f := new(Forest)
// 	f.numLeaves = 0
// 	f.rows = 0

// 	if forestFile == nil {
// 		// for in-ram
// 		f.data = new(ramForestData)
// 	} else {

// 		if cached {
// 			d := new(cacheForestData)
// 			d.file = forestFile
// 			d.cache = newDiskForestCache(20)
// 			f.data = d
// 		} else {
// 			// for on-disk
// 			d := new(diskForestData)
// 			d.file = forestFile
// 			f.data = d
// 		}
// 	}

// 	f.data.resize(1)
// 	f.positionMap = make(map[MiniHash]uint64)
// 	return f
// }

// // TODO remove, only here for testing
// func (f *Forest) ReconstructStats() (uint64, uint8) {
// 	return f.numLeaves, f.rows
// }

const sibSwap = false
const bridgeVerbose = false

// empty is needed for detection (to find errors) but I'm not sure it's needed
// for deletion.  I think you can just leave garbage around, as it'll either
// get immediately overwritten, or it'll be out to the right, beyond the edge
// of the forest
var empty [32]byte

// Adds hashes to the tree under node hash with starting location
// Returns the new hash of the root to replace the at
// func (t *patriciaLookup) recursiveAddRight(hash Hash, startLocation uint64, adds []Hash) (Hash, error) {

// 	logrus.Debug("Starting recursive add")

// 	if len(adds) == 0 {
// 		return hash, nil
// 	}

// 	if hash == empty {
// 		if len(adds) != 1 {
// 			panic("The first block has one add")
// 		}
// 		siblingNode :=
// 		newHash := siblingNode.hash()
// 		t.treeNodes.write(newHash, siblingNode)
// 		t.leafLocations[adds[0]] = startLocation
// 		return newHash, nil
// 	}

// 	node, ok := t.treeNodes.read(hash)
// 	if !ok {
// 		panic("could not find node")
// 	}
// 	t.treeNodes.delete(hash)

// 	var i uint64
// 	i = node.max() - startLocation

// 	if node.max() < startLocation {
// 		i = 0
// 	}

// 	if i > uint64(len(adds)) {
// 		i = uint64(len(adds))
// 	}

// 	inNode := adds[:i]
// 	outsideNode := adds[i:]

// 	// Add what must be added in the right child
// 	newRightHash, err := t.recursiveAddRight(node.right, startLocation, inNode)

// 	if err != nil {
// 		panic(err)
// 	}

// 	newNode :=
// 	t.treeNodes.write(newNode.hash(), newNode)

// 	// If there are no additional adds, we replace the right child with the new right child
// 	if len(outsideNode) == 0 {

// 		return newNode.hash(), nil

// 	} else if len(outsideNode) >= 1 {
// 		// If there are elements not in the node,
// 		// they must go in a new subtree which joins with the old node to make a new node
// 		// Form the rest into their own subtree

// 		siblingNode :=
// 		siblingNodeHash := siblingNode.hash()
// 		t.treeNodes.write(siblingNodeHash, siblingNode)
// 		t.leafLocations[outsideNode[0]] = startLocation + i
// 		combinedNode := newPatriciaNode(newNode, siblingNode)
// 		t.treeNodes.write(combinedNode.hash(), combinedNode)

// 		newNewNodeHash, err := t.recursiveAddRight(combinedNode.hash(), startLocation+i+1, outsideNode[1:])

// 		return newNewNodeHash, err

// 	}

// 	panic("")

// }

// Adds hashes to the tree under node hash with starting location
// Returns the new hash of the root to replace the given node
// Given node assumed to be deleted before this function is called
func (t *patriciaLookup) recursiveAddRight(node patriciaNode, startLocation uint64, adds []Hash) (Hash, error) {

	logrus.Debug("Starting recursive add")

	if len(adds) == 0 {
		panic("recursiveAddRight: Do not call if nothing to add")
	}

	var i uint64
	i = node.prefix.max() - startLocation

	if node.prefix.max() < startLocation {
		i = 0
	}

	if i > uint64(len(adds)) {
		i = uint64(len(adds))
	}

	inNode := adds[:i]
	outsideNode := adds[i:]
	var newNode patriciaNode
	// Add what must be added in the right child
	if i > 0 {
		rightNode, ok := t.treeNodes.read(node.right)
		if !ok {
			panic("could not find node")
		}
		t.treeNodes.delete(node.right)
		newRightHash, err := t.recursiveAddRight(rightNode, startLocation, inNode)

		if err != nil {
			panic(err)
		}
		newNode = newInternalPatriciaNode(node.left, newRightHash, node.prefix)
	} else {
		newNode = node
	}

	newNodeHash := newNode.hash()
	t.treeNodes.write(newNodeHash, newNode)

	// If there are no additional adds, we replace the right child with the new right child
	if len(outsideNode) == 0 {
		return newNodeHash, nil
	} else if len(outsideNode) >= 1 {
		// If there are elements not in the node,
		// they must go in a new subtree which joins with the old node to make a new node
		// Form the rest into their own subtree

		siblingNode := newLeafPatriciaNode(outsideNode[0], startLocation+i)
		siblingNodeHash := siblingNode.hash()
		t.treeNodes.write(siblingNodeHash, siblingNode)
		// t.leafLocations[outsideNode[0].Mini()] = startLocation + i
		t.treeNodes.write(outsideNode[0], patriciaNode{outsideNode[0], empty, singletonRange(startLocation + i)})
		combinedNode := newPatriciaNode(newNode, siblingNode)

		// t.treeNodes.write(combinedNodeHash, combinedNode)

		if len(outsideNode) > 1 {
			newNewNodeHash, err := t.recursiveAddRight(combinedNode, startLocation+i+1, outsideNode[1:])
			return newNewNodeHash, err
		} else {
			combinedNodeHash := combinedNode.hash()
			t.treeNodes.write(combinedNodeHash, combinedNode)
			return combinedNodeHash, nil
		}
	}
	panic("")
}

// Add adds leaves to the forest.  This is the easy part.
func (f *Forest) addv2(adds []Leaf) error {
	start := time.Now()

	if len(adds) == 0 {
		panic("")
	}

	location := f.maxLeaf
	addHashes := make([]Hash, 0)
	for _, add := range adds {

		// f.lookup.printAll()
		addHashes = append(addHashes, add.Hash)

		// err := f.lookup.add(f.maxLeaf, add.Hash)
		// if err != nil {
		// 	return err
		// }

		f.maxLeaf++
		f.numLeaves++
	}
	if f.lookup.stateRoot == empty {
		if len(adds) != 1 {
			panic("The first block has one add")
		}
		rootNode := newLeafPatriciaNode(addHashes[0], 0)
		newHash := rootNode.hash()
		f.lookup.treeNodes.write(newHash, rootNode)
		// f.lookup.leafLocations[addHashes[0].Mini()] = 0
		f.lookup.treeNodes.write(addHashes[0], patriciaNode{addHashes[0], empty, singletonRange(0)})
		f.lookup.stateRoot = newHash
	} else {
		rootNode, ok := f.lookup.treeNodes.read(f.lookup.stateRoot)
		if !ok {
			panic("could not find state root")
		}
		f.lookup.treeNodes.delete(f.lookup.stateRoot)
		newStateRoot, err := f.lookup.recursiveAddRight(rootNode, location, addHashes)
		if err != nil {
			panic("")
		}
		f.lookup.stateRoot = newStateRoot
	}

	end := time.Now()
	logrus.Debug("Time to add", len(adds), "UTXOs:", end.Sub(start))
	return nil
	// for _, add := range adds {
	// 	// fmt.Printf("adding %x pos %d\n", add.Hash[:4], f.numLeaves)
	// 	f.positionMap[add.Mini()] = f.numLeaves

	// 	rootPositions, _ := getRootsReverse(f.numLeaves, f.rows)
	// 	pos := f.numLeaves
	// 	n := add.Hash
	// 	f.data.write(pos, n)
	// 	for h := uint8(0); (f.numLeaves>>h)&1 == 1; h++ {
	// 		// grab, pop, swap, hash, new
	// 		root := f.data.read(rootPositions[h]) // grab
	// 		//			fmt.Printf("grabbed %x from %d\n", root[:12], roots[h])
	// 		n = parentHash(root, n)   // hash
	// 		pos = parent(pos, f.rows) // rise
	// 		f.data.write(pos, n)      // write
	// 		//			fmt.Printf("wrote %x to %d\n", n[:4], pos)
	// 	}
	// 	f.numLeaves++
	// }
}

// Modify changes the forest, adding and deleting leaves and updating internal nodes.
// Note that this does not modify in place!  All deletes occur simultaneous with
// adds, which show up on the right.
// Also, the deletes need there to be correct proof data, so you should first call Verify().
func (f *Forest) Modify(adds []Leaf, dels []uint64) (*undoBlock, error) {

	numDels, numAdds := len(dels), len(adds)
	// delta := int64(numAdds - numDels) // watch 32/64 bit

	logrus.Debugf("Modify: starting with %d leaves, deleting %d, adding %d\n", f.numLeaves, numDels, numAdds)

	// Changing this
	// if int64(f.numLeaves)+delta < 0 {
	// 	return nil, fmt.Errorf("can't delete %d leaves, only %d exist",
	// 		len(dels), f.numLeaves)
	// }
	//
	if int(f.numLeaves) < len(dels) {
		return nil, fmt.Errorf("can't delete %d leaves, only %d exist",
			len(dels), f.numLeaves)
	}

	if !checkSortedNoDupes(dels) { // check for sorted deletion slice
		fmt.Printf("%v\n", dels)
		return nil, fmt.Errorf("Deletions in incorrect order or duplicated")
	}
	for _, a := range adds { // check for empty leaves
		if a.Hash == empty {
			return nil, fmt.Errorf("Can't add empty (all 0s) leaf to accumulator")
		}
	}

	// fmt.Println("empty slots", len(f.lookup.treeNodes.emptySlots))
	// // remap to expand the forest if needed
	// for int64(f.numLeaves)+delta > int64(1<<f.rows) {
	// 	// fmt.Printf("current cap %d need %d\n",
	// 	// 1<<f.rows, f.numLeaves+delta)
	// 	err := f.reMap(f.rows + 1)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	// v3 should do the exact same thing as v2 now
	// fmt.Printf("Beginning Deletes\n")
	err := f.removev5(dels)
	if err != nil {
		return nil, err
	}
	// f.cleanup(uint64(numDels))

	// save the leaves past the edge for undo
	// dels hasn't been mangled by remove up above, right?
	// BuildUndoData takes all the stuff swapped to the right by removev3
	// and saves it in the order it's in, which should make it go back to
	// the right place when it's swapped in reverse
	// ub := f.BuildUndoData(uint64(numAdds), dels)

	// fmt.Printf("Beginning Adds\n")
	err = f.addv2(adds)
	if err != nil {
		return nil, err
	}

	// fmt.Printf("done modifying block, added %d\n", len(adds))
	// fmt.Printf("post add %s\n", f.ToString())
	// for m, p := range f.positionMap {
	// 	fmt.Printf("%x @%d\t", m[:4], p)
	// }
	// fmt.Printf("\n")

	return nil, err
}

// // reMap changes the rows in the forest
// func (f *Forest) reMap(destRows uint8) error {

// 	if destRows == f.rows {
// 		return fmt.Errorf("can't remap %d to %d... it's the same",
// 			destRows, destRows)
// 	}

// 	if destRows > f.rows+1 || (f.rows > 0 && destRows < f.rows-1) {
// 		return fmt.Errorf("changing by more than 1 not programmed yet")
// 	}

// 	fmt.Printf("remap forest %d rows -> %d rows\n", f.rows, destRows)

// 	// for row reduction
// 	if destRows < f.rows {
// 		return fmt.Errorf("row reduction not implemented")
// 	}
// 	// I don't think you ever need to remap down.  It really doesn't
// 	// matter.  Something to program someday if you feel like it for fun.
// 	// fmt.Printf("size is %d\n", f.data.size())
// 	// rows increase
// 	f.data.resize(2 << destRows)
// 	// fmt.Printf("size is %d\n", f.data.size())
// 	pos := uint64(1 << destRows) // leftmost position of row 1
// 	reach := pos >> 1            // how much to next row up
// 	// start on row 1, row 0 doesn't move
// 	for h := uint8(1); h < destRows; h++ {
// 		runLength := reach >> 1
// 		for x := uint64(0); x < runLength; x++ {
// 			// ok if source position is non-empty
// 			ok := f.data.size() > (pos>>1)+x &&
// 				f.data.read((pos>>1)+x) != empty
// 			src := f.data.read((pos >> 1) + x)
// 			if ok {
// 				f.data.write(pos+x, src)
// 			}
// 		}
// 		pos += reach
// 		reach >>= 1
// 	}

// 	// zero out (what is now the) right half of the bottom row
// 	//	copy(t.fs[1<<(t.rows-1):1<<t.rows], make([]Hash, 1<<(t.rows-1)))
// 	for x := uint64(1 << f.rows); x < 1<<destRows; x++ {
// 		// here you may actually need / want to delete?  but numleaves
// 		// should still ensure that you're not reading over the edge...
// 		f.data.write(x, empty)
// 	}

// 	f.rows = destRows
// 	return nil
// }

// // // Don't think I need this so commenting out - Bolton
// // // sanity checks forest sanity: does numleaves make sense, and are the roots
// // // populated?
// // func (f *Forest) sanity() error {

// // 	if f.numLeaves > 1<<f.rows {
// // 		return fmt.Errorf("forest has %d leaves but insufficient rows %d",
// // 			f.numLeaves, f.rows)
// // 	}
// // 	rootPositions, _ := getRootsReverse(f.numLeaves, f.rows)
// // 	for _, t := range rootPositions {
// // 		if f.data.read(t) == empty {
// // 			return fmt.Errorf("Forest has %d leaves %d roots, but root @%d is empty",
// // 				f.numLeaves, len(rootPositions), t)
// // 		}
// // 	}
// // 	if uint64(len(f.positionMap)) > f.numLeaves {
// // 		return fmt.Errorf("sanity: positionMap %d leaves but forest %d leaves",
// // 			len(f.positionMap), f.numLeaves)
// // 	}

// // 	return nil
// // }

// // // PosMapSanity is costly / slow: check that everything in posMap is correct
// // func (f *Forest) PosMapSanity() error {
// // 	for i := uint64(0); i < f.numLeaves; i++ {
// // 		if f.positionMap[f.data.read(i).Mini()] != i {
// // 			return fmt.Errorf("positionMap error: map says %x @%d but @%d",
// // 				f.data.read(i).Prefix(), f.positionMap[f.data.read(i).Mini()], i)
// // 		}
// // 	}
// // 	return nil
// // }

// // RestoreForest restores the forest on restart. Needed when resuming after exiting.
// // miscForestFile is where numLeaves and rows is stored
// func RestoreForest(
// 	miscForestFile *os.File, forestFile *os.File, toRAM, cached bool) (*Forest, error) {

// 	// start a forest for restore
// 	f := new(Forest)

// 	// Restore the numLeaves
// 	err := binary.Read(miscForestFile, binary.BigEndian, &f.numLeaves)
// 	if err != nil {
// 		return nil, err
// 	}
// 	fmt.Println("Forest leaves:", f.numLeaves)
// 	// Restore number of rows
// 	// TODO optimize away "rows" and only save in minimzed form
// 	// (this requires code to shrink the forest
// 	binary.Read(miscForestFile, binary.BigEndian, &f.rows)
// 	if err != nil {
// 		return nil, err
// 	}
// 	fmt.Println("Forest rows:", f.rows)

// 	// open the forest file on disk even if we're going to ram
// 	diskData := new(diskForestData)
// 	diskData.file = forestFile

// 	if toRAM {
// 		// for in-ram
// 		ramData := new(ramForestData)
// 		fmt.Printf("%d rows resize to %d\n", f.rows, 2<<f.rows)
// 		ramData.resize(2 << f.rows)

// 		// Can't read all at once!  There's a (secret? at least not well
// 		// documented) maxRW of 1GB.
// 		var bytesRead int
// 		for bytesRead < len(ramData.m) {
// 			n, err := diskData.file.Read(ramData.m[bytesRead:])
// 			if err != nil {
// 				return nil, err
// 			}
// 			bytesRead += n
// 			fmt.Printf("read %d bytes of forest file into ram\n", bytesRead)
// 		}

// 		f.data = ramData

// 		// for i := uint64(0); i < f.data.size(); i++ {
// 		// f.data.write(i, diskData.read(i))
// 		// if i%100000 == 0 && i != 0 {
// 		// fmt.Printf("read %d nodes from disk\n", i)
// 		// }
// 		// }

// 	} else {
// 		if cached {
// 			// on disk, with cache
// 			cfd := new(cacheForestData)
// 			cfd.cache = newDiskForestCache(20)
// 			cfd.file = forestFile
// 			f.data = cfd
// 		} else {
// 			// on disk, no cache
// 			f.data = diskData
// 		}
// 		// assume no resize needed
// 	}

// 	// Restore positionMap by rebuilding from all leaves
// 	f.positionMap = make(map[MiniHash]uint64)
// 	fmt.Printf("%d leaves for position map\n", f.numLeaves)
// 	for i := uint64(0); i < f.numLeaves; i++ {
// 		f.positionMap[f.data.read(i).Mini()] = i
// 		if i%100000 == 0 && i != 0 {
// 			fmt.Printf("Added %d leaves %x\n", i, f.data.read(i).Mini())
// 		}
// 	}
// 	if f.positionMap == nil {
// 		return nil, fmt.Errorf("Generated positionMap is nil")
// 	}

// 	fmt.Println("Done restoring forest")

// 	// for cacheForestData the `hashCount` field gets
// 	// set through the size() call.
// 	f.data.size()

// 	return f, nil
// }

// func (f *Forest) PrintPositionMap() string {
// 	var s string
// 	for pos := uint64(0); pos < f.numLeaves; pos++ {
// 		l := f.data.read(pos).Mini()
// 		s += fmt.Sprintf("pos %d, leaf %x map to %d\n", pos, l, f.positionMap[l])
// 	}

// 	return s
// }

// // WriteMiscData writes the numLeaves and rows to miscForestFile
// func (f *Forest) WriteMiscData(miscForestFile *os.File) error {
// 	fmt.Println("numLeaves=", f.numLeaves)
// 	fmt.Println("f.rows=", f.rows)

// 	err := binary.Write(miscForestFile, binary.BigEndian, f.numLeaves)
// 	if err != nil {
// 		return err
// 	}

// 	err = binary.Write(miscForestFile, binary.BigEndian, f.rows)
// 	if err != nil {
// 		return err
// 	}

// 	f.data.close()

// 	return nil
// }

// // WriteForestToDisk writes the whole forest to disk
// // this only makes sense to do if the forest is in ram.  So it'll return
// // an error if it's not a ramForestData
// func (f *Forest) WriteForestToDisk(dumpFile *os.File) error {

// 	ramForest, ok := f.data.(*ramForestData)
// 	if !ok {
// 		return fmt.Errorf("WriteForest only possible with ram forest")
// 	}

// 	_, err := dumpFile.Seek(0, 0)
// 	if err != nil {
// 		return fmt.Errorf("WriteForest seek %s", err.Error())
// 	}
// 	_, err = dumpFile.Write(ramForest.m)
// 	if err != nil {
// 		return fmt.Errorf("WriteForest write %s", err.Error())
// 	}

// 	return nil
// }

// // getRoots returns all the roots of the trees
// func (f *Forest) getRoots() []Hash {

// 	rootPositions, _ := getRootsReverse(f.numLeaves, f.rows)
// 	roots := make([]Hash, len(rootPositions))

// 	for i, _ := range roots {
// 		roots[i] = f.data.read(rootPositions[i])
// 	}

// 	return roots
// }

// Stats :
func (f *Forest) Stats() string {

	s := fmt.Sprintf("numleaves: %d hashesever: %d forest: %d\n",
		f.numLeaves, f.HistoricHashes, f.data.size())

	s += fmt.Sprintf("\thashT: %.2f remT: %.2f (of which MST %.2f) proveT: %.2f",
		f.TimeInHash.Seconds(), f.TimeRem.Seconds(), f.TimeMST.Seconds(),
		f.TimeInProve.Seconds())
	return s
}

// ToString prints out the whole thing.  Only viable for small forests
func (f *Forest) ToString() string {

	return f.lookup.String()
}

// // FindLeaf finds a leave from the positionMap and returns a bool
// func (f *Forest) FindLeaf(leaf Hash) bool {
// 	_, found := f.positionMap[leaf.Mini()]
// 	return found
// }
