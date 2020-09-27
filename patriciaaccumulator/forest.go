package patriciaaccumulator

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/bits"
	"os"
	"sort"
	"time"
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

type patriciaLookup struct {
	stateRoot     Hash
	treeNodes     map[Hash]patriciaNode
	leafLocations map[Hash]uint64
	// TODO add a leaf node to location map?
}

type patriciaNode struct {
	left     Hash
	right    Hash
	midpoint uint64 // The midpoint of the binary interval represented by the common
}

func (p *patriciaNode) min() uint64 {
	// If a leaf node,
	if p.left == p.right {
		return p.midpoint
	}
	halfWidth := ((p.midpoint - 1) & p.midpoint) ^ p.midpoint
	return p.midpoint - halfWidth
}

func (p *patriciaNode) max() uint64 {
	// If a leaf node,
	if p.left == p.right {
		return p.midpoint + 1
	}
	halfWidth := ((p.midpoint - 1) & p.midpoint) ^ p.midpoint
	return p.midpoint + halfWidth
}

func (p *patriciaNode) inRange(v uint64) bool {
	// If a leaf node, in range if equal to location
	if p.left == p.right {
		return v == p.midpoint
	}
	// Otherwise
	return p.min() <= v && v < p.max()
}

func (p *patriciaNode) inLeft(v uint64) bool {
	if p.left == p.right {
		panic("inLeft should not be called on leaf node")
	}
	if p.midpoint == 0 {
		panic("inLeft should not be called on midpoint 0")
	}
	return p.min() <= v && v < p.midpoint
}

func (p *patriciaNode) hash() Hash {
	var empty Hash
	if p.left == empty || p.right == empty {
		panic("got an empty leaf here. ")
	}
	hashBytes := append(p.left[:], p.right[:]...)
	midpointBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(midpointBytes, p.midpoint)
	return sha256.Sum256(append(hashBytes, midpointBytes...))
}

func newPatriciaNode(child1, child2 patriciaNode) patriciaNode {

	var p patriciaNode

	var leftChild, rightChild patriciaNode

	if !(child1.max() <= child2.min() || child2.max() <= child1.min()) {
		panic(fmt.Sprintf("Cannot combine nodes with overlapping ranges %d to %d and %d to %d", child1.min(), child1.max(), child2.min(), child2.max()))
	}
	if !(child1.min() < child1.max()) {
		panic(fmt.Sprintf("Node has min (%d) not less than max (%d)", child1.min(), child1.max()))
	}
	if !(child2.min() < child2.max()) {
		panic(fmt.Sprintf("Node has min (%d) not less than max (%d)", child2.min(), child2.max()))
	}
	// Does child1 come first?
	if child1.min() < child2.min() {
		leftChild = child1
		rightChild = child2
	} else {
		leftChild = child2
		rightChild = child1
	}

	p.left = leftChild.hash()
	p.right = rightChild.hash()

	differentBits := leftChild.midpoint ^ rightChild.midpoint

	i := bits.Reverse64(differentBits)
	i = i &^ (i - 1)
	i = bits.Reverse64(i)
	// i is first different bit between child midpoints

	p.midpoint = (leftChild.midpoint &^ (i - 1)) | i

	return p
}

// func NewPatriciaLookup() *patriciaLookup {
// 	t := new(patriciaLookup)
// 	t.treeNodes = make(map[Hash]patriciaNode)
// 	return t
// }

func (t *patriciaLookup) merge(other *patriciaLookup) {
	for hash, node := range other.treeNodes {
		t.treeNodes[hash] = node
	}
}

// For debugging
func (t *patriciaLookup) String() string {

	if t.stateRoot != empty {
		return fmt.Sprint("Root is: ", t.stateRoot[:6], "\n", t.subtreeString(t.stateRoot), "\n")
	}
	return "empty tree"
}

func (t *patriciaLookup) subtreeString(nodeHash Hash) string {
	node, _ := t.treeNodes[nodeHash]

	out := ""

	if node.left == node.right {
		return fmt.Sprint("leafnode ", "hash ", nodeHash[:6], " midpoint ", node.midpoint, " left ", node.left[:6], " right ", node.right[:6], "\n")
	}
	out += fmt.Sprint("hash ", nodeHash[:6], " midpoint ", node.midpoint, " left ", node.left[:6], " right ", node.right[:6], "\n")
	out += t.subtreeString(node.left)
	out += t.subtreeString(node.right)

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
	proof.midpoints = make([]uint64, 0, 64)
	proof.hashes = make([]Hash, 0, 64)
	var node patriciaNode
	var nodeHash Hash
	var neighborHash Hash
	// var midpoints = make([]uint64, 0, 64)
	// var neighborHashes = make([]Hash, 0, 64)
	var ok bool
	// Start at the root of the tree
	node, ok = t.treeNodes[t.stateRoot]
	if !ok {
		return proof, fmt.Errorf("state root %x not found", t.stateRoot)
	}
	// Discover the path to the leaf
	for {
		proof.midpoints = append(proof.midpoints, node.midpoint)
		if !node.inRange(target) {
			// The target location is not in range; this is an error
			// REMARK (SURYA): The current system has no use for proof of non-existence, nor has the means to create one
			return proof, fmt.Errorf("target location %d not found", target)
		}

		// If the min is the max, we are at a leaf
		// TODO do we want to include the midpoint of the leaf node?
		if node.left == node.right {
			// REMARK (SURYA): We do not check whether the hash corresponds to the hash of a leaf
			//perform a sanity check here
			if len(proof.midpoints) != len(proof.hashes)+1 {
				panic("# midpoints not equal to # hashes")
			}
			if node.midpoint != target {
				panic("midpoint of leaf not equal to target")
			}
			// proof = ConstructProof(target, midpoints, neighborHashes)
			return proof, nil
		}
		if node.inLeft(target) {
			nodeHash = node.left
			neighborHash = node.right
		} else {
			nodeHash = node.right
			neighborHash = node.left
		}

		// We are not yet at the leaf, so we descend the tree.
		node, ok = t.treeNodes[nodeHash]
		if !ok {
			return proof, fmt.Errorf("Patricia Node %x not found", nodeHash)
		}
		_, ok = t.treeNodes[neighborHash]
		if !ok {
			return proof, fmt.Errorf("Patricia Node %x not found", neighborHash)
		}
		proof.hashes = append(proof.hashes, neighborHash)
	}
}

// RetrieveListProofs creates a list of individual proofs for targets, in sorted order
func (t *patriciaLookup) RetrieveListProofs(targets []uint64) ([]PatriciaProof, error) {

	// A slice of proofs of individual elements
	individualProofs := make([]PatriciaProof, 0, 3000)

	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })

	for _, target := range targets {
		proof, _ := t.RetrieveProof(target)

		if proof.midpoints[0] != t.treeNodes[t.stateRoot].midpoint {
			panic("Wrong root midpoint")
		}

		individualProofs = append(individualProofs, proof)
	}

	return individualProofs, nil

}

// RetrieveBatchProof creates a proof for a batch of targets against a state root
// The proof consists of:
//   The targets we are proving for (in left to right order)
// 	 all midpoints on the main branches from the root to (Just before?) the proved leaves (in any order)
//   all hashes of nodes that are neighbors of nodes on the main branches, but not on a main branch themselves (in DFS order with lower level nodes first, the order the hashes will be needed when the stateless node reconstructs the proof branches)
// NOTE: This version is meant to trim the data that's not needed,
func (t *patriciaLookup) RetrieveBatchProof(targets []uint64) BatchProof {

	// If no targets, return empty batchproof
	if len(targets) == 0 {
		return BatchProof{targets, make([]Hash, 0, 0), make([]uint64, 0, 0)}
	}

	// A slice of proofs of individual elements
	individualProofs, _ := t.RetrieveListProofs(targets)

	var midpoints = make([]uint64, 0, 0)

	for _, proof := range individualProofs {
		midpoints = append(midpoints, proof.midpoints...)
	}

	sortRemoveDuplicates(midpoints)
	// TODO compress this list

	hashes := make([]Hash, len(individualProofs[0].hashes))
	copy(hashes, individualProofs[0].hashes)

	// To compress this data into a BatchProof we must remove
	// the hash children of nodes which occur in two individual proofs but fork
	for i := 1; i < len(individualProofs); i++ {
		proofCurrent := individualProofs[i]
		proofPrev := individualProofs[i-1]
		// Iterate through i and i-1 to find the fork
		for j, midpoint := range proofCurrent.midpoints {
			if midpoint != proofPrev.midpoints[j] {
				// Fork found
				// Delete the hashes at j-1 from both
				// filterDelete(hashes, proofCurrent.hashes[j-1])
				filterDelete(hashes, proofPrev.hashes[j-1])
				// Now add the hashes from proof A from after the fork
				hashes = append(hashes, proofCurrent.hashes[j:]...)
				break

			}
		}
	}

	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })
	return BatchProof{targets, hashes, midpoints}

	// TODO

}

func (f *Forest) ProveBatch(hashes []Hash) (BatchProof, error) {
	targets := make([]uint64, len(hashes))
	for i, hash := range hashes {
		targets[i] = f.lookup.leafLocations[hash]
	}

	return f.lookup.RetrieveBatchProof(targets), nil
}

// Helper function that sorts a slice and removes duplicate
func sortRemoveDuplicates(a []uint64) {
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })

	for i := 0; i < len(a)-1; i++ {
		if a[i] == a[i+1] {
			// From https://yourbasic.org/golang/delete-element-slice/
			// Remove the element at index i from a.
			copy(a[i:], a[i+1:]) // Shift a[i+1:] left one index.
			// a[len(a)-1] = ""     // Erase last element (write zero value).
			a = a[:len(a)-1] // Truncate slice.

			i--
		}
	}
}

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
	newLeafNode := patriciaNode{toAdd, toAdd, location}
	t.treeNodes[newLeafNode.hash()] = newLeafNode
	t.leafLocations[toAdd] = location

	fmt.Println("in add", location, toAdd, t.stateRoot, len(t.treeNodes))

	if t.stateRoot == empty {
		// If the patriciaLookup is empty (has empty root), treeNodes should be empty
		if len(t.treeNodes) > 1 {
			return fmt.Errorf("stateRoot empty, but treeNodes was populated")
		}

		// The patriciaLookup is empty, we make a single root and return
		t.stateRoot = newLeafNode.hash()

		return nil

	}
	node, ok := t.treeNodes[t.stateRoot]
	fmt.Println("root midpoint is", node.midpoint)
	if !ok {
		return fmt.Errorf("state root %x not found", t.stateRoot)
	}

	var hash Hash
	var neighbor patriciaNode
	neighborBranch := make([]patriciaNode, 0, 64)
	mainBranch := make([]patriciaNode, 0, 64)

	// We descend the tree until we find a node which does not contain the location at which we are adding
	// This node should be the sibling of the new leaf
	for node.inRange(location) {
		// mainBranch will consist of all the nodes to be replaced, from the root to the parent of the new sibling of the new leaf
		mainBranch = append(mainBranch, node)
		// If the min is the max, we are at a preexisting leaf
		// THIS SHOULD NOT HAPPEN. We never add on top of something already added in this code
		// (Block validity should already be checked at this point)
		if node.left == node.right {
			panic("If the min is the max, we are at a preexisting leaf. We should never add on top of something already added.")
		}

		// Determine if we descend left or right
		if node.inLeft(location) {
			neighbor = t.treeNodes[node.right]
			hash = node.left
		} else {
			neighbor = t.treeNodes[node.left]
			hash = node.right
		}
		// Add the other direction node to the branch
		neighborBranch = append(neighborBranch, neighbor)
		// Update node down the tree
		node = t.treeNodes[hash]

	} // End loop

	// Combine leaf node with its new sibling
	nodeToAdd := newPatriciaNode(newLeafNode, node)
	t.treeNodes[nodeToAdd.hash()] = nodeToAdd

	// We travel up the branch, recombining nodes
	for len(neighborBranch) > 0 {
		// nodeToAdd must now replace the hash of the last node of mainBranch in the second-to-last node of mainBranch
		// Pop neighborNode off
		neighborNode := neighborBranch[len(neighborBranch)-1]
		neighborBranch = neighborBranch[:len(neighborBranch)-1]
		nodeToAdd = newPatriciaNode(neighborNode, nodeToAdd)
		t.treeNodes[nodeToAdd.hash()] = nodeToAdd
	}
	// Delete all nodes in the main branch, they have been replaced
	for _, node := range mainBranch {
		delete(t.treeNodes, node.hash())
	}

	// The new state root is the hash of the last node added
	t.stateRoot = nodeToAdd.hash()
	fmt.Println("new root midpoint is", nodeToAdd.midpoint)

	return nil
}

// Based on add above: this code removes a location
func (t *patriciaLookup) remove(location uint64) {

	// TODO: should state root be a property of patriciaLookup, and we avoid a single lookup representing multiple roots?
	// TODO: Should the proof branch be the input, so this can be called without appatriciaLookup by a stateless node
	// branch := t.RetrieveProof(toAdd, location)

	empty := Hash{}
	if t.stateRoot == empty {
		panic("State root is empty hash")
	}

	node, ok := t.treeNodes[t.stateRoot]
	if !ok {
		panic("Could not find root in nodes")
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
		if node.inLeft(location) {
			neighbor = t.treeNodes[node.right]
			hash = node.left
		} else {
			neighbor = t.treeNodes[node.left]
			hash = node.right
		}
		// Add the other direction node to the branch
		neighborBranch = append(neighborBranch, neighbor)
		// Update node down the tree
		node = t.treeNodes[hash]
		fmt.Printf("midpoint %d\n", node.midpoint)

	} // End loop
	mainBranch = append(mainBranch, node)

	// Check that the leaf node we found has the right location
	if node.midpoint != location {
		panic(fmt.Sprintf("Found wrong location in remove, location is %d, but midpoint is %d", location, node.midpoint))
	}

	// Delete all nodes in the main branch, they are to be replaced
	for _, node := range mainBranch {
		delete(t.treeNodes, node.hash())
	}

	// If there is one element in the neighbor nodes, the leaf we deleted was a child of the root
	// The neighbor becomes the new root.
	if len(neighborBranch) == 1 {
		// return neighborBranch[0].hash()
		return
	}

	// Otherwise, the lowest new node is the combination of the sibling and uncle of the deleted leaf
	// That is, the last two nodes in the neighborBranch
	nodeToAdd := newPatriciaNode(neighborBranch[len(neighborBranch)-1], neighborBranch[len(neighborBranch)-2])
	neighborBranch = neighborBranch[:len(neighborBranch)-2]
	t.treeNodes[nodeToAdd.hash()] = nodeToAdd

	// We travel up the branch, recombining nodes
	for len(neighborBranch) > 0 {
		// nodeToAdd must now replace the hash of the last node of mainBranch in the second-to-last node of mainBranch
		neighborNode := neighborBranch[len(neighborBranch)-1]
		neighborBranch = neighborBranch[:len(neighborBranch)-1]
		nodeToAdd = newPatriciaNode(neighborNode, nodeToAdd)
		t.treeNodes[nodeToAdd.hash()] = nodeToAdd
	}

	// The new state root is the hash of the last node added
	t.stateRoot = nodeToAdd.hash()
}

// Delete the leaves at the dels location for the trie forest
func (f *Forest) removev5(dels []uint64) error {

	if f.numLeaves < uint64(len(dels)) {
		panic(fmt.Sprintf("Attempting to delete %v nodes, only %v exist", len(dels), f.numLeaves))
	}
	nextNumLeaves := f.numLeaves - uint64(len(dels))

	// check that all dels are before the maxLeaf
	for _, dpos := range dels {
		if dpos > f.maxLeaf {
			return fmt.Errorf(
				"Trying to delete leaf at %d, beyond max %d", dpos, f.maxLeaf)
		}

		f.lookup.remove(dpos)
	}

	f.numLeaves = nextNumLeaves

	return nil
}

// NewForest: Makes a new forest
//
func NewForest(forestFile *os.File, cached bool) *Forest {
	f := new(Forest)
	f.numLeaves = 0
	f.maxLeaf = 0

	if forestFile == nil {
		// for in-ram
		f.data = new(ramForestData)
	} else {
		// panic("We cannot yet create a forest from cache or memory")

		if cached {
			d := new(cacheForestData)
			d.file = forestFile
			d.cache = newDiskForestCache(20)
			f.data = d
		} else {
			// for on-disk
			d := new(diskForestData)
			d.file = forestFile
			f.data = d
		}
	}

	f.data.resize(1)
	// f.positionMap = make(map[MiniHash]uint64)
	f.lookup = patriciaLookup{Hash{}, make(map[Hash]patriciaNode), make(map[Hash]uint64)}
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

// TODO forest.removev4 and pollard.rem2 are VERY similar.  It seems like
// whether it's forest or pollard, most of the complicated stuff is the same.
// so maybe they can both satisfy an interface.  In the case of remove, the only
// specific calls are HnFromPos and swapNodes
//
//

// rnew -- emove v4 with swapHashRange
// func (f *Forest) removev4(dels []uint64) error {
// 	nextNumLeaves := f.numLeaves - uint64(len(dels))
// 	// check that all dels are there
// 	for _, dpos := range dels {
// 		if dpos > f.numLeaves {
// 			return fmt.Errorf(
// 				"Trying to delete leaf at %d, beyond max %d", dpos, f.numLeaves)
// 		}
// 	}
// 	var hashDirt []uint64
// 	swapRows := remTrans2(dels, f.numLeaves, f.rows)
// 	// loop taken from pollard rem2.  maybe pollard and forest can both
// 	// satisfy the same interface..?  maybe?  that could work...
// 	// TODO try that ^^^^^^
// 	for r := uint8(0); r < f.rows; r++ {
// 		hashDirt = updateDirt(hashDirt, swapRows[r], f.numLeaves, f.rows)
// 		for _, swap := range swapRows[r] {
// 			f.swapNodes(swap, r)
// 		}
// 		// do all the hashes at once at the end
// 		err := f.hashRow(hashDirt)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	f.numLeaves = nextNumLeaves

// 	return nil
// }

func updateDirt(hashDirt []uint64, swapRow []arrow, numLeaves uint64, rows uint8) (nextHashDirt []uint64) {
	var prevHash uint64
	hashDirt = dedupeSwapDirt(hashDirt, swapRow)
	for len(swapRow) != 0 || len(hashDirt) != 0 {
		// check if doing dirt. if not dirt, swap.
		// (maybe a little clever here...)
		popSwap, hashDest := makeDestInRow(swapRow, hashDirt, rows)
		if popSwap {
			swapRow = swapRow[1:]
		} else {
			hashDirt = hashDirt[1:]
		}
		if !inForest(hashDest, numLeaves, rows) ||
			hashDest == 0 || // TODO would be great to use nextNumLeaves... but tricky
			hashDest == prevHash { // TODO this doesn't cover everything
			continue
		}
		prevHash = hashDest
		i := sort.Search(len(nextHashDirt), func(i int) bool {
			return nextHashDirt[i] >= hashDest
		})
		if i >= len(nextHashDirt) || nextHashDirt[i] != hashDest {
			// hashDest was not in the list, and i is where
			// it should be inserted
			nextHashDirt = append(nextHashDirt, 0)
			copy(nextHashDirt[i+1:], nextHashDirt[i:])
			nextHashDirt[i] = hashDest
		}
	}
	return nextHashDirt
}

func makeDestInRow(maybeArrow []arrow, hashDirt []uint64, rows uint8) (bool, uint64) {
	if len(maybeArrow) == 0 {
		// re-descending here which isn't great
		hashDest := parent(hashDirt[0], rows)
		return false, hashDest
	}

	// swapping
	hashDest := parent(maybeArrow[0].to, rows)
	return true, hashDest
}

// func (f *Forest) swapNodes(s arrow, row uint8) {
// 	if s.from == s.to {
// 		// these shouldn't happen, and seems like the don't

// 		fmt.Printf("%s\nmove %d to %d\n", f.ToString(), s.from, s.to)
// 		panic("got non-moving swap")
// 	}
// 	if row == 0 {
// 		f.data.swapHash(s.from, s.to)
// 		f.positionMap[f.data.read(s.to).Mini()] = s.to
// 		f.positionMap[f.data.read(s.from).Mini()] = s.from
// 		return
// 	}
// 	// fmt.Printf("swapnodes %v\n", s)
// 	a := childMany(s.from, row, f.rows)
// 	b := childMany(s.to, row, f.rows)
// 	run := uint64(1 << row)

// 	// happens before the actual swap, so swapping a and b
// 	for i := uint64(0); i < run; i++ {
// 		f.positionMap[f.data.read(a+i).Mini()] = b + i
// 		f.positionMap[f.data.read(b+i).Mini()] = a + i
// 	}

// 	// start at the bottom and go to the top
// 	for r := uint8(0); r <= row; r++ {
// 		// fmt.Printf("shr %d %d %d\n", a, b, run)
// 		f.data.swapHashRange(a, b, run)
// 		a = parent(a, f.rows)
// 		b = parent(b, f.rows)
// 		run >>= 1
// 	}
// }

// // reHash hashes new data in the forest based on dirty positions.
// // right now it seems "dirty" means the node itself moved, not that the
// // parent has changed children.
// // TODO: switch the meaning of "dirt" to mean parents with changed children;
// // this will probably make it a lot simpler.
// func (f *Forest) reHash(dirt []uint64) error {
// 	if f.rows == 0 || len(dirt) == 0 { // nothing to hash
// 		return nil
// 	}
// 	rootPositions, rootRows := getRootsReverse(f.numLeaves, f.rows)

// 	dirty2d := make([][]uint64, f.rows)
// 	r := uint8(0)
// 	dirtyRemaining := 0
// 	for _, pos := range dirt {
// 		if pos > f.numLeaves {
// 			return fmt.Errorf("Dirt %d exceeds numleaves %d", pos, f.numLeaves)
// 		}
// 		dRow := detectRow(pos, f.rows)
// 		// increase rows if needed
// 		for r < dRow {
// 			r++
// 		}
// 		if r > f.rows {
// 			return fmt.Errorf("position %d at row %d but forest only %d high",
// 				pos, r, f.rows)
// 		}
// 		// if bridgeVerbose {
// 		// fmt.Printf("h %d\n", h)
// 		// }
// 		dirty2d[r] = append(dirty2d[r], pos)
// 		dirtyRemaining++
// 	}

// 	// this is basically the same as VerifyBlockProof.  Could maybe split
// 	// it to a separate function to reduce redundant code..?
// 	// nah but pretty different because the dirtyMap has stuff that appears
// 	// halfway up...

// 	var currentRow, nextRow []uint64

// 	// floor by floor
// 	for r = uint8(0); r < f.rows; r++ {
// 		if bridgeVerbose {
// 			fmt.Printf("dirty %v\ncurrentRow %v\n", dirty2d[r], currentRow)
// 		}

// 		// merge nextRow and the dirtySlice.  They're both sorted so this
// 		// should be quick.  Seems like a CS class kind of algo but who knows.
// 		// Should be O(n) anyway.

// 		currentRow = mergeSortedSlices(currentRow, dirty2d[r])
// 		dirtyRemaining -= len(dirty2d[r])
// 		if dirtyRemaining == 0 && len(currentRow) == 0 {
// 			// done hashing early
// 			break
// 		}

// 		for i, pos := range currentRow {
// 			// skip if next is sibling
// 			if i+1 < len(currentRow) && currentRow[i]|1 == currentRow[i+1] {
// 				continue
// 			}
// 			if len(rootPositions) == 0 {
// 				return fmt.Errorf(
// 					"currentRow %v no roots remaining, this shouldn't happen",
// 					currentRow)
// 			}
// 			// also skip if this is a root
// 			if pos == rootPositions[0] {
// 				continue
// 			}

// 			right := pos | 1
// 			left := right ^ 1
// 			parpos := parent(left, f.rows)

// 			//				fmt.Printf("bridge hash %d %04x, %d %04x -> %d\n",
// 			//					left, leftHash[:4], right, rightHash[:4], parpos)
// 			if f.data.read(left) == empty || f.data.read(right) == empty {
// 				f.data.write(parpos, empty)
// 			} else {
// 				par := parentHash(f.data.read(left), f.data.read(right))
// 				f.HistoricHashes++
// 				f.data.write(parpos, par)
// 			}
// 			nextRow = append(nextRow, parpos)
// 		}
// 		if rootRows[0] == r {
// 			rootPositions = rootPositions[1:]
// 			rootRows = rootRows[1:]
// 		}
// 		currentRow = nextRow
// 		nextRow = []uint64{}
// 	}

// 	return nil
// }

// // cleanup removes extraneous hashes from the forest.  Currently only the bottom
// // Probably don't need this at all, if everything else is working.
// func (f *Forest) cleanup(overshoot uint64) {
// 	for p := f.numLeaves; p < f.numLeaves+overshoot; p++ {
// 		delete(f.positionMap, f.data.read(p).Mini()) // clear position map
// 		// TODO ^^^^ that probably does nothing. or at least should...
// 		// f.data.write(p, empty) // clear forest
// 	}
// }

// // Add adds leaves to the forest.  This is the easy part.
// func (f *Forest) Add(adds []Leaf) {
// 	f.addv2(adds)
// }

// Add adds leaves to the forest.  This is the easy part.
func (f *Forest) addv2(adds []Leaf) error {

	for _, add := range adds {

		// f.lookup.printAll()

		err := f.lookup.add(f.maxLeaf, add.Hash)
		if err != nil {
			return err
		}

		f.maxLeaf++
		f.numLeaves++
	}
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

	fmt.Printf("starting with %d leaves, deleting %d, adding %d\n", f.numLeaves, numDels, numAdds)

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
	fmt.Printf("Beginning Deletes\n")
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
	ub := f.BuildUndoData(uint64(numAdds), dels)

	fmt.Printf("Beginning Adds\n")
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

	return ub, err
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
// 	// set throught the size() call.
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
