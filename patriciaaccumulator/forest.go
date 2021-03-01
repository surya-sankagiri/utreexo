package patriciaaccumulator

import (
	"fmt"
	"os"
	"time"

	logrus "github.com/sirupsen/logrus"
)

// TODO clean up this file

// An all zeros byte array, for representing the root hash of an empty tree
var empty [32]byte

// Forest is the entire accumulator of the UTXO set.
type Forest struct {
	numLeaves uint64 // number of leaves in the forest (bottom row)
	maxLeaf   uint64 // the index of the largest leaf that has ever existed in the trie

	lookup *patriciaLookup // TODO maybe get rid of this struct altogether and just put the fields in Forest
}

// NewForest makes a new forest
// forestFile indicates location to save tree nodes on disk. If nil, defaults to blocks/utree/forestdata/
func NewForest(forestFile *os.File, cached bool) *Forest {

	if forestFile != nil {
		panic("The code does not currently support creating the file in a non-default location")
	}

	if cached {
		panic("This code does not currently support caching")
	}

	f := new(Forest)
	f.numLeaves = 0
	f.maxLeaf = 0

	treeNodes := newRAMCacheTreeNodes(nil, 10000000)

	f.lookup = &patriciaLookup{Hash{}, &treeNodes}

	return f
}

// NewForestParams makes a new forest
// takes as input a number of slots
func NewForestParams(ramCacheSlots int) *Forest {
	f := new(Forest)
	f.numLeaves = 0
	f.maxLeaf = 0

	treeNodes := newRAMCacheTreeNodes(nil, ramCacheSlots)

	f.lookup = &patriciaLookup{Hash{}, &treeNodes}

	return f
}

// Add adds leaves to the forest.
func (f *Forest) Add(adds []Leaf) error {

	// If there is nothing to add, simply return
	if len(adds) == 0 {
		return nil
	}

	location := f.maxLeaf

	// Update counts
	f.maxLeaf += uint64(len(adds))
	f.numLeaves += uint64(len(adds))

	// Compile the hashes from the leaves to be added into one slice
	addHashes := make([]Hash, 0)

	for _, add := range adds {

		addHashes = append(addHashes, add.Hash)
	}

	// Check if there is anything currently in the trie
	if f.lookup.stateRoot == empty {
		// If there is not, make a new tree out of the added elements.

		newStateRoot, err := f.lookup.makeNewTree(location, addHashes)
		if err != nil {
			return err
		}

		f.lookup.stateRoot = newStateRoot

	} else {
		// If there is, use recursiveAddRight to add the new nodes

		newStateRoot, err := f.lookup.recursiveAddRight(f.lookup.stateRoot, location, addHashes, nil)
		if err != nil {
			return err
		}

		f.lookup.stateRoot = newStateRoot
	}

	return nil

}

// Remove removes the leaves at the specified locations from the trie forest
func (f *Forest) Remove(locations []uint64) error {

	// Sanity check number of nodes deleted
	if f.numLeaves < uint64(len(locations)) {
		return fmt.Errorf("Attempting to delete %v nodes, only %v exist", len(locations), f.numLeaves)
	}

	// Update number of leaves
	f.numLeaves -= uint64(len(locations))

	logrus.Debug("Calling remove from subtree")

	// Use removeFromSubtree to find and remove these locations
	newStateRoot, _, err := f.lookup.removeFromSubtree(locations, f.lookup.stateRoot)
	if err != nil {
		return err
	}
	f.lookup.stateRoot = newStateRoot

	return nil

}

// Modify changes the forest, adding and deleting leaves and updating internal nodes.
// Note that this does not modify in place!  All deletes are followed by
// adds, which show up on the right.
// Also, the deletes need there to be correct proof data, so you should first call Verify().
func (f *Forest) Modify(adds []Leaf, dels []uint64) (*undoBlock, error) {

	numDels, numAdds := len(dels), len(adds)
	logrus.Printf("Modify: starting with %d leaves, deleting %d, adding %d, max leaf %d\n", f.numLeaves, numDels, numAdds, f.maxLeaf)

	// Check that dels are received in sorted order
	if !checkSortedNoDupes(dels) {
		fmt.Printf("%v\n", dels)
		return nil, fmt.Errorf("Deletions in incorrect order or duplicated")
	}

	t0 := time.Now()

	logrus.Printf("Beginning Remove\n")
	f.lookup.treeNodes.clearHitTracker()
	err := f.Remove(dels)
	f.lookup.treeNodes.printHitTracker()
	if err != nil {
		return nil, err
	}

	t1 := time.Now()

	logrus.Printf("Beginning Adds\n")
	f.lookup.treeNodes.clearHitTracker()
	err = f.Add(adds)
	f.lookup.treeNodes.printHitTracker()

	if err != nil {
		return nil, err
	}
	// time.Sleep(1 * time.Second)

	t2 := time.Now()

	logrus.Printf("Done modifying block, added %d, deleted %d\n", numAdds, numDels)
	logrus.Println("Modified - remove time: ", t1.Sub(t0))
	logrus.Println("Modified - add time:", t2.Sub(t1))

	return nil, err
}

// String prints out the whole tree. Only viable for small forests.
func (f *Forest) String() string {

	return f.lookup.String()
}
