package patriciaaccumulator

import (
	"fmt"
	"os"
	"time"

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

	lookup *patriciaLookup

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

			f.lookup = &patriciaLookup{Hash{}, &treeNodes, make(map[MiniHash]uint64)}
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

// NewForestParams Makes a new forest
func NewForestParams(ramCacheSlots int) *Forest {
	f := new(Forest)
	f.numLeaves = 0
	f.maxLeaf = 0

	// panic("We cannot yet create a forest from cache or memory")

	// for on-disk with cache
	// 2_000_000 lems in ram seems good, not really enough to cause ram issues, but enough to speed things up
	// With 2_000_000 it seems to only use 3 gigs at most, so 10million should be safe
	treeNodes := newRAMCacheTreeNodes(nil, ramCacheSlots)
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

	f.lookup = &patriciaLookup{Hash{}, &treeNodes, make(map[MiniHash]uint64)}
	// Changed this to a reference
	// This code runs, but I don't understand why.
	// Shouldn't treeNodes be overwritten when it goes out of scope?
	// I guess it's actually idiomatic
	// https://stackoverflow.com/a/28485041/3854633

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

// Add adds leaves to the forest.  This is the easy part.
func (f *Forest) addv2(adds []Leaf) error {
	start := time.Now()

	if len(adds) == 0 {
		return nil
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
		// rootNode, ok := f.lookup.treeNodes.read(f.lookup.stateRoot)
		// if !ok {
		// 	panic("could not find state root")
		// }
		// f.lookup.treeNodes.delete(f.lookup.stateRoot)
		if len(addHashes) == 0 {
			panic("There should be hashes to add")
		}
		initialStateRoot := f.lookup.stateRoot

		newStateRoot, err := f.lookup.recursiveAddRight(f.lookup.stateRoot, location, addHashes, nil)
		if err != nil {
			panic("")
		}

		if newStateRoot == initialStateRoot {
			panic("The new state root should be different")
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

	logrus.Printf("Modify: starting with %d leaves, deleting %d, adding %d, max leaf %d\n", f.numLeaves, numDels, numAdds, f.maxLeaf)

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

	t0 := time.Now()
	// v3 should do the exact same thing as v2 now
	// fmt.Printf("Beginning Deletes\n")
	f.lookup.treeNodes.clearHitTracker()
	err := f.removev5(dels)
	f.lookup.treeNodes.printHitTracker()
	if err != nil {
		return nil, err
	}
	// f.cleanup(uint64(numDels))

	t1 := time.Now()

	// save the leaves past the edge for undo
	// dels hasn't been mangled by remove up above, right?
	// BuildUndoData takes all the stuff swapped to the right by removev3
	// and saves it in the order it's in, which should make it go back to
	// the right place when it's swapped in reverse
	// ub := f.BuildUndoData(uint64(numAdds), dels)

	// fmt.Printf("Beginning Adds\n")
	f.lookup.treeNodes.clearHitTracker()
	err = f.addv2(adds)
	f.lookup.treeNodes.printHitTracker()

	if err != nil {
		return nil, err
	}

	t2 := time.Now()

	// fmt.Printf("done modifying block, added %d\n", len(adds))
	// fmt.Printf("post add %s\n", f.ToString())
	// for m, p := range f.positionMap {
	// 	fmt.Printf("%x @%d\t", m[:4], p)
	// }
	// fmt.Printf("\n")
	logrus.Println("Modified - remove time: ", t1.Sub(t0))
	logrus.Println("Modified - add time:", t2.Sub(t1))

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
