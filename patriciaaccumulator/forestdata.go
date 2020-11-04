package patriciaaccumulator

import (
	"encoding/binary"
	"fmt"
	"os"

	lru "github.com/hashicorp/golang-lru"
)

// Size of a hash and a patricia node 32 + 2 * 32 + 8
const slotSize = 104

// ForestData is the thing that holds all the hashes in the forest.  Could
// be in a file, or in ram, or maybe something else.
type ForestData interface {
	read(hash Hash) (patriciaNode, bool)
	// TODO we only ever write a node to its own hash, so get rid of first argument
	// NEW DISK IMPLEMENTATION EDIT
	// write(hash Hash, node patriciaNode)
	write(node patriciaNode)
	delete(hash Hash)
	// swapHash(a, b uint64)
	// swapHashRange(a, b, w uint64)
	size() uint64
	// resize(newSize uint64) // make it have a new size (bigger)
	close()
}

// A treenodes with a ram cache. Every entry exists in either the disk or the ram, not both
type ramCacheTreeNodes struct {
	disk        diskTreeNodes
	ram         *lru.Cache
	maxRAMElems int
}

// newRamCacheTreeNodes returns a new treenodes container with a ram cache
func newRAMCacheTreeNodes(file *os.File, maxRAMElems int) ramCacheTreeNodes {
	cache, err := lru.New(maxRAMElems)
	if err != nil {
		panic("Error making cache")
	}
	return ramCacheTreeNodes{newDiskTreeNodes(file), cache, maxRAMElems}
}

// read ignores errors. Probably get an empty hash if it doesn't work
func (d ramCacheTreeNodes) read(hash Hash) (patriciaNode, bool) {

	val, ok := d.ram.Get(hash)
	if ok {
		// In memory
		return val.(patriciaNode), ok
	}
	// If not in memory, read disk
	return d.disk.read(hash)

}

// write writes a key-value pair
func (d ramCacheTreeNodes) write(node patriciaNode) { //NEW DISK IMPLEMENTATION EDIT //write(hash Hash, node patriciaNode) {
	hash := node.hash()
	inRAM := d.ram.Contains(hash)

	if inRAM {
		// Already in ram, we are done
		// d.ram[hash] = node
		return
	}

	_, ok := d.disk.read(hash)
	if ok {
		// Already in disk, done
		return
	}

	// Not in ram or disk
	if d.ram.Len() < d.maxRAMElems {
		// Enough space, just write to ram
		d.ram.Add(hash, node)
	} else {
		// Not enough space, move something in ram to disk
		oldHash, oldNode, ok := d.ram.RemoveOldest()
		if !ok {
			panic("Should not be empty")
		}
		//NEW DISK IMPLEMENTATION EDIT
		//d.disk.write(oldHash.(Hash), oldNode.(patriciaNode))
		d.disk.write(oldNode.(patriciaNode))
		d.ram.Add(hash, node)

	}
}

func (d ramCacheTreeNodes) delete(node patriciaNode) { //NEW DISK IMPLEMENTATION EDIT //delete(hash Hash) {
	//NEW DISK IMPLEMENTATION EDIT
	hash := node.hash()
	// Delete from ram
	present := d.ram.Remove(hash)

	if !present {
		// Delete from disk
		//NEW DISK IMPLEMENTATION EDIT
		//d.disk.delete(hash)
		d.disk.delete(node)
	}
}

// size gives you the size of the forest
func (d ramCacheTreeNodes) size() uint64 {
	return d.disk.size() + uint64(d.ram.Len())
}

func (d ramCacheTreeNodes) close() {
	d.disk.close()
}

// diskTreeNodes is a disk backed key value store from hashes to patricia nodes
// for the lookup.
// originally, we had this as a golang map, but we realized, as the original
// UTREEXO team did, that it was infeasible to store the whole thing in RAM.
// Thus, we created this.
// Our design is somwhat different. In the original UTREEXO a hash's location in the file corresponded directly with its location in the tree.
// Here instead, we combine the positionMap in
/*
type diskTreeNodes struct {
	file     *os.File
	indexMap map[MiniHash]uint64
}

// newDiskTreeNodes makes a new file-backed tree nodes container
func newDiskTreeNodes(file *os.File) diskTreeNodes {
	return diskTreeNodes{file, make(map[MiniHash]uint64)}
}

// ********************************************* forest on disk
// type diskForestData struct {
// 	file *os.File
// }

// read ignores errors. Probably get an empty hash if it doesn't work
func (d diskTreeNodes) read(hash Hash) (patriciaNode, bool) {
	var slotBytes [slotSize]byte
	var nodeHash, left, right Hash

	index, ok := d.indexMap[hash.Mini()]
	// not in index so not present
	if !ok {
		return patriciaNode{}, ok
	}
	_, err := d.file.ReadAt(slotBytes[:], int64(index*slotSize))
	if err != nil {
		fmt.Printf("\tWARNING!! read %x pos %d %s\n", hash, index, err.Error())
	}

	copy(nodeHash[:], slotBytes[0:32])
	copy(left[:], slotBytes[32:64])
	copy(right[:], slotBytes[64:96])
	midpoint := binary.LittleEndian.Uint64(slotBytes[96:104])
	if nodeHash != hash {
		panic("MiniHash Collision TODO do something different to secure this, this is just a kludge in place of a more sophisticated disk-backed key-value store")
	}

	node := patriciaNode{left, right, midpoint}
	if nodeHash != node.hash() {
		panic("Hash of node is wrong")
	}
	return node, true
}

// write writes a key-value pair
func (d diskTreeNodes) write(hash Hash, node patriciaNode) {
	s, err := d.file.Stat()
	elems := s.Size() / slotSize
	if err != nil {
		panic("file size error")
	}

	_, ok := d.read(hash)

	if ok {
		// Key already present, overwrite it
		panic("This should not happen, the key is always the hash of the value")
		// Futhermore, we should never write twice if something is already there
	} else {
		err := d.file.Truncate(int64((elems + 1) * slotSize))
		if err != nil {
			panic(err)
		}

		_, err = d.file.WriteAt(hash[:], int64(elems*slotSize))
		_, err = d.file.WriteAt(node.left[:], int64(elems*slotSize+32))
		_, err = d.file.WriteAt(node.right[:], int64(elems*slotSize+64))
		midpointBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(midpointBytes, node.midpoint)
		_, err = d.file.WriteAt(midpointBytes[:], int64(elems*slotSize+96))

		// Update the index
		d.indexMap[hash.Mini()] = uint64(elems)

		if err != nil {
			fmt.Printf("\tWARNING!! write pos %s\n", err.Error())
		}
	}

}

func (d diskTreeNodes) delete(hash Hash) {
	start := time.Now()
	s, err := d.file.Stat()
	elems := s.Size() / slotSize

	index, ok := d.indexMap[hash.Mini()]
	// not in index so not present
	if !ok {
		panic("Deleting thing that does not exist")
	}

	// copy the last element of the file

	var endhash, left, right Hash
	var midpointBytes [8]byte
	prelims := time.Now()
	_, err = d.file.ReadAt(endhash[:], int64((elems-1)*slotSize))
	_, err = d.file.ReadAt(left[:], int64((elems-1)*slotSize+32))
	_, err = d.file.ReadAt(right[:], int64((elems-1)*slotSize+64))
	_, err = d.file.ReadAt(midpointBytes[:], int64((elems-1)*slotSize+96))
	readEnd := time.Now()
	// write to the overwritten slot

	_, err = d.file.WriteAt(endhash[:], int64(index*slotSize))
	_, err = d.file.WriteAt(left[:], int64(index*slotSize+32))
	_, err = d.file.WriteAt(right[:], int64(index*slotSize+64))
	_, err = d.file.WriteAt(midpointBytes[:], int64(index*slotSize+96))
	writeEnd := time.Now()
	// Update the index
	d.indexMap[endhash.Mini()] = index

	delete(d.indexMap, hash.Mini())
	truncateBegin := time.Now()
	err = d.file.Truncate(int64((elems - 1) * slotSize))
	end := time.Now()
	if left[0] == right[0] { //random condition
		fmt.Println("Time for prelims:", prelims.Sub(start), "for reads:", readEnd.Sub(prelims), "for writes:", writeEnd.Sub(readEnd), "for truncate:", end.Sub(truncateBegin))
	}
	if err != nil {
		fmt.Printf("\tWARNING!! write pos %s\n", err.Error())
	}

}

// size gives you the size of the forest
func (d diskTreeNodes) size() uint64 {
	s, err := d.file.Stat()
	if err != nil {
		fmt.Printf("\tWARNING: %s. Returning 0", err.Error())
		return 0
	}
	return uint64(s.Size() / slotSize)
}

func (d diskTreeNodes) close() {
	err := d.file.Close()
	if err != nil {
		fmt.Printf("diskForestData close error: %s\n", err.Error())
	}
}
*/
type midpointLeaf struct {
	midpoint uint64
	leaf     bool
}

// diskTreeNodes : New implementation of diskTreeNodes - Nov 4 2020
type diskTreeNodes struct {
	file             *os.File
	hashMidpointMap  map[MiniHash]midpointLeaf
	midpointIndexMap map[midpointLeaf]uint64
	emptyIndices     []uint64
}

// newDiskTreeNodes makes a new file-backed tree nodes container
func newDiskTreeNodes(file *os.File) diskTreeNodes {
	return diskTreeNodes{file, make(map[MiniHash]midpointLeaf), make(map[midpointLeaf]uint64), make([]uint64, 0)}
}

// write writes a new PatriciaNode to memory
func (d diskTreeNodes) write(node patriciaNode) {
	var index uint64
	hash := node.hash()
	// check if this hash is already present in hashMidpointMap or not; it shouldn't be
	_, ok := d.hashMidpointMap[hash.Mini()]
	if ok {
		panic("Use re-write function instead")
	}
	// check if new node is a leaf
	leaf := node.left == node.right
	// check if this midpoint is already present in leafIndexMap or not; it shouldn't be
	_, ok = d.midpointIndexMap[midpointLeaf{node.midpoint, leaf}]
	if ok {
		if leaf {
			panic("Leaf already present")
		} else {
			panic("Use re-write function instead")
		}
	}
	// if there is an empty spot, add the midpoint there, else add it to the end of the file
	if len(d.emptyIndices) > 0 {
		index, d.emptyIndices = d.emptyIndices[0], d.emptyIndices[1:] //use the first index in the list of empty indices, remove it from list of removed
	} else {
		index = d.size()
		err := d.file.Truncate(int64((index + 1) * slotSize))
		if err != nil {
			panic(err)
		}
	}
	//do the actual writing to memory
	_, err := d.file.WriteAt(hash[:], int64(index*slotSize))
	if err != nil {
		panic(err)
	}
	_, err = d.file.WriteAt(node.left[:], int64(index*slotSize+32))
	if err != nil {
		panic(err)
	}
	_, err = d.file.WriteAt(node.right[:], int64(index*slotSize+64))
	if err != nil {
		panic(err)
	}
	midpointBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(midpointBytes, node.midpoint)
	_, err = d.file.WriteAt(midpointBytes[:], int64(index*slotSize+96))
	if err != nil {
		panic(err)
	}
	// Update the maps
	d.midpointIndexMap[midpointLeaf{node.midpoint, leaf}] = index
	d.hashMidpointMap[hash.Mini()] = midpointLeaf{node.midpoint, leaf}

}

// rewrite writes a PatriciaNode to memory where the midpoint pre-existed
func (d diskTreeNodes) rewrite(node patriciaNode, oldHash MiniHash) {
	hash := node.hash()
	// check that the node is a leaf or not; it shouldn't be
	leaf := node.left == node.right
	if leaf {
		panic("Leaf should never be rewritten")
	}

	// check if this midpoint is already present in midpointIndexMap or not; it should be
	index, ok := d.midpointIndexMap[midpointLeaf{node.midpoint, leaf}]
	if !ok {
		panic("Use write function instead")
	}
	// check if this hash is already present in hashMidpointMap or not; it shouldn't be
	_, ok = d.hashMidpointMap[hash.Mini()]
	if ok {
		panic("Why rewrite? Already present")
	}
	// check if oldHash is already present in hashMidpointMap or not; it should be
	_, ok = d.hashMidpointMap[oldHash]
	if !ok {
		panic("Incorrect old hash")
	}

	//do the actual writing to memory
	_, err := d.file.WriteAt(hash[:], int64(index*slotSize))
	if err != nil {
		panic(err)
	}
	_, err = d.file.WriteAt(node.left[:], int64(index*slotSize+32))
	if err != nil {
		panic(err)
	}
	_, err = d.file.WriteAt(node.right[:], int64(index*slotSize+64))
	if err != nil {
		panic(err)
	}
	// Update the map

	delete(d.hashMidpointMap, oldHash)
	d.hashMidpointMap[hash.Mini()] = midpointLeaf{node.midpoint, leaf}
}

// read reads a PatriciaNode from memory, given the hash
func (d diskTreeNodes) read(hash Hash) (patriciaNode, bool) {
	mpl, ok := d.hashMidpointMap[hash.Mini()]
	// not in hashMidpointMap so not present
	if !ok {
		panic("ReadError: Hash not present")
	}
	return d.readMidpoint(mpl)
}

// readMidpoint reads a PatriciaNode from memory, given the midpoint
func (d diskTreeNodes) readMidpoint(mpl midpointLeaf) (patriciaNode, bool) {
	var slotBytes [slotSize]byte
	var nodeHash, left, right Hash
	index, ok := d.midpointIndexMap[mpl]
	// not in midpointIndexMap so not present
	if !ok {
		panic("ReadError: Midpoint not present")
	}

	_, err := d.file.ReadAt(slotBytes[:], int64(index*slotSize))
	if err != nil {
		panic(err)
	}

	copy(nodeHash[:], slotBytes[0:32])
	copy(left[:], slotBytes[32:64])
	copy(right[:], slotBytes[64:96])
	midpoint := binary.LittleEndian.Uint64(slotBytes[96:104])
	if midpoint != mpl.midpoint {
		panic("Midpoints messed up")
	}

	if (left == right) != mpl.leaf {
		panic("Midpoints messed up")
	}

	node := patriciaNode{left, right, midpoint}
	if nodeHash != node.hash() {
		panic("Hash of node is wrong")
	}
	return node, true
}

// delete deletes a PatriciaNode from memory and adds the index to emptyIndices
func (d diskTreeNodes) delete(node patriciaNode) { // in principle, we only need node.hash().Mini() and node.midpoint
	hash := node.hash()
	leaf := node.left == node.right
	// check if this midpoint is already present in midpointIndexMap or not; it should be
	index, ok := d.midpointIndexMap[midpointLeaf{node.midpoint, leaf}]
	if !ok {
		panic("TBD midpoint not present")
	}
	// check if this hash is already present in hashMidpointMap or not; it should be
	mpl, ok := d.hashMidpointMap[hash.Mini()]
	if !ok {
		panic("TBD hash not present")
	}

	// check if the midpoints match
	if mpl.midpoint != node.midpoint {
		panic("midpoints mismatched!")
	}
	//edit the hashmaps
	delete(d.midpointIndexMap, mpl)
	delete(d.hashMidpointMap, hash.Mini())

	//add index to emptyIndices
	d.emptyIndices = append(d.emptyIndices, index)
}

// size gives you the size of the forest
func (d diskTreeNodes) size() uint64 {
	s, err := d.file.Stat()
	if err != nil {
		panic(err)
	}
	return uint64(s.Size() / slotSize)
}

func (d diskTreeNodes) close() {
	err := d.file.Close()
	if err != nil {
		fmt.Printf("diskTreeNodes close error: %s\n", err.Error())
	}
}
