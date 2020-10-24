package patriciaaccumulator

import (
	"encoding/binary"
	"fmt"
	"os"

	lru "github.com/hashicorp/golang-lru"
)

// Size of a hash and a patricia node 32 + 2 * 32 + 8
const slotSize = 104

// A treenodes with a ram cache. Every entry exists in either the disk or the ram, not both
type ramCacheTreeNodes struct {
	disk        diskTreeNodes
	ram         *lru.Cache
	maxRAMElems int
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
func (d ramCacheTreeNodes) write(hash Hash, node patriciaNode) {

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
		d.disk.write(oldHash.(Hash), oldNode.(patriciaNode))
		d.ram.Add(hash, node)

	}
}

func (d ramCacheTreeNodes) delete(hash Hash) {

	// Delete from ram
	present := d.ram.Remove(hash)

	if !present {
		// Delete from disk
		d.disk.delete(hash)
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
type diskTreeNodes struct {
	file     *os.File
	indexMap map[MiniHash]uint64
}

// ForestData is the thing that holds all the hashes in the forest.  Could
// be in a file, or in ram, or maybe something else.
type ForestData interface {
	read(hash Hash) (patriciaNode, bool)
	// TODO we only ever write a node to its own hash, so get rid of first argument
	write(hash Hash, node patriciaNode)
	delete(hash Hash)
	// swapHash(a, b uint64)
	// swapHashRange(a, b, w uint64)
	size() uint64
	// resize(newSize uint64) // make it have a new size (bigger)
	close()
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
	_, err = d.file.ReadAt(endhash[:], int64((elems-1)*slotSize))
	_, err = d.file.ReadAt(left[:], int64((elems-1)*slotSize+32))
	_, err = d.file.ReadAt(right[:], int64((elems-1)*slotSize+64))
	_, err = d.file.ReadAt(midpointBytes[:], int64((elems-1)*slotSize+96))

	// write to the overwritten slot

	_, err = d.file.WriteAt(endhash[:], int64(index*slotSize))
	_, err = d.file.WriteAt(left[:], int64(index*slotSize+32))
	_, err = d.file.WriteAt(right[:], int64(index*slotSize+64))
	_, err = d.file.WriteAt(midpointBytes[:], int64(index*slotSize+96))

	// Update the index
	d.indexMap[endhash.Mini()] = index

	delete(d.indexMap, hash.Mini())

	err = d.file.Truncate(int64((elems - 1) * slotSize))

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
