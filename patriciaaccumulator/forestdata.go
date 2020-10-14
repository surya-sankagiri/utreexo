package patriciaaccumulator

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Size of a hash and a patricia node 32 + 2 * 32 + 8
const slotSize = 104

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
func (d *diskTreeNodes) read(hash Hash) (patriciaNode, bool) {
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
func (d *diskTreeNodes) write(hash Hash, node patriciaNode) {
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

func (d *diskTreeNodes) delete(hash Hash) {
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

// // swapHash swaps 2 hashes.  Don't go out of bounds.
// func (d *diskForestData) swapHash(a, b uint64) {
// 	ha := d.read(a)
// 	hb := d.read(b)
// 	d.write(a, hb)
// 	d.write(b, ha)
// }

// // swapHashRange swaps 2 continuous ranges of hashes.  Don't go out of bounds.
// // uses lots of ram to make only 3 disk seeks (depending on how you count? 4?)
// // seek to a start, read a, seek to b start, read b, write b, seek to a, write a
// // depends if you count seeking from b-end to b-start as a seek. or if you have
// // like read & replace as one operation or something.
// func (d *diskForestData) swapHashRange(a, b, w uint64) {
// 	arange := make([]byte, leafSize*w)
// 	brange := make([]byte, leafSize*w)
// 	_, err := d.file.ReadAt(arange, int64(a*leafSize)) // read at a
// 	if err != nil {
// 		fmt.Printf("\tshr WARNING!! read pos %d len %d %s\n",
// 			a*leafSize, w, err.Error())
// 	}
// 	_, err = d.file.ReadAt(brange, int64(b*leafSize)) // read at b
// 	if err != nil {
// 		fmt.Printf("\tshr WARNING!! read pos %d len %d %s\n",
// 			b*leafSize, w, err.Error())
// 	}
// 	_, err = d.file.WriteAt(arange, int64(b*leafSize)) // write arange to b
// 	if err != nil {
// 		fmt.Printf("\tshr WARNING!! write pos %d len %d %s\n",
// 			b*leafSize, w, err.Error())
// 	}
// 	_, err = d.file.WriteAt(brange, int64(a*leafSize)) // write brange to a
// 	if err != nil {
// 		fmt.Printf("\tshr WARNING!! write pos %d len %d %s\n",
// 			a*leafSize, w, err.Error())
// 	}
// }

// size gives you the size of the forest
func (d *diskTreeNodes) size() uint64 {
	s, err := d.file.Stat()
	if err != nil {
		fmt.Printf("\tWARNING: %s. Returning 0", err.Error())
		return 0
	}
	return uint64(s.Size() / slotSize)
}

// // resize makes the forest bigger (never gets smaller so don't try)
// func (d *diskForestData) resize(newSize uint64) {
// 	err := d.file.Truncate(int64(newSize * leafSize * 2))
// 	if err != nil {
// 		panic(err)
// 	}
// }

func (d *diskTreeNodes) close() {
	err := d.file.Close()
	if err != nil {
		fmt.Printf("diskForestData close error: %s\n", err.Error())
	}
}
