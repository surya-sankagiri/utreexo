package patriciaaccumulator

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"

	lru "github.com/hashicorp/golang-lru"
	// "github.com/peterbourgon/db"
	logrus "github.com/sirupsen/logrus"

	"github.com/boltdb/bolt"
)

// Size of a hash and a patricia node 32 + 2 * 32 + 8
const slotSize = 104

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
func (d *ramCacheTreeNodes) read(hash Hash) (patriciaNode, bool) {

	val, ok := d.ram.Get(hash)
	if ok {
		// In memory
		return val.(patriciaNode), ok
	}
	// If not in memory, read disk
	logrus.Trace(fmt.Sprintln("If not in memory, read disk"))
	return d.disk.read(hash)

}

// write writes a key-value pair
func (d *ramCacheTreeNodes) write(hash Hash, node patriciaNode) {

	inRAM := d.ram.Contains(hash)

	if inRAM {
		// Already in ram, we are done
		// d.ram[hash] = node
		logrus.Trace("Trying to write something that already exists")
		return
	}

	_, ok := d.disk.read(hash)
	if ok {
		// Already in disk, done
		logrus.Trace("Trying to write something that already exists")
		return
	}

	// Not in ram or disk
	if d.ram.Len() < d.maxRAMElems {
		// Enough space, just write to ram
		d.ram.Add(hash, node)
	} else {
		// Not enough space, move something in ram to disk
		logrus.Trace("move something in ram to disk\n")
		oldHash, oldNode, ok := d.ram.RemoveOldest()
		if !ok {
			panic("Should not be empty")
		}
		d.disk.write(oldHash.(Hash), oldNode.(patriciaNode))
		d.ram.Add(hash, node)

	}

}

func (d *ramCacheTreeNodes) delete(hash Hash) {

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
	file            *os.File
	indexMap        map[MiniHash]uint64
	filePopulatedTo uint64   // Size of the prefix of the file where data might be
	emptySlots      []uint64 // Indices (before filePopulatedTo, which are empty)
}

// newDiskTreeNodes makes a new file-backed tree nodes container
func newDiskTreeNodes(file *os.File) diskTreeNodes {
	return diskTreeNodes{file, make(map[MiniHash]uint64), 0, make([]uint64, 0)}
}

func (d *diskTreeNodes) fileSize() int64 {
	s, err := d.file.Stat()

	if err != nil {
		panic("Error finding size of file")
	}

	return s.Size() / slotSize
}

// func (d diskTreeNodes) isValid() {
// 	fmt.Printf("\tCheckingisvalid\n")
// 	fmt.Printf("\tSize is %d, file size is %d\n", d.size(), d.fileSize())

// 	for i := 0; i < int(d.filePopulatedTo); i++ {
// 		var slotBytes [slotSize]byte
// 		var readHash, left, right Hash

// 		_, err := d.file.ReadAt(slotBytes[:], int64(i*slotSize))
// 		if err != nil {
// 			panic("f")
// 		}

// 		// Read the file
// 		copy(readHash[:], slotBytes[0:32])
// 		copy(left[:], slotBytes[32:64])
// 		copy(right[:], slotBytes[64:96])
// 		// midpoint := binary.LittleEndian.Uint64(slotBytes[96:104])

// 		// Compile the node struct
// 		// node := patriciaNode{left, right, midpoint}
// 		fmt.Printf("\tindex is %d\n", i)
// 		fmt.Printf("\tread hash is %x\n", readHash)

// 		f, ok := d.indexMap[readHash.Mini()]
// 		fmt.Printf("\tindex in indexmap is %d\n", f)
// 		if ok {
// 			if int(f) != i {
// 				panic("")
// 			}
// 		}
// 	}
// 	fmt.Printf("\t\n")
// }

// ********************************************* forest on disk
// type diskForestData struct {
// 	file *os.File
// }

func (d *diskTreeNodes) read(hash Hash) (patriciaNode, bool) {

	// fmt.Printf("\tCalling read\n")

	// startSize := d.size()

	var slotBytes [slotSize]byte
	var readHash, left, right Hash

	index, ok := d.indexMap[hash.Mini()]
	// not in index so not present
	if !ok {
		return patriciaNode{}, ok
	}

	_, err := d.file.ReadAt(slotBytes[:], int64(index*slotSize))
	if err != nil {
		fmt.Printf("\tWARNING!! read %x pos %d %s\n", hash, index, err.Error())
	}

	// Read the file
	copy(readHash[:], slotBytes[0:32])
	copy(left[:], slotBytes[32:64])
	copy(right[:], slotBytes[64:96])
	midpoint := binary.LittleEndian.Uint64(slotBytes[96:104])

	// Compile the node struct
	node := patriciaNode{left, right, midpoint}

	// d.isValid()

	if readHash != hash {
		fmt.Printf("\tindex is %d\n", index)
		fmt.Printf("\tinput hash is %x\n", hash)
		fmt.Printf("\tread hash is %x\n", readHash)
		fmt.Printf("\tcomputed hash is %x\n", node.hash())
		fmt.Printf("\tnode midpoint is %d\n", node.midpoint)
		panic("MiniHash Collision TODO do something different to secure this, this is just a kludge in place of a more sophisticated disk-backed key-value store")
	}

	if readHash != node.hash() {
		panic("Hash of node is wrong")
	}

	// endSize := d.size()

	// fmt.Printf("\t%d %d\n", startSize, endSize)

	// if endSize-startSize != 0 {
	// 	panic("")
	// }

	return node, true
}

// write writes a key-value pair
func (d *diskTreeNodes) write(hash Hash, node patriciaNode) {

	// startSize := d.size()

	// d.foosize += 1

	// fmt.Printf("\tCalling write\n")

	fileSize := d.fileSize()

	// If file is small, make it size 100
	if fileSize < 100 {
		err := d.file.Truncate(100 * slotSize)
		if err != nil {
			panic(err)
		}
	}

	fileSize = d.fileSize()

	// _, ok := d.read(hash)

	// if ok {
	// 	// Key already present, overwrite it
	// 	panic("This should not happen, the key is always the hash of the value")
	// 	// Futhermore, we should never write twice if something is already there
	// }

	if node.hash() != hash {
		panic("Input node with hash not matching the input hash")
	}
	// If there is no more room left in the file, double the size of the file
	if fileSize == int64(d.filePopulatedTo) && len(d.emptySlots) == 0 {
		logrus.Trace("Doubling file size\n")
		err := d.file.Truncate(2 * fileSize * slotSize)
		if err != nil {
			panic(err)
		}
	}
	var indexToWrite uint64
	// If there is an emptySlot, write to it
	if len(d.emptySlots) > 0 {
		logrus.Trace("Found an empty slot\n")
		indexToWrite = d.emptySlots[0]
		d.emptySlots = d.emptySlots[1:]
	} else {
		// Otherwise, write to a new slot
		logrus.Trace("Wrote to new slot\n")
		logrus.Debugf("filePopulatedTo: %d", d.filePopulatedTo)
		indexToWrite = d.filePopulatedTo
		d.filePopulatedTo++
	}
	logrus.Debugf("writing node with midpoint %d at index %d\n", node.midpoint, indexToWrite)

	_, err := d.file.WriteAt(hash[:], int64(indexToWrite*slotSize))
	_, err = d.file.WriteAt(node.left[:], int64(indexToWrite*slotSize+32))
	_, err = d.file.WriteAt(node.right[:], int64(indexToWrite*slotSize+64))
	midpointBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(midpointBytes, node.midpoint)
	_, err = d.file.WriteAt(midpointBytes[:], int64(indexToWrite*slotSize+96))

	// Update the index
	d.indexMap[hash.Mini()] = indexToWrite

	if err != nil {
		fmt.Printf("\tWARNING!! write pos %s\n", err.Error())
	}

	// fmt.Println("empty slots", len(d.emptySlots))

	// endSize := d.size()

	// fmt.Printf("\t%d %d foo %d \n", startSize, endSize, d.foosize)

	// if endSize-startSize != 1 {
	// 	panic("")
	// }
	// fmt.Println(d.filePopulatedTo)

	// d.isValid()

}

func (d *diskTreeNodes) delete(hash Hash) {

	// d.foosize -= 1

	// fmt.Printf("\tCalling delete\n")

	index, ok := d.indexMap[hash.Mini()]
	// not in index so not present
	if !ok {
		panic("Deleting thing that does not exist")
	}

	d.emptySlots = append(d.emptySlots, index)

	// copy the last element of the file

	// var endhash, left, right Hash
	// var midpointBytes [8]byte
	// _, err = d.file.ReadAt(endhash[:], int64((elems-1)*slotSize))
	// _, err = d.file.ReadAt(left[:], int64((elems-1)*slotSize+32))
	// _, err = d.file.ReadAt(right[:], int64((elems-1)*slotSize+64))
	// _, err = d.file.ReadAt(midpointBytes[:], int64((elems-1)*slotSize+96))

	// // write to the overwritten slot

	// _, err = d.file.WriteAt(endhash[:], int64(index*slotSize))
	// _, err = d.file.WriteAt(left[:], int64(index*slotSize+32))
	// _, err = d.file.WriteAt(right[:], int64(index*slotSize+64))
	// _, err = d.file.WriteAt(midpointBytes[:], int64(index*slotSize+96))

	// Update the index
	// d.indexMap[endhash.Mini()] = index

	delete(d.indexMap, hash.Mini())

	// err = d.file.Truncate(int64((elems - 1) * slotSize))

	// if err != nil {
	// 	fmt.Printf("\tWARNING!! write pos %s\n", err.Error())
	// }

	// d.isValid()

}

// size gives you the size of the forest
func (d diskTreeNodes) size() uint64 {

	return d.filePopulatedTo - uint64(len(d.emptySlots))
	// s, err := d.file.Stat()
	// if err != nil {
	// 	fmt.Printf("\tWARNING: %s. Returning 0", err.Error())
	// 	return 0
	// }
	// return uint64(s.Size() / slotSize)
}

func (d diskTreeNodes) close() {
	err := d.file.Close()
	if err != nil {
		fmt.Printf("diskForestData close error: %s\n", err.Error())
	}
}

type dbTreeNodes struct {
	db    *bolt.DB
	count uint64
}

func newdbTreeNodes() dbTreeNodes {

	// // Simplest transform function: put all the data files into the base dir.
	// flatTransform := func(s string) []string { return []string{} }

	db, err := bolt.Open("./utree/db", 0600, nil)

	// db, err := db.New()
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("treeNodes"))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	return dbTreeNodes{db, 0}
}

func (d *dbTreeNodes) read(hash Hash) (patriciaNode, bool) {

	// fmt.Printf("\tCalling read\n")

	// startSize := d.size()

	var slotBytes [72]byte
	var left, right Hash

	if err := d.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket([]byte("treeNodes")).Get(hash[:])
		copy(slotBytes[:], value[:])
		// fmt.Printf("The value of 'foo' is: %s\n", value)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// if err != nil {
	// 	fmt.Printf("\tWARNING!! read %x %s\n", hash, err.Error())
	// }

	// Read the slot into a struct
	copy(left[:], slotBytes[0:32])
	copy(right[:], slotBytes[32:64])
	midpoint := binary.LittleEndian.Uint64(slotBytes[64:72])

	// Compile the node struct
	node := patriciaNode{left, right, midpoint}

	// d.isValid()

	// if readHash != hash {
	// 	fmt.Printf("\tinput hash is %x\n", hash)
	// 	fmt.Printf("\tread hash is %x\n", readHash)
	// 	fmt.Printf("\tcomputed hash is %x\n", node.hash())
	// 	panic("MiniHash Collision TODO do something different to secure this, this is just a kludge in place of a more sophisticated disk-backed key-value store")
	// }

	// if readHash != node.hash() {
	// 	panic("Hash of node is wrong")
	// }

	// endSize := d.size()

	// fmt.Printf("\t%d %d\n", startSize, endSize)

	// if endSize-startSize != 0 {
	// 	panic("")
	// }

	return node, true
}

// write writes a key-value pair
func (d *dbTreeNodes) write(hash Hash, node patriciaNode) {

	// fmt.Printf("\tCalling write\n")

	// _, ok := d.read(hash)

	// if ok {
	// 	// Key already present, overwrite it
	// 	panic("This should not happen, the key is always the hash of the value")
	// 	// Futhermore, we should never write twice if something is already there
	// }

	if node.hash() != hash {
		panic("Input node with hash not matching the input hash")
	}

	slot := append(node.left[:], node.right[:]...)
	midpointBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(midpointBytes, node.midpoint)
	slot = append(slot, midpointBytes...)

	if len(slot) != 72 {
		panic("Slot wrong size")
	}

	// _, err := d.file.WriteAt(hash[:], int64(indexToWrite*slotSize))
	if err := d.db.Update(func(tx *bolt.Tx) error {
		tx.Bucket([]byte("treeNodes")).Put(hash[:], slot)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// err := b.Put(hash[:], slot)

	// err := d.db.Write(hex.Dump(hash[:]), slot)

	// if err != nil {
	// 	fmt.Printf("\tWARNING!! write pos %s\n", err.Error())
	// 	panic(err.Error())
	// }

	d.count++
}

func (d *dbTreeNodes) delete(hash Hash) {

	// d.foosize -= 1

	// fmt.Printf("\tCalling delete\n")

	// err := d.db.Erase(hex.Dump(hash[:]))

	// _, err := d.file.WriteAt(hash[:], int64(indexToWrite*slotSize))
	if err := d.db.Update(func(tx *bolt.Tx) error {
		tx.Bucket([]byte("treeNodes")).Delete(hash[:])
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// // not in index so not present
	// if err != nil {
	// 	panic("Deleting thing that does not exist")
	// }

	d.count--

}

// size gives you the size of the forest
func (d *dbTreeNodes) size() uint64 {

	return uint64(d.count)
	// s, err := d.file.Stat()
	// if err != nil {
	// 	fmt.Printf("\tWARNING: %s. Returning 0", err.Error())
	// 	return 0
	// }
	// return uint64(s.Size() / slotSize)
}

func (d *dbTreeNodes) close() {
	// err := d.db.Close()
	// if err != nil {
	// 	fmt.Printf("diskForestData close error: %s\n", err.Error())
	// }
}
