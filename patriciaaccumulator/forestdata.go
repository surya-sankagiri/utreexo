package patriciaaccumulator

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"

	lru "github.com/hashicorp/golang-lru"

	// "github.com/peterbourgon/db"
	logrus "github.com/sirupsen/logrus"

	badger "github.com/dgraph-io/badger/v2"
)

// Size of a hash and a patricia node 32 + 2 * 32 + 8
// for new version, this is now just a node
const slotSize = 2*32 + 8

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
	diskSize() uint64
	// resize(newSize uint64) // make it have a new size (bigger)
	close()
}

// A treenodes with a ram cache. Every entry exists in either the disk or the ram, not both
type ramCacheTreeNodes struct {
	disk        superDiskTreeNodes
	ram         *lru.Cache
	maxRAMElems int
}

// newRamCacheTreeNodes returns a new treenodes container with a ram cache
func newRAMCacheTreeNodes(file *os.File, maxRAMElems int) ramCacheTreeNodes {
	cache, err := lru.New(maxRAMElems)
	if err != nil {
		panic("Error making cache")
	}
	return ramCacheTreeNodes{newSuperDiskTreeNodes(file), cache, maxRAMElems}
}

// read ignores errors. Probably get an empty hash if it doesn't work
func (d *ramCacheTreeNodes) read(hash Hash) (patriciaNode, bool) {

	logrus.Trace("Cache Level read ", hash[:6])

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

	logrus.Trace("Cache Level write ", hash[:6])

	inRAM := d.ram.Contains(hash)

	if inRAM {
		// d.ram[hash] = node
		panic("Trying to write something that already exists")
	}

	// _, ok := d.disk.read(hash)
	// if ok {
	// 	// Already in disk, done
	// 	panic("Trying to write something that already exists")
	// }

	// Not in ram or disk
	if d.ram.Len() < d.maxRAMElems {
		// Enough space, just write to ram
		logrus.Trace("Space in RAM, writing to RAM")
		d.ram.Add(hash, node)
	} else {
		logrus.Trace("No Space in RAM")
		logrus.Trace("move something in ram to disk")
		oldHash, oldNode, ok := d.ram.RemoveOldest()
		if !ok {
			panic("Should not be empty")
		}
		d.disk.write(oldHash.(Hash), oldNode.(patriciaNode))
		d.ram.Add(hash, node)

	}

}

func (d *ramCacheTreeNodes) delete(hash Hash) {

	logrus.Trace("Cache Level delete ", hash[:6])

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

// size gives you the disk size of the forest
func (d ramCacheTreeNodes) diskSize() uint64 {
	return d.disk.diskSize()
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

// ********************************************* forest on disk

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
	// copy(right[:], slotBytes[64:96])
	// FIXME
	prefixUint := binary.LittleEndian.Uint64(slotBytes[64:72])
	panic("")

	// Compile the node struct
	node := patriciaNode{left, right, prefixRange(prefixUint)}

	// d.isValid()

	if readHash != hash {
		fmt.Printf("\tindex is %d\n", index)
		fmt.Printf("\tinput hash is %x\n", hash)
		fmt.Printf("\tread hash is %x\n", readHash)
		fmt.Printf("\tcomputed hash is %x\n", node.hash())
		// fmt.Printf("\tnode midpoint is %d\n", node.midpoint)
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

	_, ok := d.read(hash)

	if ok {
		// Key already present,
		panic("This should not happen, we should never write twice if something is already there")
	}

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
	logrus.Debug("writing node")

	_, err := d.file.WriteAt(hash[:], int64(indexToWrite*slotSize))
	_, err = d.file.WriteAt(node.left[:], int64(indexToWrite*slotSize+32))
	_, err = d.file.WriteAt(node.right[:], int64(indexToWrite*slotSize+64))
	prefixBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefixBytes, uint64(node.prefix))
	_, err = d.file.WriteAt(prefixBytes[:], int64(indexToWrite*slotSize+96))

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
func (d *diskTreeNodes) size() uint64 {

	s := d.filePopulatedTo - uint64(len(d.emptySlots))

	if s != uint64(len(d.indexMap)) {
		panic(fmt.Sprint("Size not equal to indexmap size", s, uint64(len(d.indexMap))))
	}

	return s

}

func (d *diskTreeNodes) diskSize() uint64 {

	return d.size()

}

func (d *diskTreeNodes) close() {
	err := d.file.Close()
	if err != nil {
		fmt.Printf("diskTreeNodes close error: %s\n", err.Error())
	}
}

type dbTreeNodes struct {
	db    *badger.DB
	count uint64
}

func newdbTreeNodes() dbTreeNodes {

	db, err := badger.Open(badger.DefaultOptions("./utree/badger"))

	// db, err := db.New()
	if err != nil {
		log.Fatal(err)
	}

	// if err := db.Update(func(tx *bolt.Tx) error {
	// 	_, err := tx.CreateBucket([]byte("treeNodes"))
	// 	if err != nil {
	// 		return err
	// 	}
	// 	return nil
	// }); err != nil {
	// 	log.Fatal(err)
	// }

	return dbTreeNodes{db, 0}
}

func (d *dbTreeNodes) read(hash Hash) (patriciaNode, bool) {

	logrus.Trace("DB Reading ", hash[:6])
	// startSize := d.size()

	var slotBytes [72]byte
	retrieved := false
	var left, right Hash

	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(hash[:])
		if err != nil {
			// did not retireve
			return nil
		}
		retrieved = true

		var valCopy []byte
		err = item.Value(func(val []byte) error {
			// This func with val would only be called if item.Value encounters no error.

			// // Accessing val here is valid.
			// fmt.Printf("The answer is: %s\n", val)

			// Copying or parsing val is valid.
			valCopy = append([]byte{}, val...)

			// // Assigning val slice to another variable is NOT OK.
			// valNot = val // Do not do this.
			return nil
		})
		if err != nil {
			panic(err)
		}

		copy(slotBytes[:], valCopy)

		return nil
	})
	if err != nil {
		panic(err)
	}

	if !retrieved {
		return patriciaNode{}, false
	}

	// Read the slot into a struct
	copy(left[:], slotBytes[0:32])
	copy(right[:], slotBytes[32:64])
	prefix := binary.LittleEndian.Uint64(slotBytes[64:72])

	// Compile the node struct
	node := patriciaNode{left, right, prefixRange(prefix)}

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

	logrus.Trace("DB Writing ", hash[:6])

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
	binary.LittleEndian.PutUint64(midpointBytes, uint64(node.prefix))
	slot = append(slot, midpointBytes...)

	if len(slot) != 72 {
		panic("Slot wrong size")
	}

	err := d.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(hash[:], slot[:])
		return err
	})

	if err != nil {
		panic(err)
	}

	d.count++
}

func (d *dbTreeNodes) delete(hash Hash) {

	logrus.Trace("Deleting ", hash[:6])

	// d.foosize -= 1

	// fmt.Printf("\tCalling delete\n")

	// err := d.db.Erase(hex.Dump(hash[:]))

	// _, err := d.file.WriteAt(hash[:], int64(indexToWrite*slotSize))
	err := d.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(hash[:])
		return err
	})

	if err != nil {
		panic(err)
	}

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

// size gives you the size of the forest
func (d *dbTreeNodes) diskSize() uint64 {

	return uint64(d.count)
	// s, err := d.file.Stat()
	// if err != nil {
	// 	fmt.Printf("\tWARNING: %s. Returning 0", err.Error())
	// 	return 0
	// }
	// return uint64(s.Size() / slotSize)
}

func (d *dbTreeNodes) close() {
	err := d.db.Close()
	if err != nil {
		fmt.Printf("diskForestData close error: %s\n", err.Error())
	}
}

// A disk backed node store which makes a huge file
type superDiskTreeNodes struct {
	files []*os.File
	size_ uint64
}

// We make a file big enough to hold all our data
// There are at most 70 million UTXOs
// meaning there are 70 million leaves
// 70 million internal nodes
// and 70 million leaf location datas if we ever decide to fold that in
// thats 3 * 70 million
// we then double again to ensure the space is always half empty.
const superDiskFileEntries = 70000000 * 3 * 2
const superDiskTotalFileSize = superDiskFileEntries * slotSize
const superDiskFiles = 1000 // Make sure this divides superDiskFileEntries
const superDiskIndividualFileSize = superDiskTotalFileSize / superDiskFiles

// This is about 43 GB

// newsuperDiskTreeNodes makes a new file-backed tree nodes container
func newSuperDiskTreeNodes(file *os.File) superDiskTreeNodes {

	var files = make([]*os.File, 0)

	for i := 0; i < superDiskFiles; i++ {
		fmt.Println("Making huge file for tree entries: ", i)
		filename := filepath.Join(filepath.Join(".", "utree/forestdata"), fmt.Sprintf("forestfile%d.dat", i))
		file, err := os.OpenFile(
			filename, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			panic(err)
		}
		file.Truncate(superDiskIndividualFileSize)
		files = append(files, file)
		fmt.Println("Done making huge file for tree entries")
	}

	return superDiskTreeNodes{files, 0}
}

// We choose a location from the hash
func hashToIndex(hash Hash) uint64 {
	return binary.LittleEndian.Uint64(hash[:8]) % superDiskFileEntries
}

func (d *superDiskTreeNodes) fileSize() int64 {

	return superDiskTotalFileSize
}

// ********************************************* forest on disk

func (d *superDiskTreeNodes) read(hash Hash) (patriciaNode, bool) {

	// fmt.Printf("\tCalling read\n")

	// startSize := d.size()

	var slotBytes [slotSize]byte
	var left, right Hash

	idx := hashToIndex(hash)

	for i := 0; i < 256; i++ {
		index := (idx + uint64(i)) % superDiskFileEntries
		fileNo := index % superDiskFiles
		fileIdx := index / superDiskFiles
		_, err := d.files[fileNo].ReadAt(slotBytes[:], int64(fileIdx*slotSize))
		if err != nil {
			fmt.Printf("\tWARNING!! read %x pos %d %s\n", hash, idx, err.Error())
		}
		// Read the file
		// copy(readHash[:], slotBytes[0:32])

		copy(left[:], slotBytes[0:32])
		copy(right[:], slotBytes[32:64])
		prefixUint := binary.LittleEndian.Uint64(slotBytes[64:72])

		// Compile the node struct
		node := patriciaNode{left, right, prefixRange(prefixUint)}

		if left != empty && right == empty && prefixRange(prefixUint).isSingleton() && prefixUint != 0 {
			// we read a leaf entry
			if left != hash {
				continue
			}
			return node, true
		}

		if node.hash() != hash {
			continue
		}

		// d.isValid()

		// if readHash != hash {
		// 	fmt.Printf("\tindex is %d\n", i)
		// 	fmt.Printf("\tinput hash is %x\n", hash)
		// 	fmt.Printf("\tread hash is %x\n", readHash)
		// 	fmt.Printf("\tcomputed hash is %x\n", node.hash())
		// 	// fmt.Printf("\tnode midpoint is %d\n", node.midpoint)
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

	panic("Got through loop, didn't find read")

}

// write writes a key-value pair
func (d *superDiskTreeNodes) write(hash Hash, node patriciaNode) {

	logrus.Trace("\tCalling write\n")

	// _, ok := d.read(hash)

	// if ok {
	// 	// Key already present,
	// 	panic("This should not happen, we should never write twice if something is already there")
	// }

	// if node.hash() != hash {
	// 	panic("Input node with hash not matching the input hash")
	// }

	idx := hashToIndex(hash)

	var readHash Hash

	for i := 0; i < 256; i++ {
		index := (idx + uint64(i)) % superDiskFileEntries
		fileNo := index % superDiskFiles
		fileIdx := index / superDiskFiles

		// Read the file to see if its empty
		_, err := d.files[fileNo].ReadAt(readHash[:], int64(fileIdx*slotSize))
		if err != nil {
			fmt.Printf("\tWARNING!! read %x pos %d %s\n", hash, idx, err.Error())
		}
		if readHash != empty {
			continue
		}
		// Found an empty slot

		// _, err = d.files[fileNo].WriteAt(hash[:], int64(fileIdx*slotSize))
		_, err = d.files[fileNo].WriteAt(node.left[:], int64(fileIdx*slotSize))
		_, err = d.files[fileNo].WriteAt(node.right[:], int64(fileIdx*slotSize+32))
		prefixBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(prefixBytes, uint64(node.prefix))
		_, err = d.files[fileNo].WriteAt(prefixBytes[:], int64(fileIdx*slotSize+64))

		if err != nil {
			fmt.Printf("\tWARNING!! write pos %s\n", err.Error())
		}

		d.size_++

		return

	}
	panic("Did not find place to write")

}

func (d *superDiskTreeNodes) delete(hash Hash) {

	logrus.Trace("\tCalling delete\n")

	// _, ok := d.read(hash)

	// if ok {
	// 	// Key already present,
	// 	panic("This should not happen, we should never write twice if something is already there")
	// }

	// if node.hash() != hash {
	// 	panic("Input node with hash not matching the input hash")
	// }

	idx := hashToIndex(hash)

	var readSlot [slotSize]byte
	var left, right Hash
	var emptySlot [slotSize]byte

	for i := 0; i < 256; i++ {
		index := (idx + uint64(i)) % superDiskFileEntries
		fileNo := index % superDiskFiles
		fileIdx := index / superDiskFiles

		// Read the file to see if its empty
		_, err := d.files[fileNo].ReadAt(readSlot[:], int64(fileIdx*slotSize))
		if err != nil {
			fmt.Printf("\tWARNING!! read %x pos %d %s\n", hash, idx, err.Error())
		}

		copy(left[:], readSlot[0:32])
		copy(right[:], readSlot[32:64])
		prefixUint := binary.LittleEndian.Uint64(readSlot[64:72])

		// Compile the node struct
		node := patriciaNode{left, right, prefixRange(prefixUint)}

		if left == hash && right == empty {
			// Found the slot with thing to delete - it's a leaf location slot

			_, err = d.files[fileNo].WriteAt(emptySlot[:], int64(fileIdx*slotSize))

			if err != nil {
				fmt.Printf("\tWARNING!! write pos %s\n", err.Error())
			}

			d.size_--

			return
		}
		if node.hash() == hash {
			// Found the slot with thing to delete - it's a node

			_, err = d.files[fileNo].WriteAt(emptySlot[:], int64(fileIdx*slotSize))

			if err != nil {
				fmt.Printf("\tWARNING!! write pos %s\n", err.Error())
			}

			d.size_--

			return
		}

	}

	panic("Did not find thing to delete")

}

// size gives you the size of the forest
func (d *superDiskTreeNodes) size() uint64 {

	return d.size_

}

func (d *superDiskTreeNodes) diskSize() uint64 {

	return d.size()

}

func (d *superDiskTreeNodes) close() {

	for _, file := range d.files {
		err := file.Close()
		if err != nil {
			fmt.Printf("superDiskTreeNodes close error: %s\n", err.Error())
		}
	}

}
