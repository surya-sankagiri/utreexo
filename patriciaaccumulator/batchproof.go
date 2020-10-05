package patriciaaccumulator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

// OLD CODE:
// // BatchProof :
// type BatchProof struct {
// 	Targets []uint64
// 	Proof   []Hash
// 	// list of leaf locations to delete, along with a bunch of hashes that give the proof.
// 	// the position of the hashes is implied / computable from the leaf positions
// }

// TODO it is actually possible to avoid including a prefix in every node of a proof and instead only hash in the prefix lengths
// This makesthe system more space-efficient. See https://ethresear.ch/t/binary-trie-format/7621/6

// BatchProof consists of a proof for a batch of leaves
type BatchProof struct {
	Targets        []uint64
	hashes         []Hash  // List of all hashes in the proof (that is, hashes of siblings of ancestors of deleted elements) (should they be in DFS order?)
	midpointsWidth []uint8 // List of widths of midpoints of nodes that are ancestors of deleted elements
}

// LongBatchProof is a similar to BatchProof, but with midpoints explicit. in the PatriciaAccumulator Implementation - Bolton
type LongBatchProof struct {
	Targets   []uint64
	hashes    []Hash   // List of all hashes in the proof (that is, hashes of siblings of ancestors of deleted elements) (should they be in DFS order?)
	midpoints []uint64 // List of equal midpoints of nodes that are ancestors of deleted elements
	// Checking a proof requires all midpoints in the branch to the element, and all hashes of siblings
	// We therefore have the midpoint tree. Our first step in proof checking is constructing this tree
	// We then have to fill in the hashes of the children we proceed in order left to right
	// 1. when a hash is a leaf, we have that hash
	// 2. when we don't have a leaf, we take the next element of hashes, and we order hashes so that this element is the correct next one.
	// TODO in the more efficient version, this is a slice of uint8s representing the branching off points.
}

// PatriciaProof is a potential replacement structure for a single proof
type PatriciaProof struct {
	target    uint64
	hashes    []Hash   // List of all hashes in the proof (that is, hashes of siblings of ancestors of deleted elements) (should they be in DFS order?)
	midpoints []uint64 // List of equal midpoints of nodes that are ancestors of deleted elements
	// Checking a proof requires all midpoints in the branch to the element, and all hashes of siblings
	// We therefore have the midpoint tree. Our first step in proof checking is constructing this tree
	// We then have to fill in the hashes of the children we proceed in order left to right
	// 1. when a hash is a leaf, we have that hash
	// 2. when we don't have a leaf, we take the next element of hashes, and we order hashes so that this element is the correct next one.
}

func (bp BatchProof) SortTargets() {
	// Dummy function
}

func (bp BatchProof) Reconstruct(nl uint64, h uint8) (map[uint64]Hash, error) {
	// Dummy function
	return make(map[uint64]Hash), nil
}

// MergeProofs takes as inputs a slice of PatriciaProofs and gives as output a single BatchProof-Surya
// TODO: Debug this function
// TODO: make the running time of the code more efficient, if possible -Surya
// TODO: make the BatchProof struct more efficient by removing repeated entries -Surya
// func MergeProofs(indProofs []PatriciaProof) BatchProof {
// 	var batchProof BatchProof
// 	Targets := []uint64{}
// 	for _, proof := range indProofs {
// 		Targets = append(Targets, proof.target)
// 	}
// 	sortedIndices := ArgsortNew(Targets)
// 	for _, i := range sortedIndices {
// 		batchProof.Targets = append(batchProof.Targets, indProofs[i].target)
// 		batchProof.midpoints = append(batchProof.midpoints, indProofs[i].midpoints...)
// 		batchProof.hashes = append(batchProof.hashes, indProofs[i].hashes...)
// 	}
// 	return batchProof
// }

// Code for implementing ArgSort, lifted from the internet--Surya
// --------------------------------------------------------
// argsort, like in Numpy, it returns an array of indexes into an array. Note
// that the gonum version of argsort reorders the original array and returns
// indexes to reconstruct the original order.
type argsort struct {
	s    []uint64 // Points to orignal array but does NOT alter it.
	inds []int    // Indexes to be returned.
}

func (a argsort) Len() int {
	return len(a.s)
}

func (a argsort) Less(i, j int) bool {
	return a.s[a.inds[i]] < a.s[a.inds[j]]
}

func (a argsort) Swap(i, j int) {
	a.inds[i], a.inds[j] = a.inds[j], a.inds[i]
}

// ArgsortNew allocates and returns an array of indexes into the source float
// array.
func ArgsortNew(src []uint64) []int {
	inds := make([]int, len(src))
	for i := range src {
		inds[i] = i
	}
	Argsort(src, inds)
	return inds
}

// Argsort alters a caller-allocated array of indexes into the source float
// array. The indexes must already have values 0...n-1.
func Argsort(src []uint64, inds []int) {
	if len(src) != len(inds) {
		panic("floats: length of inds does not match length of slice")
	}
	a := argsort{s: src, inds: inds}
	sort.Sort(a)
}

// -------------------------------------

// there seems to be two syntaxes for write; need to figure that out-Surya
// The trick is to write both the number of Targets and the number of midpoints

// ToBytes serializes a BatchProof
func (bp *BatchProof) ToBytes() []byte {
	var buf bytes.Buffer

	// first write the number of Targets (4 byte uint32)
	numTargets := uint32(len(bp.Targets))
	if numTargets == 0 {
		// TODO this should instead return some representation of an "empty proof"

		return nil
	}
	err := binary.Write(&buf, binary.BigEndian, numTargets)
	if err != nil {
		panic("error in converting batchproof to bytes.")
	}
	// then write the number of midpoints (4 byte uint32)
	numMidpoints := uint32(len(bp.midpointsWidth))
	if numMidpoints == 0 {
		// TODO its actually possible, i think if we have a single element tree, then there is one target and nothing else
		// panic("non-zero Targets but no midpoints.")
	}
	err = binary.Write(&buf, binary.BigEndian, numMidpoints)
	if err != nil {
		panic("error in converting batchproof to bytes.")
	}
	// next, actually write the Targets
	for _, t := range bp.Targets {
		err := binary.Write(&buf, binary.BigEndian, t)
		if err != nil {
			panic("error in converting batchproof to bytes.")
		}
	}
	// this is followed by the midpoints
	for _, m := range bp.midpointsWidth {
		err := binary.Write(&buf, binary.BigEndian, m)
		if err != nil {
			panic("error in converting batchproof to bytes.")
		}
	}

	// then the rest is just hashes
	for _, h := range bp.hashes {
		_, err = buf.Write(h[:])
		if err != nil {
			panic("error in converting batchproof to bytes.")
		}
	}
	return buf.Bytes()
}

// // ToBytes give the bytes for a BatchProof.  It errors out silently because
// // I don't think the binary.Write errors ever actually happen

// func (bp *BatchProof) ToBytes() []byte {
// 	var buf bytes.Buffer

// 	// first write the number of Targets (4 byte uint32)
// 	numTargets := uint32(len(bp.Targets))
// 	if numTargets == 0 {
// 		return nil
// 	}
// 	err := binary.Write(&buf, binary.BigEndian, numTargets)
// 	if err != nil {
// 		fmt.Printf("huh %s\n", err.Error())
// 		return nil
// 	}
// 	for _, t := range bp.Targets {
// 		// there's no need for these to be 64 bit for the next few decades...
// 		err := binary.Write(&buf, binary.BigEndian, t)
// 		if err != nil {
// 			fmt.Printf("huh %s\n", err.Error())
// 			return nil
// 		}
// 	}
// 	// then the rest is just hashes
// 	for _, h := range bp.Proof {
// 		_, err = buf.Write(h[:])
// 		if err != nil {
// 			fmt.Printf("huh %s\n", err.Error())
// 			return nil
// 		}
// 	}

// 	return buf.Bytes()
// }

// // TODO: understand the two functions below and if necessary, write the same for BatchProof
// // ToString for debugging, shows the blockproof
// func (bp *BatchProof) SortTargets() {
// 	sortUint64s(bp.Targets)
// }

// // ToString for debugging, shows the blockproof
// func (bp *BatchProof) ToString() string {
// 	s := fmt.Sprintf("%d Targets: ", len(bp.Targets))
// 	for _, t := range bp.Targets {
// 		s += fmt.Sprintf("%d\t", t)
// 	}
// 	s += fmt.Sprintf("\n%d proofs: ", len(bp.Proof))
// 	for _, p := range bp.Proof {
// 		s += fmt.Sprintf("%04x\t", p[:4])
// 	}
// 	s += "\n"
// 	return s
// }

func FromBytesBatchProof(b []byte) (BatchProof, error) {
	var bp BatchProof

	// if empty slice, return empty BatchProof with 0 Targets
	if len(b) == 0 {
		return bp, nil
	}
	// otherwise, if there are less than 8 bytes we can't even see the number
	// of Targets and number of midpoints so something is wrong
	if len(b) < 8 {
		return bp, fmt.Errorf("batchproof only %d bytes", len(b))
	}

	buf := bytes.NewBuffer(b)
	// read 4 byte number of Targets
	var numTargets uint32
	err := binary.Read(buf, binary.BigEndian, &numTargets)
	if err != nil {
		return bp, err
	}

	// read 4 byte number of midpoints
	var numMidpoints uint32
	err = binary.Read(buf, binary.BigEndian, &numMidpoints)
	if err != nil {
		return bp, err
	}
	//read the Targets
	bp.Targets = make([]uint64, numTargets)
	for i := range bp.Targets {
		err := binary.Read(buf, binary.BigEndian, &bp.Targets[i])
		if err != nil {
			return bp, err
		}
	}
	//read the midpoints
	bp.midpointsWidth = make([]uint8, numMidpoints)
	for i := range bp.midpointsWidth {
		err := binary.Read(buf, binary.BigEndian, &bp.midpointsWidth[i])
		if err != nil {
			return bp, err
		}
	}

	remaining := buf.Len()
	// the rest is hashes
	if remaining%32 != 0 {
		return bp, fmt.Errorf("%d bytes left, should be n*32", buf.Len())
	}
	bp.hashes = make([]Hash, remaining/32)

	for i := range bp.hashes {
		copy(bp.hashes[i][:], buf.Next(32))
	}
	return bp, nil
}

// // FromBytesBatchProof gives a block proof back from the serialized bytes
// func FromBytesBatchProof(b []byte) (BatchProof, error) {
// 	var bp BatchProof

// 	// if empty slice, return empty BatchProof with 0 Targets
// 	if len(b) == 0 {
// 		return bp, nil
// 	}
// 	// otherwise, if there are less than 4 bytes we can't even see the number
// 	// of Targets so something is wrong
// 	if len(b) < 4 {
// 		return bp, fmt.Errorf("batchproof only %d bytes", len(b))
// 	}

// 	buf := bytes.NewBuffer(b)
// 	// read 4 byte number of Targets
// 	var numTargets uint32
// 	err := binary.Read(buf, binary.BigEndian, &numTargets)
// 	if err != nil {
// 		return bp, err
// 	}
// 	bp.Targets = make([]uint64, numTargets)
// 	for i, _ := range bp.Targets {
// 		err := binary.Read(buf, binary.BigEndian, &bp.Targets[i])
// 		if err != nil {
// 			return bp, err
// 		}
// 	}
// 	remaining := buf.Len()
// 	// the rest is hashes
// 	if remaining%32 != 0 {
// 		return bp, fmt.Errorf("%d bytes left, should be n*32", buf.Len())
// 	}
// 	bp.Proof = make([]Hash, remaining/32)

// 	for i, _ := range bp.Proof {
// 		copy(bp.Proof[i][:], buf.Next(32))
// 	}
// 	return bp, nil
// }

// TODO OH WAIT -- this is not how to to it!  Don't hash all the way up to the
// roots to verify -- just hash up to any populated node!  Saves a ton of CPU!

// // verifyBatchProof takes a block proof and reconstructs / verifies it.
// // takes a blockproof to verify, and the known correct roots to check against.
// // also takes the number of leaves and forest rows (those are redundant
// // if we don't do weird stuff with overly-high forests, which we might)
// // it returns a bool of whether the proof worked, and a map of the sparse
// // forest in the blockproof
// func verifyBatchProof(
// 	bp BatchProof, roots []Hash,
// 	numLeaves uint64, rows uint8) (bool, map[uint64]Hash) {

// 	// if nothing to prove, it worked
// 	if len(bp.Targets) == 0 {
// 		return true, nil
// 	}

// 	// Construct a map with positions to hashes
// 	proofmap, err := bp.Reconstruct(numLeaves, rows)
// 	if err != nil {
// 		fmt.Printf("VerifyBlockProof Reconstruct ERROR %s\n", err.Error())
// 		return false, proofmap
// 	}

// 	rootPositions, rootRows := getRootsReverse(numLeaves, rows)

// 	// partial forest is built, go through and hash everything to make sure
// 	// you get the right roots

// 	tagRow := bp.Targets
// 	nextRow := []uint64{}
// 	sortUint64s(tagRow) // probably don't need to sort

// 	// TODO it's ugly that I keep treating the 0-row as a special case,
// 	// and has led to a number of bugs.  It *is* special in a way, in that
// 	// the bottom row is the only thing you actually prove and add/delete,
// 	// but it'd be nice if it could all be treated uniformly.

// 	if verbose {
// 		fmt.Printf("tagrow len %d\n", len(tagRow))
// 	}

// 	var left, right uint64

// 	// iterate through rows
// 	for row := uint8(0); row <= rows; row++ {
// 		// iterate through tagged positions in this row
// 		for len(tagRow) > 0 {
// 			// Efficiency gains here. If there are two or more things to verify,
// 			// check if the next thing to verify is the sibling of the current leaf
// 			// we're on. Siblingness can be checked with bitwise XOR but since Targets are
// 			// sorted, we can do bitwise OR instead.
// 			if len(tagRow) > 1 && tagRow[0]|1 == tagRow[1] {
// 				left = tagRow[0]
// 				right = tagRow[1]
// 				tagRow = tagRow[2:]
// 			} else { // if not only use one tagged position
// 				right = tagRow[0] | 1
// 				left = right ^ 1
// 				tagRow = tagRow[1:]
// 			}

// 			if verbose {
// 				fmt.Printf("left %d rootPoss %d\n", left, rootPositions[0])
// 			}
// 			// check for roots
// 			if left == rootPositions[0] {
// 				if verbose {
// 					fmt.Printf("one left in tagrow; should be root\n")
// 				}
// 				// Grab the hash of this position from the map
// 				computedRootHash, ok := proofmap[left]
// 				if !ok {
// 					fmt.Printf("ERR no proofmap for root at %d\n", left)
// 					return false, nil
// 				}
// 				// Verify that this root hash matches the one we stored
// 				if computedRootHash != roots[0] {
// 					fmt.Printf("row %d root, pos %d expect %04x got %04x\n",
// 						row, left, roots[0][:4], computedRootHash[:4])
// 					return false, nil
// 				}
// 				// otherwise OK and pop of the root
// 				roots = roots[1:]
// 				rootPositions = rootPositions[1:]
// 				rootRows = rootRows[1:]
// 				break
// 			}

// 			// Grab the parent position of the leaf we've verified
// 			parentPos := parent(left, rows)
// 			if verbose {
// 				fmt.Printf("%d %04x %d %04x -> %d\n",
// 					left, proofmap[left], right, proofmap[right], parentPos)
// 			}

// 			// this will crash if either is 0000
// 			// reconstruct the next row and add the parent to the map
// 			parhash := parentHash(proofmap[left], proofmap[right])
// 			nextRow = append(nextRow, parentPos)
// 			proofmap[parentPos] = parhash
// 		}

// 		// Make the nextRow the tagRow so we'll be iterating over it
// 		// reset th nextRow
// 		tagRow = nextRow
// 		nextRow = []uint64{}

// 		// if done with row and there's a root left on this row, remove it
// 		if len(rootRows) > 0 && rootRows[0] == row {
// 			// bit ugly to do these all separately eh
// 			roots = roots[1:]
// 			rootPositions = rootPositions[1:]
// 			rootRows = rootRows[1:]
// 		}
// 	}

// 	return true, proofmap
// }

// // Reconstruct takes a number of leaves and rows, and turns a block proof back
// // into a partial proof tree. Should leave bp intact
// func (bp *BatchProof) Reconstruct(
// 	numleaves uint64, forestRows uint8) (map[uint64]Hash, error) {

// 	if verbose {
// 		fmt.Printf("reconstruct blockproof %d tgts %d hashes nl %d fr %d\n",
// 			len(bp.Targets), len(bp.Proof), numleaves, forestRows)
// 	}
// 	proofTree := make(map[uint64]Hash)

// 	// If there is nothing to reconstruct, return empty map
// 	if len(bp.Targets) == 0 {
// 		return proofTree, nil
// 	}
// 	proof := bp.Proof // back up proof
// 	Targets := bp.Targets
// 	rootPositions, rootRows := getRootsReverse(numleaves, forestRows)

// 	if verbose {
// 		fmt.Printf("%d roots:\t", len(rootPositions))
// 		for _, t := range rootPositions {
// 			fmt.Printf("%d ", t)
// 		}
// 	}
// 	// needRow / nextrow hold the positions of the data which should be in the blockproof
// 	var needSibRow, nextRow []uint64 // only even siblings needed

// 	// a bit strange; pop off 2 hashes at a time, and either 1 or 2 positions
// 	for len(bp.Proof) > 0 && len(Targets) > 0 {

// 		if Targets[0] == rootPositions[0] {
// 			// target is a root; this can only happen at row 0;
// 			// there's a "proof" but don't need to actually send it
// 			if verbose {
// 				fmt.Printf("placed single proof at %d\n", Targets[0])
// 			}
// 			proofTree[Targets[0]] = bp.Proof[0]
// 			bp.Proof = bp.Proof[1:]
// 			Targets = Targets[1:]
// 			continue
// 		}

// 		// there should be 2 proofs left then
// 		if len(bp.Proof) < 2 {
// 			return nil, fmt.Errorf("only 1 proof left but need 2 for %d",
// 				Targets[0])
// 		}

// 		// populate first 2 proof hashes
// 		right := Targets[0] | 1
// 		left := right ^ 1

// 		proofTree[left] = bp.Proof[0]
// 		proofTree[right] = bp.Proof[1]
// 		needSibRow = append(needSibRow, parent(Targets[0], forestRows))
// 		// pop em off
// 		if verbose {
// 			fmt.Printf("placed proofs at %d, %d\n", left, right)
// 		}
// 		bp.Proof = bp.Proof[2:]

// 		if len(Targets) > 1 && Targets[0]|1 == Targets[1] {
// 			// pop off 2 positions
// 			Targets = Targets[2:]
// 		} else {
// 			// only pop off 1
// 			Targets = Targets[1:]
// 		}
// 	}

// 	// deal with 0-row root, regardless of whether it was used or not
// 	if rootRows[0] == 0 {
// 		rootPositions = rootPositions[1:]
// 		rootRows = rootRows[1:]
// 	}

// 	// now all that's left is the proofs. go bottom to root and iterate the haveRow
// 	for h := uint8(1); h < forestRows; h++ {
// 		for len(needSibRow) > 0 {
// 			// if this is a root, it's not needed or given
// 			if needSibRow[0] == rootPositions[0] {
// 				needSibRow = needSibRow[1:]
// 				rootPositions = rootPositions[1:]
// 				rootRows = rootRows[1:]
// 				continue
// 			}
// 			// either way, we'll get the parent
// 			nextRow = append(nextRow, parent(needSibRow[0], forestRows))

// 			// if we have both siblings here, don't need any proof
// 			if len(needSibRow) > 1 && needSibRow[0]^1 == needSibRow[1] {
// 				needSibRow = needSibRow[2:]
// 			} else {
// 				// return error if we need a proof and can't get it
// 				if len(bp.Proof) == 0 {
// 					fmt.Printf("roots %v needsibrow %v\n", rootPositions, needSibRow)
// 					return nil, fmt.Errorf("h %d no proofs left at pos %d ",
// 						h, needSibRow[0]^1)
// 				}
// 				// otherwise we do need proof; place in sibling position and pop off
// 				proofTree[needSibRow[0]^1] = bp.Proof[0]
// 				bp.Proof = bp.Proof[1:]
// 				// and get rid of 1 element of needSibRow
// 				needSibRow = needSibRow[1:]
// 			}
// 		}

// 		// there could be a root on this row that we don't need / use; if so pop it
// 		if len(rootRows) > 0 && rootRows[0] == h {
// 			rootPositions = rootPositions[1:]
// 			rootRows = rootRows[1:]
// 		}

// 		needSibRow = nextRow
// 		nextRow = []uint64{}
// 	}
// 	if len(bp.Proof) != 0 {
// 		return nil, fmt.Errorf("too many proofs, %d remain", len(bp.Proof))
// 	}
// 	bp.Proof = proof // restore from backup
// 	return proofTree, nil
// }
