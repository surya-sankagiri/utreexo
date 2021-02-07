package patriciaaccumulator

import (
	"fmt"

	logrus "github.com/sirupsen/logrus"
)

// verifyBatchProof takes a block proof and reconstructs / verifies it.
// takes a blockproof to verify, list of leafHashes at the targets, and the state root to check against.
// it returns a bool of whether the proof worked
func verifyLongBatchProof(
	bp LongBatchProof, root Hash, leafHashes []Hash) bool {

	// if nothing to prove, it worked
	if len(bp.Targets) == 0 {
		return true
	}

	proofRoot, _ := bp.getRootHash(leafHashes)

	return proofRoot == root

}

func verifyBatchProof(
	bp BatchProof, root Hash, leafHashes []Hash) bool {

	lbp := unpackBatchProof(bp)

	return verifyLongBatchProof(lbp, root, leafHashes)
}

//unpackBatchProof converts BatchProof to LongBatchProof
func unpackBatchProof(bp BatchProof) LongBatchProof {
	prefixes := make([]prefixRange, len(bp.prefixLogWidths))
	i := 0
	logrus.Println("PrefixLogWidths:", bp.prefixLogWidths)
	logrus.Println("Targets:", bp.Targets)
	for _, t := range bp.Targets {
		// for every target t, convert all prefixlogwidths associated with t to prefixes
		if bp.prefixLogWidths[i] != 0 {
			panic("Logical error!")
		}
		for {
			prefixes[i] = prefixFromLogWidth(t, bp.prefixLogWidths[i])
			i++
			if i == len(bp.prefixLogWidths) || bp.prefixLogWidths[i] == 0 {
				break
			}
		}
		// if i == len(bp.prefixLogWidths) {
		// 	break
		// }
	}
	return LongBatchProof{bp.Targets, bp.hashes, prefixes}
}

// getRootHash determines the root that the batchproof was produced on.
// Also returns the number of internal hashes used for recursion
// The input leafhashes is the slice of hashes,
// corresponding to the  targets in the proof
func (bp LongBatchProof) getRootHash(leafHashes []Hash) (Hash, int) {
	// a sanity check
	if len(leafHashes) != len(bp.Targets) {
		panic("Wrong number of targets")
	}
	// The number of internal nodes of the batchproof should be one less than the number of endpoints
	// Endpoints are either hashes or targets
	// Internal nodes appear as prefixes but not targets
	if len(bp.hashes) != len(bp.prefixes)-2*len(bp.Targets)+1 {
		fmt.Print(bp)
		panic("Malformed LongBatchProof")
	}
	if len(bp.prefixes) == 0 {
		panic("Number of prefixes in a proof should never be zero")
	}
	if len(bp.prefixes) == 1 {
		// This should mean there is a single leaf
		if len(bp.Targets) != 1 {
			panic("Not a single leaf")

		}
		// Create a node out of this single leaf and return its hash
		// prefix := prefixFromLogWidth(bp.Targets[0], 0)
		if !bp.prefixes[0].contains(bp.Targets[0]) {
			panic("A single prefix should contain the corresponding target")
		}
		node := newLeafPatriciaNode(leafHashes[0], bp.Targets[0])
		return node.hash(), 1
	}

	biggestPrefix := bp.prefixes[0]
	// Figure out the root midpoint
	fmt.Println(len(bp.prefixes))
	for _, prefix := range bp.prefixes {

		if biggestPrefix.subset(prefix) {
			biggestPrefix = prefix
		}
		fmt.Println(prefix, biggestPrefix)
	}
	rootPrefix := biggestPrefix
	if rootPrefix.isSingleton() {
		fmt.Println("root prefix:", rootPrefix)
		panic("rootPrefix should not be a singleton")
	}

	// Does the root midpoint have two children?
	var leftBatchProof, rightBatchProof LongBatchProof
	hasLeftChild := false
	hasRightChild := false
	for _, prefix := range bp.prefixes {
		if prefix.subset(rootPrefix.left()) {
			hasLeftChild = true
			leftBatchProof.prefixes = append(leftBatchProof.prefixes, prefix)
		} else if prefix.subset(rootPrefix.right()) {
			hasRightChild = true
			rightBatchProof.prefixes = append(rightBatchProof.prefixes, prefix)
		} else if prefix != rootPrefix { // it is possible that prefix == rootPrefix, in which case we do nothing
			fmt.Println("prefix:", prefix, "root prefix", rootPrefix)
			panic(fmt.Sprintf("Prefix should be subset of rootPrefix"))
		}
	}
	for _, target := range bp.Targets {
		if rootPrefix.left().contains(target) {
			if !hasLeftChild {
				panic("Should have a left child")
			}
			leftBatchProof.Targets = append(leftBatchProof.Targets, target)

		} else if rootPrefix.right().contains(target) {
			if !hasRightChild {
				panic("Should have a right child")
			}
			rightBatchProof.Targets = append(rightBatchProof.Targets, target)

		} else {
			panic("Target should be contained in rootPrefix")
		}
	}

	numLeftTargets := len(leftBatchProof.Targets)
	// If it has a left and right child, we must simply prove the subtrees
	if hasLeftChild && hasRightChild {
		leftBatchProof.hashes = bp.hashes
		leftHash, leftUsed := leftBatchProof.getRootHash(leafHashes[:numLeftTargets])
		rightBatchProof.hashes = bp.hashes[leftUsed:]
		rightHash, rightUsed := rightBatchProof.getRootHash(leafHashes[numLeftTargets:])

		node := newInternalPatriciaNode(leftHash, rightHash, rootPrefix)

		return node.hash(), leftUsed + rightUsed
	}
	// If no right child
	if hasLeftChild && !hasRightChild {
		leftBatchProof.hashes = bp.hashes
		leftHash, leftUsed := leftBatchProof.getRootHash(leafHashes[:numLeftTargets])

		if len(bp.hashes) < leftUsed+1 {
			fmt.Print(bp, leftUsed, leafHashes, "\n")
			panic("Not enough hashes")
		}

		node := newInternalPatriciaNode(leftHash, bp.hashes[leftUsed], rootPrefix)

		return node.hash(), leftUsed + 1
	}
	// If no left child
	if !hasLeftChild && hasRightChild {
		leftUsed := 1
		if len(bp.hashes) < 1 {
			panic("There are no hashes left, but there should be some to cover the right side")
		}
		rightBatchProof.hashes = bp.hashes[leftUsed:]
		rightHash, rightUsed := rightBatchProof.getRootHash(leafHashes[numLeftTargets:])

		node := newInternalPatriciaNode(bp.hashes[0], rightHash, rootPrefix)

		return node.hash(), 1 + rightUsed
	}
	// If there should never be neither left or right, except in the leaf case, which we have already covered
	panic("Oops")
}
