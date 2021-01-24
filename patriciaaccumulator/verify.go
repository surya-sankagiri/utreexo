package patriciaaccumulator

import "math"

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

//prefixFromLogWidth returns the unique prefixRange whose width is 2**logwidth and contains target
//TODO: move to prefixrange.go?
func prefixFromLogWidth(target uint64, logwidth uint8) prefixRange {
	width := uint64(math.Exp2(logwidth))
	n := target / width         // n shd. be s.t. target is in the interval [n*width, (n+1)*width)
	prefix := (2*n + 1) * width // (2*n + 1) * width is the representation for the interval [n*width, (n+1)*width)
	return prefixRange{prefix}
}

//unpackBatchProof converts BatchProof to LongBatchProof
func unpackBatchProof(bp BatchProof) LongBatchProof {
	prefixes := make([]prefixRange, len(bp.prefixLogWidths))
	i := 0
	for t := range bp.Targets {
		// for every target t, convert all prefixlogwidths associated with t to prefixes
		if bp.prefixLogWidths[i] != 0 {
			panic("Logical error!")
		}
		for {
			prefixes[i] = prefixFromLogWidth(t, bp.prefixLogWidths[i])
			i += 1
			if i == len(bp.prefixLogWidths) || bp.prefixLogWidths[i] == 0 {
				break
			}
		}
	}
	return LongBatchProof{bp.Targets, bp.hashes, prefixes}
}

// getRootHash determines the root that the batchproof was produced on
// Also returns the number of hashes used for recursion
func (bp LongBatchProof) getRootHash(leafHashes []Hash) (Hash, int) {
	// a sanity check
	if len(leafHashes) != len(bp.Targets) {
		panic("Wrong number of targets")
	}

	if len(bp.prefixes) == 0 {
		// This should mean there is a single leaf
		if len(bp.Targets) != 1 {
			panic("Not a single leaf")

		}
		// Create a node out of this single leaf and return its hash
		prefix := prefixFromLogWidth(bp.Targets[0], 0)
		node := newInternalPatriciaNode(leafHashes[0], leafHashes[0], prefix)
		return node.hash(), 1
	}

	biggestPrefix := bp.prefixes[0]
	// Figure out the root midpoint
	for _, prefix := range bp.prefixes {
		if biggestPrefix.subset(prefix) {
			biggestPrefix = prefix
		}
	}
	rootPrefix := biggestPrefix

	// Does the root midpoint have two children?
	var leftBatchProof, rightBatchProof LongBatchProof
	hasLeftChild := false
	hasRightChild := false
	for _, prefix := range bp.prefixes {
		if prefix.midpoint() < rootPrefix.midpoint() {
			hasLeftChild = true
			leftBatchProof.prefixes = append(leftBatchProof.prefixes, prefix)
		} else if prefix.midpoint() > rootPrefix.midpoint() {
			hasRightChild = true
			rightBatchProof.prefixes = append(rightBatchProof.prefixes, prefix)
		}
	}
	for _, target := range bp.Targets {
		if target < rootPrefix.midpoint() {
			hasLeftChild = true
			leftBatchProof.Targets = append(leftBatchProof.Targets, target)

		} else if target >= rootPrefix.midpoint() { // Equal means target in right side
			hasRightChild = true
			rightBatchProof.Targets = append(rightBatchProof.Targets, target)

		}
	}

	// var leftHash, rightHash Hash
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

		node := newInternalPatriciaNode(leftHash, bp.hashes[leftUsed], rootPrefix)

		return node.hash(), leftUsed + 1
	}
	// If no left child
	if !hasLeftChild && hasRightChild {
		leftUsed := 1
		rightBatchProof.hashes = bp.hashes[leftUsed:]
		rightHash, rightUsed := rightBatchProof.getRootHash(leafHashes[numLeftTargets:])

		node := newInternalPatriciaNode(bp.hashes[0], rightHash, rootPrefix)

		return node.hash(), 1 + rightUsed
	}
	// If there should never be neither left or right, except in the leaf case, which we have already covered
	panic("Oops")
}
