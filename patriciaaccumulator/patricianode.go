package patriciaaccumulator

import (
	"crypto/sha256"
	"encoding/binary"
)

type patriciaNode struct {
	left   Hash
	right  Hash
	prefix prefixRange
}

func (p *patriciaNode) hash() Hash {
	// var empty Hash
	// if p.left == empty || p.right == empty {
	// 	panic("got an empty leaf here. ")
	// }
	hashBytes := append(p.left[:], p.right[:]...)
	midpointBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(midpointBytes, uint64(p.prefix))
	return sha256.Sum256(append(hashBytes, midpointBytes...))
}

func newLeafPatriciaNode(hash Hash, location uint64) patriciaNode {
	return patriciaNode{hash, hash, singletonRange(location)}
}

func newInternalPatriciaNode(left, right Hash, prefix prefixRange) patriciaNode {
	if prefix.isSingleton() {
		panic("Tried to make internal patricia node with singleton range")
	}
	return patriciaNode{left, right, prefix}
}

func newPatriciaNode(child1, child2 patriciaNode) patriciaNode {

	var p patriciaNode

	var leftChild, rightChild patriciaNode

	// Does child1 come first?
	if child1.prefix.min() < child2.prefix.min() {
		leftChild = child1
		rightChild = child2
	} else {
		leftChild = child2
		rightChild = child1
	}

	p.left = leftChild.hash()
	p.right = rightChild.hash()

	return newInternalPatriciaNode(leftChild.hash(), rightChild.hash(), commonPrefix(leftChild.prefix, rightChild.prefix))
}
