package patriciaaccumulator

import (
	"fmt"
	"math/bits"
)

// The prefixRange type represents a binary interval of integers,
// that is, an inclusive-exclusive interval of the form [n * 2^k, (n+1) * 2^k)
// Put another way, a prefixRange represents the set of 63-bit integers with a specifc prefix.
// This type is represented by a uint64,
// with [n * 2^k, (n+1) * 2^k) corresponding to
//  (2n+1) * 2^k
type prefixRange uint64

// The number of elements in the set
func (r prefixRange) width() uint64 {
	// If r = (2n+1) * 2^k representing [n * 2^k, (n+1) * 2^k),
	// we are looking for 2^k
	// Thus, we want to 0 out all bits but the lowest order 1 bit of r and return it
	// One can see that the following bit arithmetic does this.
	return uint64(((r - 1) & r) ^ r)
}

func (r prefixRange) logWidth() uint8 {

	return uint8(bits.TrailingZeros64(r.width()))
}

// The minimum value in the set
func (r prefixRange) min() uint64 {
	// If r = (2n+1) * 2^k representing [n * 2^k, (n+1) * 2^k),
	// we are looking for n * 2^k
	// Thus, we want
	return (uint64(r) - r.width()) >> 1
}

func (r prefixRange) midpoint() uint64 {
	if r.isSingleton() {
		panic("Cannot take left midpoint of singleton")
	}
	return r.min() + r.width()/2
}

// (One more than) the maximum value in the set
func (r prefixRange) max() uint64 {
	return r.min() + r.width()

}

func (r *prefixRange) String() string {

	return fmt.Sprint("(Range from ", r.min(), " to ", r.max(), ")")
}

// subset returns whether r is a subset of s
func (r prefixRange) subset(s prefixRange) bool {
	return (s.min() <= r.min()) && (r.max() <= s.max())
}

// returns whether r contains i
func (r prefixRange) contains(i uint64) bool {
	return (r.min() <= i) && (i < r.max())
}

// returns whether r contains only 1 element
func (r prefixRange) isSingleton() bool {
	return r.width() == 1
}

// The only value in a singleton set
func (r prefixRange) value() uint64 {
	if !r.isSingleton() {
		panic("Cannot call value on non-singleton")
	}
	return r.min()
}

func singletonRange(v uint64) prefixRange {
	return prefixRange(2*v + 1)
}

// returns the left child prefixRange of r (the first half of r)
func (r prefixRange) left() prefixRange {

	return prefixRange(r.min() + r.midpoint())
}

// returns the right child prefixRange of r (the second half of r)
func (r prefixRange) right() prefixRange {

	return prefixRange(r.max() + r.midpoint())
}

// returns the parent prefixRange of r (the second half of r)
func (r prefixRange) parent() prefixRange {

	// Determine which of min and max has higher 2-arity
	if bits.TrailingZeros64(r.min()) < bits.TrailingZeros64(r.max()) {
		return prefixRange(r.min() - r.width() + r.max())
	}

	return prefixRange(r.min() + r.max() + r.width())

}

// The smallest prefix range containing the two disjoint ranges
func commonPrefix(child1, child2 prefixRange) prefixRange {
	if child1.subset(child2) || child2.subset(child1) {
		panic("Cannot combine overlapping ranges")
	}

	var min, max uint64
	min1 := child1.min()
	min2 := child2.min()
	max1 := child1.max()
	max2 := child2.max()

	if min1 < min2 {
		min = min1
	} else {
		min = min2
	}
	if max1 < max2 {
		max = max2
	} else {
		max = max1
	}

	max--

	diffBits := min ^ max
	reverseDiffBits := bits.Reverse64(diffBits)
	// Includes first diff bit
	prefixMask := bits.Reverse64(reverseDiffBits ^ (reverseDiffBits - 1))

	newMin := min & prefixMask
	newMax := max&prefixMask + ^prefixMask + 1

	newRange := prefixRange(newMin + newMax)

	// fmt.Println(min, max, diffBits, reverseDiffBits, prefixMask, ^prefixMask, newMin, newMax)

	if newRange.min() != newMin {
		panic(fmt.Sprintln("Error making range", child1, child2))
	}
	if newRange.max() != newMax {
		panic(fmt.Sprintln("Error making range", child1, child2))
	}

	return newRange

}
