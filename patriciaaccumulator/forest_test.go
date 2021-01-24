package patriciaaccumulator

import (
	"fmt"
	"os"
	"testing"
)

func TestDeleteReverseOrder(t *testing.T) {

	err := os.Chdir("/Users/boltonbailey/Library/Application Support/Bitcoin/blocks")
	if err != nil {
		panic(err)
	}

	f := NewForest(nil, false)

	leaf1 := Leaf{Hash: Hash{1}}
	leaf2 := Leaf{Hash: Hash{2}}

	_, err = f.Modify([]Leaf{leaf1}, nil)

	_, err = f.Modify([]Leaf{leaf2}, nil)
	if err != nil {
		t.Fail()
	}
	_, err = f.Modify(nil, []uint64{0, 1})
	if err != nil {
		t.Log(err)
		t.Fatal("could not delete leaves 1 and 0")
	}
}

func TestModifyUnsortedDels(t *testing.T) {

	err := os.Chdir("/Users/boltonbailey/Library/Application Support/Bitcoin/blocks")
	if err != nil {
		panic(err)
	}

	f := NewForest(nil, false)

	leaf1 := Leaf{Hash: Hash{1}}
	leaf2 := Leaf{Hash: Hash{2}}

	_, err = f.Modify([]Leaf{leaf1}, nil)

	_, err = f.Modify([]Leaf{leaf2}, nil)
	if err != nil {
		t.Fail()
	}
	_, err = f.Modify(nil, []uint64{1, 0})
	if err == nil {
		t.Fatal(fmt.Errorf(
			"Modify should fail when passed dels out of order"))
	}
}

const correctString = `Root is: [117 8 236 126 131 44]
hash [117 8 236 126 131 44] prefix:(Range from 0 to 2) left [35 73 48 245 167 225] right [7 160 72 78 1 132]
 leafnode hash [35 73 48 245 167 225] at postion 0 left [1 0 0 0 0 0] right [1 0 0 0 0 0]
 leafnode hash [7 160 72 78 1 132] at postion 1 left [2 0 0 0 0 0] right [2 0 0 0 0 0]

`

func TestForestString(t *testing.T) {

	err := os.Chdir("/Users/boltonbailey/Library/Application Support/Bitcoin/blocks")
	if err != nil {
		panic(err)
	}

	f := NewForest(nil, false)

	leaf1 := Leaf{Hash: Hash{1}}
	leaf2 := Leaf{Hash: Hash{2}}

	_, err = f.Modify([]Leaf{leaf1}, nil)
	if err != nil {
		t.Fail()
	}

	_, err = f.Modify([]Leaf{leaf2}, nil)
	if err != nil {
		t.Fail()
	}

	if f.String() != correctString {
		fmt.Print(f.String())
		fmt.Print(correctString)
		t.Fail()
	}
}

func TestForestAddDel(t *testing.T) {

	// Setup accumulator

	// TODO remove boltonbailey path from chdir
	err := os.Chdir("/Users/boltonbailey/Library/Application Support/Bitcoin/blocks")
	if err != nil {
		panic(err)
	}

	f := NewForestParams(10000)

	// Setup simulated chain

	numAdds := uint32(10000)

	sc := NewSimChain(0x07)
	sc.lookahead = 400

	for b := 0; b < 5; b++ {

		adds, _, delHashes := sc.NextBlock(numAdds)

		bp, err := f.ProveBatch(delHashes)
		if err != nil {
			t.Fatal(err)
		}
		bp.SortTargets()
		_, err = f.Modify(adds, bp.Targets)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("nl %d", f.numLeaves)
	}
}

func TestRAMForestAddDel(t *testing.T) {

	// Setup accumulator
	f := NewForest(nil, false)

	// Setup simulated chain

	numAdds := uint32(10000)

	sc := NewSimChain(0x07)
	sc.lookahead = 400

	for b := 0; b < 5; b++ {

		adds, _, delHashes := sc.NextBlock(numAdds)

		bp, err := f.ProveBatch(delHashes)
		if err != nil {
			t.Fatal(err)
		}
		bp.SortTargets()
		_, err = f.Modify(adds, bp.Targets)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("nl %d", f.numLeaves)
	}
}

func TestDeleteNonExisting(t *testing.T) {

	// TODO remove boltonbailey path from chdir
	err := os.Chdir("/Users/boltonbailey/Library/Application Support/Bitcoin/blocks")
	if err != nil {
		panic(err)
	}

	f := NewForestParams(10000)

	deletions := []uint64{0}

	_, err = f.Modify(nil, deletions)
	if err == nil {
		t.Fatal(fmt.Errorf(
			"shouldn't be able to delete non-existing leaf 0 from empty forest"))
	}
}
