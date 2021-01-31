package patriciaaccumulator

import (
	"fmt"
	"os"
	"testing"
)

func TestSmallVerify(t *testing.T) {

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

	delHashes := []Hash{leaf2.Hash}

	individualProof, err := f.lookup.RetrieveProof(1)
	fmt.Println(individualProof)

	if err != nil {
		t.Fatal(err)
	}

	individualProofs, err := f.lookup.RetrieveListProofs([]uint64{1})
	fmt.Println(individualProofs)
	if err != nil {
		t.Fatal(err)
	}
	if len(individualProofs) != 1 {
		t.Fatal("Wrong number of individual proofs")
	}

	bp, err := f.ProveBatch(delHashes)

	if err != nil {
		t.Fatal(err)
	}

	fmt.Print(bp)

	correct := verifyBatchProof(bp, f.lookup.stateRoot, delHashes)

	if !correct {
		t.Fatal("verification failed")
	}
}

func TestChainVerify(t *testing.T) {

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

	for b := 0; b < 3; b++ {

		adds, _, delHashes := sc.NextBlock(numAdds)

		bp, err := f.ProveBatch(delHashes)
		if err != nil {
			t.Fatal(err)
		}
		// bp.SortTargets()
		correct := verifyBatchProof(bp, f.lookup.stateRoot, delHashes)
		if !correct {
			t.Fatal("verification failed")
		}

		_, err = f.Modify(adds, bp.Targets)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("nl %d", f.numLeaves)
	}
}
