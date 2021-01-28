package patriciaaccumulator

import (
	"os"
	"testing"
)

func TestForestVerify(t *testing.T) {

	// Setup accumulator

	// TODO remove boltonbailey path from chdir
	err := os.Chdir("/Users/suryanarayanasankagiri/Library/Application Support/Bitcoin/blocks")
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
		bp.SortTargets()
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
