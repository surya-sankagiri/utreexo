package patriciaaccumulator

import (
	"fmt"
	"os"
	"os/user"
	"testing"
)

func TestSmallVerify(t *testing.T) {

	usr, err := user.Current()
	if err != nil {
		panic(err)
	}

	home := usr.HomeDir
	dir := home + "/Library/Application Support/Bitcoin/"

	err = os.Chdir(dir + "blocks")
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

func TestMediumVerifyRight(t *testing.T) {

	usr, err := user.Current()
	if err != nil {
		panic(err)
	}

	home := usr.HomeDir
	dir := home + "/Library/Application Support/Bitcoin/"

	err = os.Chdir(dir + "blocks")
	if err != nil {
		panic(err)
	}

	f := NewForest(nil, false)

	leaf1 := Leaf{Hash: Hash{1}}
	leaf2 := Leaf{Hash: Hash{2}}
	leaf3 := Leaf{Hash: Hash{3}}
	leaf4 := Leaf{Hash: Hash{4}}
	leaf5 := Leaf{Hash: Hash{5}}
	leaf6 := Leaf{Hash: Hash{6}}
	leaf7 := Leaf{Hash: Hash{7}}
	leaf8 := Leaf{Hash: Hash{8}}

	_, err = f.Modify([]Leaf{leaf1, leaf2}, []uint64{})

	_, err = f.Modify([]Leaf{leaf3, leaf4}, []uint64{1})
	_, err = f.Modify([]Leaf{leaf5, leaf6}, []uint64{2})
	_, err = f.Modify([]Leaf{leaf7, leaf8}, []uint64{4})

	if err != nil {
		t.Fail()
	}

	delHashes := []Hash{leaf7.Hash, leaf8.Hash}

	bp, err := f.ProveBatch(delHashes)

	if err != nil {
		t.Fatal(err)
	}

	fmt.Print("Foo\n")
	fmt.Print(f)
	fmt.Print(bp)

	correct := verifyBatchProof(bp, f.lookup.stateRoot, delHashes)

	if !correct {
		t.Fatal("verification failed")
	}
}

func TestMediumVerifyLeft(t *testing.T) {

	usr, err := user.Current()
	if err != nil {
		panic(err)
	}

	home := usr.HomeDir
	dir := home + "/Library/Application Support/Bitcoin/"

	err = os.Chdir(dir + "blocks")
	if err != nil {
		panic(err)
	}

	f := NewForest(nil, false)

	leaf1 := Leaf{Hash: Hash{1}}
	leaf2 := Leaf{Hash: Hash{2}}
	leaf3 := Leaf{Hash: Hash{3}}
	leaf4 := Leaf{Hash: Hash{4}}
	leaf5 := Leaf{Hash: Hash{5}}
	leaf6 := Leaf{Hash: Hash{6}}
	leaf7 := Leaf{Hash: Hash{7}}
	leaf8 := Leaf{Hash: Hash{8}}

	_, err = f.Modify([]Leaf{leaf1, leaf2}, []uint64{})

	_, err = f.Modify([]Leaf{leaf3, leaf4}, []uint64{1})
	_, err = f.Modify([]Leaf{leaf5, leaf6}, []uint64{2})
	_, err = f.Modify([]Leaf{leaf7, leaf8}, []uint64{4})

	if err != nil {
		t.Fail()
	}

	delHashes := []Hash{leaf1.Hash, leaf4.Hash}

	bp, err := f.ProveBatch(delHashes)

	if err != nil {
		t.Fatal(err)
	}

	fmt.Print("Foo\n")
	fmt.Print(f)
	fmt.Print(bp)

	correct := verifyBatchProof(bp, f.lookup.stateRoot, delHashes)

	if !correct {
		t.Fatal("verification failed")
	}
}

func TestChainVerify(t *testing.T) {

	// Setup accumulator

	usr, err := user.Current()
	if err != nil {
		panic(err)
	}

	home := usr.HomeDir
	dir := home + "/Library/Application Support/Bitcoin/"

	err = os.Chdir(dir + "blocks")
	if err != nil {
		panic(err)
	}

	f := NewForestParams(10000)

	// Setup simulated chain

	numAdds := uint32(100)

	sc := NewSimChain(0x07)
	sc.lookahead = 400

	for b := 0; b < 3; b++ {

		adds, _, delHashes := sc.NextBlock(numAdds)

		bp, err := f.ProveBatch(delHashes)

		// The number of zeros in the bp.PrefixLogWidths should be the same as the number of hashes
		numZeros := 0
		for _, logWidth := range bp.prefixLogWidths {
			if logWidth == 0 {
				numZeros++
			}
		}
		if numZeros != len(delHashes) {
			t.Logf("Num zeros %d", numZeros)
			t.Logf("Num hashes %d", len(delHashes))
			t.Fatal("Wrong number of zeros")
		}

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
