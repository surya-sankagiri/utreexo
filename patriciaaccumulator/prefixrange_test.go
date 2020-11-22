package patriciaaccumulator

import "testing"

func TestMinMax(t *testing.T) {

	p2 := prefixRange(2)

	if p2.min() != 0 {
		t.Fail()
		t.Logf("Gave min of %v for range 2", p2.min())
	}
	if p2.max() != 2 {
		t.Fail()
		t.Logf("Gave max of %v for range 2", p2.max())
	}

	p4 := prefixRange(4)

	if p4.min() != 0 {
		t.Fail()
		t.Logf("Gave min of %v for range 4", p4.min())
	}
	if p4.max() != 4 {
		t.Fail()
		t.Logf("Gave max of %v for range 4", p4.max())
	}

	p5 := prefixRange(5)

	if p5.min() != 2 {
		t.Fail()
		t.Logf("Gave min of %v for range 5", p5.min())
	}
	if p5.max() != 3 {
		t.Fail()
		t.Logf("Gave max of %v for range 5", p5.max())
	}

}

func TestCommonPrefix(t *testing.T) {

	p2 := prefixRange(2)
	p5 := prefixRange(5)

	commonPrefix(p2, p5)

	p170 := singletonRange(170)
	p171 := singletonRange(171)

	common := commonPrefix(p170, p171)

	if common != prefixRange(170+172) {
		t.Fail()
		t.Log("Wrong combo of 170 and 171", (&common).String())
	}

}
