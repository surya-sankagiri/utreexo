package utreexo

import (
	"fmt"
	"sync"
)

// Modify is the main function that deletes then adds elements to the accumulator
func (p *Pollard) Modify(adds []LeafTXO, dels []uint64) error {
	err := p.rem2(dels)
	if err != nil {
		return err
	}
	fmt.Printf("pol pre add %s", p.toString())
	err = p.add(adds)
	if err != nil {
		return err
	}

	return nil
}

// Stats :
func (p *Pollard) Stats() string {
	s := fmt.Sprintf("pol nl %d tops %d he %d re %d ow %d \n",
		p.numLeaves, len(p.tops), p.hashesEver, p.rememberEver, p.overWire)
	return s
}

// Add a leaf to a pollard.  Not as simple!
func (p *Pollard) add(adds []LeafTXO) error {

	// General algo goes:
	// 1 make a new node & assign data (no neices; at bottom)
	// 2 if this node is on a height where there's already a top,
	// then swap neices with that top, hash the two datas, and build a new
	// node 1 higher pointing to them.
	// goto 2.

	// this does everything 1 at a time, with hashing also mixed in, so that's
	// pretty sub-optimal, but we're not doing multi-thread yet

	for _, a := range adds {

		//		if p.numLeaves < p.Minleaves ||
		//			(add.Duration < p.Lookahead && add.Duration > 0) {
		//			remember = true
		//			p.rememberEver++
		//		}
		if a.Remember {
			p.rememberEver++
		}

		err := p.addOne(a.Hash, a.Remember)
		if err != nil {
			return err
		}
	}
	//	fmt.Printf("added %d, nl %d tops %d\n", len(adds), p.numLeaves, len(p.tops))
	return nil
}

/*
Algo explanation with catchy terms: grab, swap, hash, new, pop
we're iterating through the tops of the pollard.  Tops correspond with 1-bits
in numLeaves.  As soon as we hit a 0 (no top), we're done.

grab: Grab the lowest top.
pop: pop off the lowest top.
swap: swap the neices of the node we grabbed and our new node
hash: calculate the hashes of the old top and new node
new: create a new parent node, with the hash as data, and the old top / prev new node
as neices (not neices though, children)

It's pretty dense: very little code but a bunch going on.

Not that `p.tops = p.tops[:len(p.tops)-1]` would be a memory leak (I guess?)
but that leftTop is still being pointed to anyway do it's OK.

*/

// add a single leaf to a pollard
func (p *Pollard) addOne(add Hash, remember bool) error {
	// basic idea: you're going to start at the LSB and move left;
	// the first 0 you find you're going to turn into a 1.

	// make the new leaf & populate it with the actual data you're trying to add
	n := new(polNode)
	n.data = add
	if remember {
		// flag this leaf as memorable via it's left pointer
		n.niece[0] = n // points to itself (mind blown)
	}
	// if add is forgetable, forget all the new nodes made
	var h uint8
	// loop until we find a zero; destroy tops until you make one
	for ; (p.numLeaves>>h)&1 == 1; h++ {
		// grab, pop, swap, hash, new
		leftTop := p.tops[len(p.tops)-1]                           // grab
		p.tops = p.tops[:len(p.tops)-1]                            // pop
		leftTop.niece, n.niece = n.niece, leftTop.niece            // swap
		nHash := Parent(leftTop.data, n.data)                      // hash
		n = &polNode{data: nHash, niece: [2]*polNode{&leftTop, n}} // new
		p.hashesEver++

		n.prune()

	}

	// the new tops are all the 1 bits above where we got to, and nothing below where
	// we got to.  We've already deleted all the lower tops, so append the new
	// one we just made onto the end.

	p.tops = append(p.tops, *n)
	p.numLeaves++
	return nil
}

// rem2 deletes stuff from the pollard, using remtrans2
func (p *Pollard) rem2(dels []uint64) error {
	if len(dels) == 0 {
		return nil // that was quick
	}
	ph := p.height() // height of pollard
	nextNumLeaves := p.numLeaves - uint64(len(dels))

	// get all the swaps, then apply them all
	swaprows := remTrans2(dels, p.numLeaves, ph)

	wg := new(sync.WaitGroup)

	fmt.Printf(" @@@@@@ rem2 nl %d ph %d rem %v\n", p.numLeaves, ph, dels)
	var hashdirt []uint64

	// swap all the nodes
	for h := uint8(0); h < ph; h++ {
		rowdirt := hashdirt
		hashdirt = []uint64{}
		for _, s := range swaprows[h] {
			if s.from == s.to {
				// TODO should get rid of these upstream
				continue
			}

			hn, err := p.swapNodes(s)
			if err != nil {
				return err
			}

			// chop off first rowdirt (current row) if it's getting hashed
			// by the swap
			if len(rowdirt) != 0 && rowdirt[0] == s.to {
				rowdirt = rowdirt[1:]
			}

			// TODO some of these hashes are useless as they end up outside
			// the forest.  Example, leaves 0...7, delete 4,5,6.  7 moves
			// to 4, and 10 gets hashed but discarded.  maybe this only happens
			// when something is moving TO a new top location?  If it is, that's
			// easy enough to check and skip the hashing.

			// aside from TODO above, always hash the parent of swap "to"
			// note that we never have two siblings receiving "tos"
			// oh but we might get multiple copies of the same to...? nah

			fmt.Printf("send %d to hasher\n", up1(s.to, ph))
			wg.Add(1)
			go hn.run(wg)

			// is parent's parent in forest? if so, add *parent* to dirt
			parpar := upMany(s.to, 2, ph)
			if inForest(parpar, p.numLeaves, ph) {
				par := up1(s.to, ph)
				fmt.Printf("ph %d nl %d %d parpar %d is in forest\n", ph, p.numLeaves, s.to, parpar)
				// if so, and it's not already in hashdirt, add it
				if len(hashdirt) == 0 || hashdirt[len(hashdirt)-1] != s.to {
					hashdirt = append(hashdirt, par)
				}
			}
		}
		// done with swaps for this row, now hashdirt
		// build hashable nodes from hashdirt
		for _, d := range rowdirt {
			_, _, hn, err := p.grabPos(d)
			if err != nil {
				return err
			}
			if hn == nil { // if d is a top
				fmt.Printf("hn is nil at pos %d\n", d)
				continue
			}
			wg.Add(1)
			go hn.run(wg)
		}
		wg.Wait() // wait for all hashing to finish at end of each row
		fmt.Printf("done with row %d\n", h)
	}

	// set new tops
	nextTopPoss, _ := getTopsReverse(nextNumLeaves, ph)
	nexTops := make([]polNode, len(nextTopPoss))
	for i, _ := range nexTops {
		nti, _, _, err := p.grabPos(nextTopPoss[i])
		if err != nil {
			return err
		}
		if nti == nil {
			return fmt.Errorf("want top %d at %d but nil", i, nextTopPoss[i])
		}
		nexTops[i] = *nti
	}
	p.numLeaves = nextNumLeaves
	reversePolNodeSlice(nexTops)
	p.tops = nexTops
	return nil
}

// reHash hashes all specified locations (and their parents up to roots)
// TODO currently hashing up to old roots instead of new ones
func (p *Pollard) reHash(dirt [][]uint64) error {
	if len(dirt) == 0 {
		return nil
	}

	ph := p.height()
	tops, topHeights := getTopsReverse(p.numLeaves, p.height())

	if topHeights[0] == 0 {
		tops = tops[1:]
		topHeights = topHeights[1:]
	}

	var nextDirtRow []uint64
	var curRowTop uint64
	for h := uint8(1); h < p.height(); h++ {
		fmt.Printf("th %v tops %v\n", topHeights, tops)
		if topHeights[0] == h {
			curRowTop = tops[0]
			topHeights = topHeights[1:]
			tops = tops[1:]
		}
		dirtrow := dirt[h]
		for len(dirtrow) > 0 {
			fmt.Printf("dirtslice: %v\n", dirtrow)
			err := p.reHashOne(dirtrow[0])
			if err != nil {
				return fmt.Errorf("rem2 rehash %d %s", dirtrow[0], err.Error())
			}
			par := up1(dirtrow[0], ph)
			// add dirt unless:
			// this node is a current top, or already in the nextdirt slice
			if dirtrow[0] != curRowTop &&
				(len(nextDirtRow) == 0 || nextDirtRow[len(dirtrow)-1] != par) &&
				(len(dirtrow) == 0 || dirtrow[len(dirtrow)-1] != par) {
				fmt.Printf("pol h %d add %d to nrDirt %v crt %d\n",
					h, par, nextDirtRow, curRowTop)
				nextDirtRow = append(nextDirtRow, par)
			}

			dirtrow = dirtrow[1:]
			if len(dirtrow) == 0 {
				break
			}
		}
		dirtrow = append(nextDirtRow, dirtrow...)
		nextDirtRow = []uint64{}
	}
	return nil
}

// TODO for rem:
// get rid of dirtymap and rehash entirely?  Not sure if we can
// if not then make it so only rehash hashes, and movenode doesn't.  same with
// pruning?  seems that rehash and movenode do a lot of the same things.

// rem deletes stuff from the pollard.  The hard part.
func (p *Pollard) remx(dels []uint64) error {

	if len(dels) == 0 {
		return nil // that was quick
	}

	ph := p.height() // height of pollard
	nextNumLeaves := p.numLeaves - uint64(len(dels))
	overlap := p.numLeaves & nextNumLeaves
	// remove tops and add empty tops based just on popcount
	nexTops := make([]polNode, PopCount(nextNumLeaves))
	// keeping track of these separately is annoying.  I'm sure there's a
	// clever bit shifty way to not need to do this.  It doesn't actually
	// take any cpu time or ram though.
	oldTopIdx := len(p.tops) - 1
	nexTopIdx := len(nexTops) - 1

	stash, moves := removeTransformx(dels, p.numLeaves, ph)

	fmt.Printf("stash %v\n", stash)
	// TODO how about instead of a slice of uint64s, you just
	// have a slice of pointers?  Then less descent.
	var moveDirt []uint64
	var hashDirt []uint64

	tops, topHeights := getTopsReverse(p.numLeaves, ph)
	var curRowTop uint64

	//	fmt.Printf("p.h %d nl %d rem %d nnl %d stashes %d moves %d\n",
	//		ph, p.numLeaves, len(dels), nextNumLeaves, len(rawStash), len(rawMoves))
	for h := uint8(0); h < ph; h++ {
		// copy the top over directly if there's a bit overlap
		// fmt.Printf("h %d topIdx %d overlap %b\n", h, nexTopIdx, overlap)
		if (1<<h)&overlap != 0 {
			// fmt.Printf("topidx %d nexTops %d ptops %d\n",
			// topIdx, len(nexTops), len(p.tops))
			nexTops[nexTopIdx] = p.tops[oldTopIdx]
		}

		if topHeights[0] == h {
			curRowTop = tops[0]
			topHeights = topHeights[1:]
			tops = tops[1:]
		} else {
			curRowTop = 0
		}

		// hash first
		curDirt := mergeSortedSlices(moveDirt, hashDirt)
		moveDirt, hashDirt = []uint64{}, []uint64{}
		// if there's curDirt, hash
		fmt.Printf("h %d curDirt %v\n", h, curDirt)
		for _, pos := range curDirt {
			err := p.reHashOne(pos)
			if err != nil {
				return fmt.Errorf("rem rehash %s", err.Error())
			}
			parPos := up1(pos, ph)
			lhd := len(hashDirt)
			// add dirt unless:
			// this node is a current top, or already in the dirt slice
			if pos != curRowTop &&
				(lhd == 0 || hashDirt[lhd-1] != parPos) {
				fmt.Printf("pol h %d hash %d to hashDirt \n", h, parPos)
				hashDirt = append(hashDirt, parPos)
			}
		}

		fmt.Printf("this row top %d\n", curRowTop)
		// go through moves for this height
		for len(moves) > 0 && detectHeight(moves[0].to, ph) == h {
			if len(p.tops) == 0 {
				return fmt.Errorf("no tops")
			}
			fmt.Printf("mv %d -> %d\n", moves[0].from, moves[0].to)
			err := p.moveNode(moves[0])
			if err != nil {
				return err
			}
			dirt := up1(moves[0].to, ph)
			lmvd := len(moveDirt)
			// the dirt returned by moveNode is always a parent
			if lmvd == 0 || moveDirt[lmvd-1] != dirt {
				fmt.Printf("h %d mv %d to moveDirt \n", h, dirt)
				moveDirt = append(moveDirt, dirt)
			} else {
				fmt.Printf("pol skip %d  \n", dirt)
			}
			moves = moves[1:]
		}

		// then the stash on this height.  (There can be only 1)
		for len(stash) > 0 &&
			detectHeight(stash[0].to, ph) == h {
			// populate top; stashes always become tops
			fmt.Printf("stash %d -> %d\n", stash[0].from, stash[0].to)
			pr, sibs, err := p.descendToPos(stash[0].from)
			if err != nil {
				return fmt.Errorf("rem stash %s", err.Error())
			}

			if sibs[0] == nil {
				return fmt.Errorf("got nil sib[0] stashing")
			}
			// make new top if sibling nieces are known
			// otherwise need to delete the neices (same thing really,
			// just doesn't crash)
			if pr[0] != nil {
				sibs[0].niece = pr[0].niece
			} else {
				sibs[0].chop()
			}

			nexTops[nexTopIdx] = *sibs[0]
			stash = stash[1:]
		}

		// if there's a 1 in the nextNum, decrement top number
		if (1<<h)&nextNumLeaves != 0 {
			nexTopIdx--
		}
		if (1<<h)&p.numLeaves != 0 {
			oldTopIdx--
		}

	}
	p.numLeaves = nextNumLeaves
	p.tops = nexTops
	return nil
}

// moveNode moves a node from one place to another.  Note that it leaves the
// node in the "from" place to be dealt with some other way.
// Also it hashes new parents so the hashes & pointers are consistent.
func (p *Pollard) moveNode(a arrow) error {

	prfrom, sibfrom, err := p.descendToPos(a.from)
	if err != nil {
		return fmt.Errorf("from %s", err.Error())
	}

	prto, sibto, err := p.descendToPos(a.to)
	if err != nil {
		return fmt.Errorf("to %s", err.Error())
	}

	// build out full branch to target if it's not populated
	// I think this efficient / never creates usless nodes but not sure..?
	for i := range sibto {
		tgtLR := (a.to >> uint8(i)) & 1
		if sibto[i] == nil {
			sibto[i] = new(polNode)
		}
		if len(prto) > i+1 && prto[i+1] != nil {
			prto[i+1].niece[tgtLR] = sibto[i]
		}
	}

	// this works even if moving from a top, because sibfrom[0] and prfrom[0]
	// will be the same
	// gotta move the data, but move nieces if you can.  If you can't move the
	// nieces you have to delete the destination nieces!
	//	fmt.Printf("move %04x over %04x prfromnil %v\n",
	//		sibfrom[0].data[:4], sibto[0].data[:4], prfrom[0] == nil)
	sibto[0].data = sibfrom[0].data
	if prfrom[0] != nil {
		prto[0].niece = prfrom[0].niece
	} else {
		prto[0].chop() // need this
	}
	return nil
}

// the Hash & trim function called by rem().  Not currently called on leaves
func (p *Pollard) reHashOne(pos uint64) error {
	pr, sib, err := p.descendToPos(pos)
	if err != nil {
		return err
	}

	if !pr[0].auntable() {
		// return nil
		return fmt.Errorf("pos %d unauntable %x %v", pos, pr[0].data, pr[0].niece)
	}
	//	fmt.Printf("reHashOne %d pr %d sib %d pr[0] %v\n",
	//		pos, len(pr), len(sib), pr[0].niece)
	if sib[0] == nil {
		sib[0] = new(polNode)
		if len(pr) < 2 || pr[1] == nil {
			return fmt.Errorf("rehashone sib[0] nil pr[1] nil")
		}
		pr[1].niece[pos&1] = sib[0]
		//		return fmt.Errorf("sib[0] nil")
	}
	p.hashesEver++
	sib[0].data = pr[0].auntOp()
	fmt.Printf("rehashone pos %d %x, %x -> %x\n",
		pos, pr[0].niece[0].data[:4], pr[0].niece[1].data[:4], sib[0].data[:4])
	pr[0].prune()

	return nil
}

// swapNodes swaps the nodes at positions a and b.
// returns a hashable node with b, bsib, and bpar
func (p *Pollard) swapNodes(r arrow) (*hashableNode, error) {
	if !inForest(r.from, p.numLeaves, p.height()) ||
		!inForest(r.to, p.numLeaves, p.height()) {
		return nil, fmt.Errorf("swapNodes %d %d out of bounds", r.from, r.to)
	}

	// currently swaps the "values" instead of changing what parents point
	// to.  Seems easier to reason about but maybe slower?  But probably
	// doesn't matter that much because it's changing 8 bytes vs 30-something

	// TODO could be improved by getting the highest common ancestor
	// and then splitting instead of doing 2 full descents

	a, asib, _, err := p.grabPos(r.from)
	if err != nil {
		return nil, err
	}
	b, bsib, bhn, err := p.grabPos(r.to)
	if err != nil {
		return nil, err
	}
	if asib == nil || bsib == nil {
		return nil, fmt.Errorf("swapNodes %d %d sibling not found", r.from, r.to)
	}
	// fmt.Printf("swapNodes swapping %d %x with %d %x\n",
	// 	r.from, a.data[:4], r.to, b.data[:4])

	// fmt.Printf("swapNodes siblings of %x with %x\n",
	// 	asib.data[:4], bsib.data[:4])

	// do the actual swap here
	err = polSwap(a, asib, b, bsib)
	if err != nil {
		return nil, err
	}

	return bhn, nil
}

// grabPos is like descendToPos but simpler.  Returns the thing you asked for,
// as well as its sibling.  And an error if it can't get it.
// NOTE errors are not exhaustive; could return garbage without an error
func (p *Pollard) grabPos(
	pos uint64) (n, nsib *polNode, hn *hashableNode, err error) {
	tree, branchLen, bits := detectOffset(pos, p.numLeaves)
	// fmt.Printf("grab %d, tree %d, bl %d bits %x\n", pos, tree, branchLen, bits)
	bits = ^bits
	n, nsib = &p.tops[tree], &p.tops[tree]
	for h := branchLen - 1; h != 255; h-- { // go through branch
		lr := uint8(bits>>h) & 1
		if h == 0 { // if at bottom, done
			hn = new(hashableNode)
			hn.p = &nsib.data
			n, nsib = n.niece[lr^1], n.niece[lr]
			if lr&1 == 1 { // if to is even, it's on the left
				hn.l, hn.r = &n.data, &nsib.data
			} else { // otherwise it's on the right
				hn.l, hn.r = &nsib.data, &n.data
			}

			// fmt.Printf("h%d n %x nsib %x npar %x\n",
			// 	h, n.data[:4], nsib.data[:4], npar.data[:4])
			return
		}
		// if a sib doesn't exist, need to create it and hook it in
		if n.niece[lr^1] == nil {
			n.niece[lr^1] = new(polNode)
		}
		n, nsib = n.niece[lr], n.niece[lr^1]
		// fmt.Printf("h%d n %x nsib %x npar %x\n",
		// 	h, n.data[:4], nsib.data[:4], npar.data[:4])
		if n == nil {
			// if a node doesn't exist, crash
			err = fmt.Errorf("grab %d nil neice at height %d", pos, h)
			return
		}
	}
	return // only happens when returning a top
	// in which case npar will be nil
}

// DescendToPos returns the path to the target node, as well as the sibling
// path.  Retruns paths in bottom-to-top order (backwards)
// sibs[0] is the node you actually asked for
func (p *Pollard) descendToPos(pos uint64) ([]*polNode, []*polNode, error) {
	// interate to descend.  It's like the leafnum, xored with ...1111110
	// so flip every bit except the last one.
	// example: I want leaf 12.  That's 1100.  xor to get 0010.
	// descent 0, 0, 1, 0 (left, left, right, left) to get to 12 from 30.
	// need to figure out offsets for smaller trees.

	if !inForest(pos, p.numLeaves, p.height()) {
		//	if pos >= (p.numLeaves*2)-1 {
		return nil, nil,
			fmt.Errorf("OOB: descend to %d but only %d leaves", pos, p.numLeaves)
	}

	// first find which tree we're in
	tNum, branchLen, bits := detectOffset(pos, p.numLeaves)
	//	fmt.Printf("DO pos %d top %d bits %d branlen %d\n", pos, tNum, bits, branchLen)
	n := &p.tops[tNum]
	if branchLen > 64 {
		return nil, nil, fmt.Errorf("dtp top %d is nil", tNum)
	}

	bits = ^bits // just flip all the bits...
	proofs := make([]*polNode, branchLen+1)
	sibs := make([]*polNode, branchLen+1)
	// at the top of the branch, the proof and sib are the same
	proofs[branchLen], sibs[branchLen] = n, n
	for h := branchLen - 1; h < 64; h-- {
		lr := (bits >> h) & 1

		sib := n.niece[lr^1]
		n = n.niece[lr]

		if n == nil && h != 0 {
			return nil, nil, fmt.Errorf(
				"descend pos %d nil neice at height %d", pos, h)
		}

		if n != nil {
			// fmt.Printf("target %d h %d d %04x\n", pos, h, n.data[:4])
		}

		proofs[h], sibs[h] = n, sib

	}
	//	fmt.Printf("\n")
	return proofs, sibs, nil
}

// toFull takes a pollard and converts to a forest.
// For debugging and seeing what pollard is doing since there's already
// a good toString method for  forest.
func (p *Pollard) toFull() (*Forest, error) {

	ff := NewForest()
	ff.height = p.height()
	ff.numLeaves = p.numLeaves
	ff.forest = make([]Hash, 2<<ff.height)
	if p.numLeaves == 0 {
		return ff, nil
	}

	//	for topIdx, top := range p.tops {
	//	}
	for i := uint64(0); i < (2<<ff.height)-1; i++ {
		_, sib, err := p.descendToPos(i)
		if err != nil {
			//	fmt.Printf("can't get pos %d: %s\n", i, err.Error())
			continue
			//			return nil, err
		}
		if sib[0] != nil {
			ff.forest[i] = sib[0].data
			//	fmt.Printf("wrote leaf pos %d %04x\n", i, sib[0].data[:4])
		}

	}

	return ff, nil
}

func (p *Pollard) toString() string {
	f, err := p.toFull()
	if err != nil {
		return err.Error()
	}
	return f.toString()
}

// equalToForest checks if the pollard has the same leaves as the forest.
// doesn't check tops and stuff
func (p *Pollard) equalToForest(f *Forest) bool {
	if p.numLeaves != f.numLeaves {
		return false
	}

	for leafpos := uint64(0); leafpos < f.numLeaves; leafpos++ {
		n, _, _, err := p.grabPos(leafpos)
		if err != nil {
			return false
		}
		if n.data != f.forest[leafpos] {
			fmt.Printf("leaf position %d pol %x != forest %x\n",
				leafpos, n.data[:4], f.forest[leafpos][:4])
			return false
		}
	}
	return true
}
