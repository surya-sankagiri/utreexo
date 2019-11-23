package txottl

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/mit-dci/utreexo/cmd/blockparser"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type Hash [32]byte

func HashFromString(s string) Hash {
	return sha256.Sum256([]byte(s))
}

// for parallel txofile building we need to have a buffer
type txotx struct {
	// the inputs as a string.  Note that this string has \ns in it, and it's
	// the whole block of inputs.
	// also includes the whole + line with the output!
	// includes the +, the txid the ; z and ints, but no x, ints or commas
	// (basically whatever lit produced in mainnet.txos)
	txText string

	outputtxid string
	// here's all the death heights of the output txos
	deathHeights []uint32
}

type deathInfo struct {
	deathHeight, blockPos, txPos uint32
}

// for each block, make a slice of txotxs in order.  The slice will stay in order.
// also make the deathheights slices for all the txotxs the right size.
// then hand the []txotx slice over to the worker function which can make the
// lookups in parallel and populate the deathheights.  From there you can go
// back to serial to write back to the txofile.

// ttlLookup takes the slice of txotxs and fills in the deathheights
func lookupBlock(block []*txotx, db *leveldb.DB) {

	// I don't think buffering this will do anything..?
	infoChan := make(chan deathInfo)

	var remaining uint32

	// go through every tx
	for blockPos, tx := range block {
		// go through every output
		for txPos, _ := range tx.deathHeights {
			// increment counter, and send off to a worker
			remaining++
			go lookerUpperWorker(
				tx.outputtxid, uint32(blockPos), uint32(txPos), infoChan, db)
		}
	}

	var rcv deathInfo
	for remaining > 0 {
		//		fmt.Printf("%d left\t", remaining)
		rcv = <-infoChan
		block[rcv.blockPos].deathHeights[rcv.txPos] = rcv.deathHeight
		remaining--
	}

	return
}

// lookerUpperWorker does the hashing and db read, then returns it's result
// via a channel
func lookerUpperWorker(
	txid string, blockPos, txPos uint32,
	infoChan chan deathInfo, db *leveldb.DB) {

	// start deathInfo struct to send back
	var di deathInfo
	di.blockPos, di.txPos = blockPos, txPos

	// build string and hash it (nice that this in parallel too)
	utxostring := fmt.Sprintf("%s;%d", txid, txPos)
	opHash := HashFromString(utxostring)

	// make DB lookup
	ttlbytes, err := db.Get(opHash[:], nil)
	if err == leveldb.ErrNotFound {
		//		fmt.Printf("can't find %s;%d in file", txid, txPos)
		ttlbytes = make([]byte, 4) // not found is 0
	} else if err != nil {
		// some other error
		panic(err)
	}
	if len(ttlbytes) != 4 {
		fmt.Printf("val len %d, op %s;%d\n", len(ttlbytes), txid, txPos)
		panic("ded")
	}

	di.deathHeight = BtU32(ttlbytes)
	// send back to the channel and this output is done
	infoChan <- di

	return
}

// read from the DB and tack on TTL values
func ReadTTLdb(isTestnet bool, txos string, ttldb string, sig chan bool) error {

	//Channel to alert the main loop to break
	stopGoing := make(chan bool, 1)

	//Channel to alert stopTxottl it's ok to exit
	done := make(chan bool, 1)

	//Handles SIG from the os
	go stopTxottl(sig, stopGoing, done)

	//Check if -testnet=true is given and that the actual file
	//is for testnet and vise versa
	checkTestnet(isTestnet)

	// open database
	o := new(opt.Options)
	o.CompactionTableSizeMultiplier = 8
	o.ReadOnly = true
	lvdb, err := leveldb.OpenFile(ttldb, o)
	if err != nil {
		panic(err)
	}
	defer lvdb.Close()

	//Open the *.txos file to read
	txofile, err := os.OpenFile(txos, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer txofile.Close()
	//Make a ttl.*.txos file. Append if it exists
	ttlfile, err := os.OpenFile("ttl."+txos, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer ttlfile.Close()

	//position of where the bufio should read
	var position int64

	//If there is a saved position, read off that
	//If there isn't, return 0
	if blockparser.HasAccess("ttlfilelastposition") == true {
		f, _ := os.Open("ttlfilelastposition")
		var read [4]byte
		f.Read(read[:])
		position = int64(BtU32(read[:]))
	} else {
		position = 0
	}

	//Get the tip number from the ttl.*.txos file
	//Returns 0 if there isn't a ttl.*.txos file
	tip, err := blockparser.GetTipNum("ttl." + txos)
	if err != nil {
		panic(err)
	}

	height := uint32(tip)

	// height starts at 1 because there are no transactions in block 0
	height += 1

	//Make a saved position file. Overwrites if there is one.
	ttlfilesync, err := os.OpenFile("ttlfilelastposition", os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer ttlfilesync.Close()

	blocktxs := []*txotx{new(txotx)}
	scanner := bufio.NewScanner(txofile)

	//bool for stopping the scanner.Scan loop
	var stop bool

	fmt.Println("Generating txo time to live...")

	//skip the previous read lines
	for i := 0; i < int(position); i++ {
		scanner.Scan()
	}
	//stop only becomes true when the os gives SIGINT, SIGTERM, SIGQUIT
	//AND the block that it was working on is written
	for scanner.Scan() && stop != true {
		//Update position per cycle
		position++

		switch scanner.Text()[0] {
		case '-':
			// add it in to the last txotx
			blocktxs[len(blocktxs)-1].txText += scanner.Text() + "\n"

		case '+':
			// add the whole line to inputBlob.  don't put a newline. do put
			// an x.
			blocktxs[len(blocktxs)-1].txText += scanner.Text() + "x"

			// chop up string
			parts := strings.Split(scanner.Text()[1:], ";")
			txid := parts[0]
			postsemicolon := parts[1]

			txoIndicators := strings.Split(postsemicolon, "z")
			numoutputs, err := strconv.Atoi(txoIndicators[0])
			if err != nil {
				return err
			}

			blocktxs[len(blocktxs)-1].outputtxid = txid
			blocktxs[len(blocktxs)-1].deathHeights = make([]uint32, numoutputs)

			// if len(blocktxs[len(blocktxs)-1].deathHeights) == 0 {
			//	fmt.Printf("txid\n", txid)
			//	panic("ded")
			// }
			// actually don't bother with unspendables, just look em up and they
			// won't be there.  Whatever.
			/*
				// detect unspendables & don't look for when they're spent
				unspendable := make(map[int]bool)
				// I think this is overkill as there's only ever one unspendable
				// output per tx.  but just in case. get em all.
				if len(txoIndicators) > 1 {
					unspendables := txoIndicators[1:]
					for _, zstring := range unspendables {
						n, err := strconv.Atoi(zstring)
						if err != nil {
							return err
						}
						unspendable[n] = true
					}
				}
			*/

			// done with this txotx, make the next one and append
			blocktxs = append(blocktxs, new(txotx))

		case 'h':
			// we started a tx but shouldn't have
			blocktxs = blocktxs[:len(blocktxs)-1]

			// call function to make all the db lookups and find deathheights
			// that part is in parallel.
			lookupBlock(blocktxs, lvdb)

			// write filled in txotx slice
			for _, tx := range blocktxs {
				// the txTest has all the inputs, and the output, and an x.
				// we just have to stick the numbers and commas on here.
				txstring := tx.txText
				for _, deathheight := range tx.deathHeights {
					if deathheight == 0 {
						txstring += "s,"
					} else {
						txstring += fmt.Sprintf("%d,", deathheight-height)
					}
				}

				_, err = ttlfile.WriteString(txstring + "\n")
				if err != nil {
					return err
				}
			}

			_, err = ttlfile.WriteString(scanner.Text() + "\n")
			if err != nil {
				return err
			}
			fmt.Printf("done with height %d\n", height)

			height++

			// start next block
			// wipe all block txs
			blocktxs = []*txotx{new(txotx)}

			//Check if stopSig is no longer false
			//stop = true makes the loop exit
			select {
			case stop = <-stopGoing:
			default:
			}

		default:
			panic("unknown string")
		}

	}
	fmt.Println("Done Writing.")

	//Only write where we left off after a block has been finished
	ttlfilesync.WriteAt(U32tB(uint32(position))[:], 0)

	//Tell stopTxottl that it's ok to quit now
	done <- true
	return nil
}

func checkTestnet(isTestnet bool) {
	if isTestnet == false {
		f, err := os.Open("mainnet.txos")
		if err != nil {
			fmt.Println("mainnet.txos not present. Please check option -testnet=true is set if simulating testnet")
			fmt.Println("Exiting...")
			os.Exit(2)
		}
		f.Close()
	} else {
		f, err := os.Open("testnet.txos")
		if err != nil {
			fmt.Println("testnet.txos not present. Please uncheck option -testnet=true is set if simulating mainnet")
			fmt.Println("Exiting...")
			os.Exit(2)
		}
		f.Close()
	}
}

//stopTxottl receives and handles sig from the system
//Handles SIGTERM, SIGINT, and SIGQUIT
func stopTxottl(sig chan bool, stopGoing chan bool, done chan bool) {
	<-sig
	//Tell ReadTTLdb to finish the block it's working on
	stopGoing <- true

	//Wait until ReadTTLdb says it's ok to quit
	<-done
	fmt.Println("Exiting...")
	os.Exit(0)
}
