package bridgenode

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/surya-sankagiri/utreexo/accumulator"
	"github.com/surya-sankagiri/utreexo/util"
)

// initBridgeNodeState attempts to load and initialize the chain state from the disk.
// If a chain state is not present, chain is initialized to the genesis
// returns forest, height, lastIndexOffsetHeight, pOffset and error
func initBridgeNodeState(
	p chaincfg.Params, dataDir string, forestInRam, forestCached bool,
	offsetFinished chan bool) (forest *accumulator.Forest,
	height int32, knownTipHeight int32, err error) {

	// Default behavior is that the user should delete all offsetdata
	// if they have new blk*.dat files to sync
	// User needs to re-index blk*.dat files when added new files to sync

	// Both the blk*.dat offset and rev*.dat offset is checked at the same time
	// If either is incomplete or not complete, they're both removed and made
	// anew
	// Check if the offsetfiles for both rev*.dat and blk*.dat are present
	if util.HasAccess(util.OffsetFilePath) {
		knownTipHeight, err = restoreLastIndexOffsetHeight(
			offsetFinished)
		if err != nil {
			err = fmt.Errorf("restoreLastIndexOffsetHeight error: %s", err.Error())
			return
		}
	} else {
		fmt.Println("Offsetfile not present or half present. " +
			"Indexing offset for blocks blk*.dat files...")
		knownTipHeight, err = createOffsetData(p, dataDir, offsetFinished)
		if err != nil {
			err = fmt.Errorf("createOffsetData error: %s", err.Error())
			return
		}
		fmt.Printf("tip height %d\n", knownTipHeight)
	}

	// Check if the forestdata is present
	if util.HasAccess(util.ForestFilePath) {
		fmt.Println("Has access to forestdata, resuming")
		forest, err = restoreForest(
			util.ForestFilePath, util.MiscForestFilePath, forestInRam, forestCached)
		if err != nil {
			err = fmt.Errorf("restoreForest error: %s", err.Error())
			return
		}
		height, err = restoreHeight()
		if err != nil {
			err = fmt.Errorf("restoreHeight error: %s", err.Error())
			return
		}
	} else {
		fmt.Println("Creating new forestdata")
		forest, err = createForest(forestInRam, forestCached)
		height = 1 // note that blocks start at 1, block 0 doesn't go into set
		if err != nil {
			err = fmt.Errorf("createForest error: %s", err.Error())
			return
		}
	}

	return
}

// saveBridgeNodeData saves the state of the bridgenode so that when the
// user restarts, they'll be able to resume.
// Saves height, forest fields, and pOffset
func saveBridgeNodeData(
	forest *accumulator.Forest, height int32, inRam bool) error {

	if inRam {
		forestFile, err := os.OpenFile(
			util.ForestFilePath,
			os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		err = forest.WriteForestToDisk(forestFile)
		if err != nil {
			return err
		}
	}

	heightFile, err := os.OpenFile(
		util.ForestLastSyncedBlockHeightFilePath,
		os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	err = binary.Write(heightFile, binary.BigEndian, height)
	if err != nil {
		return err
	}

	// write other misc forest data
	miscForestFile, err := os.OpenFile(
		util.MiscForestFilePath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	err = forest.WriteMiscData(miscForestFile)
	if err != nil {
		return err
	}

	return nil
}
