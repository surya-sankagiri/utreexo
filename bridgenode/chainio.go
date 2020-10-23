package bridgenode

import (
	"encoding/binary"
	"os"

	"github.com/btcsuite/btcd/chaincfg"
	accumulator "github.com/surya-sankagiri/utreexo/accumulator"

	"github.com/surya-sankagiri/utreexo/util"
)

// createOffsetData restores the offsetfile needed to index the
// blocks in the raw blk*.dat and raw rev*.dat files.
func createOffsetData(
	p chaincfg.Params, dataDir string, offsetFinished chan bool) (
	lastIndexOffsetHeight int32, err error) {

	// Set the Block Header hash
	// buildOffsetFile matches the header hash to organize
	// for blk*.dat files
	hash, err := util.GenHashForNet(p)
	if err != nil {
		return 0, err
	}

	// TODO allow the user to pass a custom offsetfile path and
	// custom lastOffsetHeight path instead of just ""
	lastIndexOffsetHeight, err = buildOffsetFile(dataDir, *hash, "", "")
	if err != nil {
		return 0, err
	}

	offsetFinished <- true

	return
}

// createForest initializes forest
func createForest(inRam, cached bool) (forest *accumulator.Forest, err error) {
	if inRam {
		forest = accumulator.NewForest(nil, false)
		return
	}

	// Where the forestfile exists
	forestFile, err := os.OpenFile(
		util.ForestFilePath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	}

	// Restores all the forest data
	forest = accumulator.NewForest(forestFile, cached)

	return
}

// restoreForest restores forest fields based off the existing forestdata
// on disk.
func restoreForest(
	forestFilename, miscFilename string,
	inRam, cached bool) (forest *accumulator.Forest, err error) {

	// Where the forestfile exists
	forestFile, err := os.OpenFile(forestFilename, os.O_RDWR, 0400)
	if err != nil {
		return
	}
	// Where the misc forest data exists
	miscForestFile, err := os.OpenFile(miscFilename, os.O_RDONLY, 0400)
	if err != nil {
		return
	}
	// panic("TODO implement this")
	_ = forestFile
	_ = miscForestFile
	// forest, err = accumulator.RestoreForest(miscForestFile, forestFile, inRam, cached)
	return
}

// restoreHeight restores height from util.ForestLastSyncedBlockHeightFilePath
func restoreHeight() (height int32, err error) {

	// if there is a heightfile, get the height from that
	// heightFile saves the last block that was written to ttldb
	if util.HasAccess(util.ForestLastSyncedBlockHeightFilePath) {
		heightFile, err := os.OpenFile(
			util.ForestLastSyncedBlockHeightFilePath,
			os.O_RDONLY, 0400)
		if err != nil {
			return 0, err
		}
		err = binary.Read(heightFile, binary.BigEndian, &height)
		if err != nil {
			return 0, err
		}
	}
	return
}

// restoreLastIndexOffsetHeight restores the lastIndexOffsetHeight
func restoreLastIndexOffsetHeight(offsetFinished chan bool) (
	lastIndexOffsetHeight int32, err error) {

	f, err := os.OpenFile(
		util.LastIndexOffsetHeightFilePath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return 0, err
	}

	// grab the last block height from currentoffsetheight
	// currentoffsetheight saves the last height from the offsetfile
	err = binary.Read(f, binary.BigEndian, &lastIndexOffsetHeight)
	if err != nil {
		return 0, err
	}
	// if there is a offset file, we should pass true to offsetFinished
	// to let stopParse() know that it shouldn't delete offsetfile
	offsetFinished <- true

	return
}
