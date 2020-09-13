package patriciaaccumulator

import "github.com/surya-sankagiri/utreexo/util"

type UPatriciaData struct {
	AccProof BatchPatriciaProof
	UtxoData []util.LeafData
}
