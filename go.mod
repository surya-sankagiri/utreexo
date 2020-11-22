module github.com/surya-sankagiri/utreexo

go 1.12

require (
	github.com/BoltonBailey/utreexo v0.0.0-20200829210935-77c8a8438003 // indirect
	github.com/adiabat/bech32 v0.0.0-20170505011816-6289d404861d
	github.com/boltdb/bolt v1.3.1
	github.com/btcsuite/btcd v0.20.1-beta
	github.com/btcsuite/btcutil v1.0.2
	github.com/dgraph-io/badger/v2 v2.2007.2
	github.com/golang/snappy v0.0.1 // indirect
	github.com/hashicorp/golang-lru v0.5.4
	github.com/openconfig/goyang v0.2.1
	github.com/peterbourgon/diskv v2.0.1+incompatible
	github.com/prologic/bitcask v0.3.6
	github.com/sirupsen/logrus v1.6.0
	github.com/syndtr/goleveldb v1.0.0
	golang.org/x/crypto v0.0.0-20200604202706-70a84ac30bf9 // indirect
	golang.org/x/sys v0.0.0-20200602225109-6fdc65e7d980 // indirect
)

replace github.com/btcsuite/btcd => github.com/rjected/btcd v0.0.0-20200718165331-907190b086ba
