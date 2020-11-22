package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

	//
	"runtime/pprof"
	"runtime/trace"
	"syscall"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/sirupsen/logrus"

	_ "net/http/pprof"

	bridge "github.com/surya-sankagiri/utreexo/bridgenode"
	"github.com/surya-sankagiri/utreexo/util"
)

var msg = `
Usage: server [OPTION]
A dynamic hash based accumulator designed for the Bitcoin UTXO set
The bridgenode server generates proofs and serves to the CSN node.

OPTIONS:
  -net=mainnet                 configure whether to use mainnet. Optional.
  -net=regtest                 configure whether to use regtest. Optional.
  -inram                       Keep forest data in ram instead of on disk
  -datadir="path/to/directory" set a custom DATADIR.
                               Defaults to the Bitcoin Core DATADIR path

  -cpuprof                     configure whether to use use cpu profiling
  -memprof                     configure whether to use use heap profiling

`

// bit of a hack. Standard flag lib doesn't allow flag.Parse(os.Args[2]).
// You need a subcommand to do so.
var optionCmd = flag.NewFlagSet("", flag.ExitOnError)
var netCmd = optionCmd.String("net", "testnet",
	"Target network. (testnet, regtest, mainnet) Usage: '-net=regtest'")
var dataDirCmd = optionCmd.String("datadir", "",
	`Set a custom datadir. Usage: "-datadir='path/to/directory'"`)
var forestInRam = optionCmd.Bool("inram", false,
	`keep forest in ram instead of disk.  Faster but needs lots of ram`)
var forestCache = optionCmd.Bool("cache", false,
	`use ram-cached forest.  Speed between on disk and fully in-ram`)
var traceCmd = optionCmd.String("trace", "",
	`Enable trace. Usage: 'trace='path/to/file'`)
var cpuProfCmd = optionCmd.String("cpuprof", "",
	`Enable pprof cpu profiling. Usage: 'cpuprof='path/to/file'`)
var memProfCmd = optionCmd.String("memprof", "",
	`Enable pprof heap profiling. Usage: 'memprof='path/to/file'`)

func init() {

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	logrus.SetOutput(os.Stdout)

	// Only logrus the warning severity or above.
	logrus.SetLevel(logrus.DebugLevel)
}

func main() {

	optionCmd.Parse(os.Args[1:])

	var param chaincfg.Params // wire.BitcoinNet
	if *netCmd == "testnet" {
		param = chaincfg.TestNet3Params
	} else if *netCmd == "regtest" {
		param = chaincfg.RegressionNetParams
	} else if *netCmd == "mainnet" {
		param = chaincfg.MainNetParams
	} else {
		fmt.Println("Invalid net flag given.")
		fmt.Println(msg)
		os.Exit(1)
	}

	if *cpuProfCmd != "" {
		f, err := os.Create(*cpuProfCmd)
		if err != nil {
			fmt.Println(err)
			fmt.Println(msg)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
	}

	if *memProfCmd != "" {
		f, err := os.Create(*memProfCmd)
		if err != nil {
			fmt.Println(err)
			fmt.Println(msg)
			os.Exit(1)
		}
		pprof.WriteHeapProfile(f)
	}

	if *traceCmd != "" {
		f, err := os.Create(*traceCmd)
		if err != nil {
			fmt.Println(err)
			fmt.Println(msg)
			os.Exit(1)
		}
		trace.Start(f)
	}

	// set datadir
	var dataDir string
	if *dataDirCmd == "" { // No custom datadir given by the user
		dataDir = util.GetBitcoinDataDir()
		if param.Name != chaincfg.MainNetParams.Name {
			dataDir = filepath.Join(dataDir, param.Name)
		}
		dataDir = filepath.Join(dataDir, "/blocks")
	} else {
		dataDir = *dataDirCmd // set dataDir to the one set by the user
	}

	// listen for SIGINT, SIGTERM, or SIGQUIT from the os
	sig := make(chan bool, 1)
	handleIntSig(sig, *cpuProfCmd, *traceCmd)

	fmt.Printf("datadir is %s\n", dataDir)
	err := bridge.BuildProofs(param, dataDir, *forestInRam, *forestCache, sig)
	// err := bridge.BuildPatriciaProofs(param, dataDir, *forestInRam, *forestCache, sig)
	if err != nil {
		fmt.Printf("Buildproofs error: %s\n", err.Error())
		panic("server halting")
	}

}

func handleIntSig(sig chan bool, cpuProfCmd, traceCmd string) {
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		<-s
		if cpuProfCmd != "" {
			pprof.StopCPUProfile()
		}

		if traceCmd != "" {
			trace.Stop()
		}
		sig <- true
	}()
}
