
# Merkle Trees Optimized for Stateless Clients in Bitcoin

This repository is the codebase for our paper ["Merkle Trees Optimized for Stateless Clients in Bitcoin"](https://fc21.ifca.ai/wtsc/WTSC21paper8.pdf) published in the Workshop on Trusted Smart Contracts at FC '21.

The repository is a fork of the mit-dci [UTREEXO](https://github.com/mit-dci/utreexo) project. Our code works by replacing the `/accumulator` module from the mit-dci code with our own `/patriciaaccumulator` module. Our implementation uses a [Merkle trie (Patricia tree)](https://en.wikipedia.org/wiki/Trie) indexed by insertion order to store bitcoin UTXO data.