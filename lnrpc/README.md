lnrpc
=====

[![Build Status](http://img.shields.io/travis/lightningnetwork/lnd.svg)](https://travis-ci.org/lightningnetwork/lnd) 
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://"github.com/breez/lightninglib/blob/master/LICENSE)
[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/"github.com/breez/lightninglib/lnrpc)

This lnrpc package implements both a client and server for `lnd`s RPC system
which is based off of the high-performance cross-platform
[gRPC](http://www.grpc.io/) RPC framework. By default, only the Go
client+server libraries are compiled within the package. In order to compile
the client side libraries for other supported languages, the `protoc` tool will
need to be used to generate the compiled protos for a specific language.

The following languages are supported as clients to `lnrpc`: C++, Go, Node.js,
Java, Ruby, Android Java, PHP, Python, C#, Objective-C.

## Service: Lightning

The list of defined RPCs on the service `Lightning` are the following (with a brief
description):

  * WalletBalance
     * Returns the wallet's current confirmed balance in BTC.
  * ChannelBalance
     * Returns the daemons' available aggregate channel balance in BTC.
  * GetTransactions
     * Returns a list of on-chain transactions that pay to or are spends from
       `lnd`.
  * SendCoins
     * Sends an amount of satoshis to a specific address.
  * ListUnspent
     * Lists available utxos within a range of confirmations.
  * SubscribeTransactions
     * Returns a stream which sends async notifications each time a transaction
       is created or one is received that pays to us.
  * SendMany
     * Allows the caller to create a transaction with an arbitrary fan-out
       (many outputs).
  * NewAddress
     * Returns a new address, the following address types are supported:
       pay-to-witness-key-hash (p2wkh) and nested-pay-to-witness-key-hash
       (np2wkh).
  * SignMessage
     * Signs a message with the node's identity key and returns a
       zbase32 encoded signature.
  * VerifyMessage
     * Verifies a signature signed by another node on a message. The other node
       must be an active node in the channel database.
  * ConnectPeer
     * Connects to a peer identified by a public key and host.
  * DisconnectPeer
     * Disconnects a peer identified by a public key.
  * ListPeers
     * Lists all available connected peers.
  * GetInfo
     * Returns basic data concerning the daemon.
  * PendingChannels
     * List the number of pending (not fully confirmed) channels.
  * ListChannels
     * List all active channels the daemon manages.
  * OpenChannelSync
     * OpenChannelSync is a synchronous version of the OpenChannel RPC call.
  * OpenChannel
     * Attempts to open a channel to a target peer with a specific amount and
       push amount.
  * CloseChannel
     * Attempts to close a target channel. A channel can either be closed
       cooperatively if the channel peer is online, or using a "force" close to
       broadcast the latest channel state.
  * SendPayment
     * Send a payment over Lightning to a target peer.
  * SendPaymentSync
     * SendPaymentSync is the synchronous non-streaming version of SendPayment.
  * SendToRoute
    * Send a payment over Lightning to a target peer through a route explicitly
      defined by the user.
  * SendToRouteSync
    * SendToRouteSync is the synchronous non-streaming version of SendToRoute.
  * AddInvoice
     * Adds an invoice to the daemon. Invoices are automatically settled once
       seen as an incoming HTLC.
  * ListInvoices
     * Lists all stored invoices.
  * LookupInvoice
     * Attempts to look up an invoice by payment hash (r-hash).
  * SubscribeInvoices
     * Creates a uni-directional stream which receives async notifications as
       the daemon settles invoices
  * DecodePayReq
     * Decode a payment request, returning a full description of the conditions
       encoded within the payment request.
  * ListPayments
     * List all outgoing Lightning payments the daemon has made.
  * DeleteAllPayments
     * Deletes all outgoing payments from DB.
  * DescribeGraph
     * Returns a description of the known channel graph from the PoV of the
       node.
  * GetChanInfo
     * Returns information for a specific channel identified by channel ID.
  * GetNodeInfo
     * Returns information for a particular node identified by its identity
       public key.
  * QueryRoutes
     * Queries for a possible route to a target peer which can carry a certain
       amount of payment.
  * GetNetworkInfo
     * Returns some network level statistics.
  * StopDaemon
     * Sends a shutdown request to the interrupt handler, triggering a graceful
       shutdown of the daemon.
  * SubscribeChannelGraph
     * Creates a stream which receives async notifications upon any changes to the
       channel graph topology from the point of view of the responding node.
  * DebugLevel
     * Set logging verbosity of lnd programmatically
  * FeeReport
     * Allows the caller to obtain a report detailing the current fee schedule
       enforced by the node globally for each channel.
  * UpdateChannelPolicy
     * Allows the caller to update the fee schedule and channel policies for all channels
       globally, or a particular channel

## Service: WalletUnlocker

The list of defined RPCs on the service `WalletUnlocker` are the following (with a brief
description):

  * CreateWallet
     * Set encryption password for the wallet database.
  * UnlockWallet
     * Provide a password to unlock the wallet database.

## Installation and Updating

```bash
$ go get -u "github.com/breez/lightninglib/lnrpc
```

## Generate protobuf definitions

1. Download [v.3.4.0](https://github.com/google/protobuf/releases/tag/v3.4.0) of
`protoc` for your operating system and add it to your `PATH`.
For example, if using macOS:
```bash
$ curl -LO https://github.com/google/protobuf/releases/download/v3.4.0/protoc-3.4.0-osx-x86_64.zip
$ unzip protoc-3.4.0-osx-x86_64.zip -d protoc
$ export PATH=$PWD/protoc/bin:$PATH
```

2. Install `golang/protobuf` at commit `aa810b61a9c79d51363740d207bb46cf8e620ed5` (v1.2.0).
```bash
$ git clone https://github.com/golang/protobuf $GOPATH/src/github.com/golang/protobuf
$ cd $GOPATH/src/github.com/golang/protobuf
$ git reset --hard aa810b61a9c79d51363740d207bb46cf8e620ed5
$ make
```

3. Install 'genproto' at commit `a8101f21cf983e773d0c1133ebc5424792003214`.
```bash
$ go get google.golang.org/genproto
$ cd $GOPATH/src/google.golang.org/genproto
$ git reset --hard a8101f21cf983e773d0c1133ebc5424792003214
```

4. Install `grpc-ecosystem/grpc-gateway` at commit `f2862b476edcef83412c7af8687c9cd8e4097c0f`.
```bash
$ git clone https://github.com/grpc-ecosystem/grpc-gateway $GOPATH/src/github.com/grpc-ecosystem/grpc-gateway
$ cd $GOPATH/src/github.com/grpc-ecosystem/grpc-gateway
$ git reset --hard f2862b476edcef83412c7af8687c9cd8e4097c0f
$ go install ./protoc-gen-grpc-gateway ./protoc-gen-swagger
```

5. Run [`gen_protos.sh`](https://github.com/breez/lightninglib/blob/master/lnrpc/gen_protos.sh) to generate new protobuf definitions.
