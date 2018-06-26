package chainntnfs_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lightninglabs/neutrino"
	"github.com/breez/lightninglib/chainntnfs"
	"github.com/breez/lightninglib/chainntnfs/bitcoindnotify"
	"github.com/breez/lightninglib/chainntnfs/btcdnotify"
	"github.com/breez/lightninglib/chainntnfs/neutrinonotify"
	"github.com/ltcsuite/ltcd/btcjson"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	"github.com/roasbeef/btcwallet/walletdb"

	"github.com/roasbeef/btcd/btcec"
	"github.com/roasbeef/btcd/chaincfg"
	"github.com/roasbeef/btcd/integration/rpctest"
	"github.com/roasbeef/btcd/rpcclient"
	"github.com/roasbeef/btcd/txscript"
	"github.com/roasbeef/btcd/wire"
	"github.com/roasbeef/btcutil"

	// Required to register the boltdb walletdb implementation.
	_ "github.com/roasbeef/btcwallet/walletdb/bdb"
)

var (
	testPrivKey = []byte{
		0x81, 0xb6, 0x37, 0xd8, 0xfc, 0xd2, 0xc6, 0xda,
		0x63, 0x59, 0xe6, 0x96, 0x31, 0x13, 0xa1, 0x17,
		0xd, 0xe7, 0x95, 0xe4, 0xb7, 0x25, 0xb8, 0x4d,
		0x1e, 0xb, 0x4c, 0xfd, 0x9e, 0xc5, 0x8c, 0xe9,
	}

	netParams       = &chaincfg.RegressionNetParams
	privKey, pubKey = btcec.PrivKeyFromBytes(btcec.S256(), testPrivKey)
	addrPk, _       = btcutil.NewAddressPubKey(pubKey.SerializeCompressed(),
		netParams)
	testAddr = addrPk.AddressPubKeyHash()
)

func getTestTxId(miner *rpctest.Harness) (*chainhash.Hash, error) {
	script, err := txscript.PayToAddrScript(testAddr)
	if err != nil {
		return nil, err
	}

	outputs := []*wire.TxOut{
		{
			Value:    2e8,
			PkScript: script,
		},
	}
	return miner.SendOutputs(outputs, 10)
}

func waitForMempoolTx(r *rpctest.Harness, txid *chainhash.Hash) error {
	var found bool
	var tx *btcutil.Tx
	var err error
	timeout := time.After(10 * time.Second)
	for !found {
		// Do a short wait
		select {
		case <-timeout:
			return fmt.Errorf("timeout after 10s")
		default:
		}
		time.Sleep(100 * time.Millisecond)

		// Check for the harness' knowledge of the txid
		tx, err = r.Node.GetRawTransaction(txid)
		if err != nil {
			switch e := err.(type) {
			case *btcjson.RPCError:
				if e.Code == btcjson.ErrRPCNoTxInfo {
					continue
				}
			default:
			}
			return err
		}
		if tx != nil && tx.MsgTx().TxHash() == *txid {
			found = true
		}
	}
	return nil
}

func testSingleConfirmationNotification(miner *rpctest.Harness,
	notifier chainntnfs.ChainNotifier, t *testing.T) {

	// We'd like to test the case of being notified once a txid reaches
	// a *single* confirmation.
	//
	// So first, let's send some coins to "ourself", obtaining a txid.
	// We're spending from a coinbase output here, so we use the dedicated
	// function.

	txid, err := getTestTxId(miner)
	if err != nil {
		t.Fatalf("unable to create test tx: %v", err)
	}

	err = waitForMempoolTx(miner, txid)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	_, currentHeight, err := miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	// Now that we have a txid, register a confirmation notification with
	// the chainntfn source.
	numConfs := uint32(1)
	confIntent, err := notifier.RegisterConfirmationsNtfn(txid, numConfs,
		uint32(currentHeight))
	if err != nil {
		t.Fatalf("unable to register ntfn: %v", err)
	}

	// Now generate a single block, the transaction should be included which
	// should trigger a notification event.
	blockHash, err := miner.Node.Generate(1)
	if err != nil {
		t.Fatalf("unable to generate single block: %v", err)
	}

	select {
	case confInfo := <-confIntent.Confirmed:
		if !confInfo.BlockHash.IsEqual(blockHash[0]) {
			t.Fatalf("mismatched block hashes: expected %v, got %v",
				blockHash[0], confInfo.BlockHash)
		}

		// Finally, we'll verify that the tx index returned is the exact same
		// as the tx index of the transaction within the block itself.
		msgBlock, err := miner.Node.GetBlock(blockHash[0])
		if err != nil {
			t.Fatalf("unable to fetch block: %v", err)
		}

		block := btcutil.NewBlock(msgBlock)
		specifiedTxHash, err := block.TxHash(int(confInfo.TxIndex))
		if err != nil {
			t.Fatalf("unable to index into block: %v", err)
		}

		if !specifiedTxHash.IsEqual(txid) {
			t.Fatalf("mismatched tx indexes: expected %v, got %v",
				txid, specifiedTxHash)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("confirmation notification never received")
	}
}

func testMultiConfirmationNotification(miner *rpctest.Harness,
	notifier chainntnfs.ChainNotifier, t *testing.T) {

	// We'd like to test the case of being notified once a txid reaches
	// N confirmations, where N > 1.
	//
	// Again, we'll begin by creating a fresh transaction, so we can obtain
	// a fresh txid.
	txid, err := getTestTxId(miner)
	if err != nil {
		t.Fatalf("unable to create test addr: %v", err)
	}

	err = waitForMempoolTx(miner, txid)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	_, currentHeight, err := miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	numConfs := uint32(6)
	confIntent, err := notifier.RegisterConfirmationsNtfn(txid, numConfs,
		uint32(currentHeight))
	if err != nil {
		t.Fatalf("unable to register ntfn: %v", err)
	}

	// Now generate a six blocks. The transaction should be included in the
	// first block, which will be built upon by the other 5 blocks.
	if _, err := miner.Node.Generate(6); err != nil {
		t.Fatalf("unable to generate single block: %v", err)
	}

	// TODO(roasbeef): reduce all timeouts after neutrino sync tightended
	// up

	select {
	case <-confIntent.Confirmed:
		break
	case <-time.After(20 * time.Second):
		t.Fatalf("confirmation notification never received")
	}
}

func testBatchConfirmationNotification(miner *rpctest.Harness,
	notifier chainntnfs.ChainNotifier, t *testing.T) {

	// We'd like to test a case of serving notifications to multiple
	// clients, each requesting to be notified once a txid receives
	// various numbers of confirmations.
	confSpread := [6]uint32{1, 2, 3, 6, 20, 22}
	confIntents := make([]*chainntnfs.ConfirmationEvent, len(confSpread))

	_, currentHeight, err := miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	// Create a new txid spending miner coins for each confirmation entry
	// in confSpread, we collect each conf intent into a slice so we can
	// verify they're each notified at the proper number of confirmations
	// below.
	for i, numConfs := range confSpread {
		txid, err := getTestTxId(miner)
		if err != nil {
			t.Fatalf("unable to create test addr: %v", err)
		}
		confIntent, err := notifier.RegisterConfirmationsNtfn(txid,
			numConfs, uint32(currentHeight))
		if err != nil {
			t.Fatalf("unable to register ntfn: %v", err)
		}
		confIntents[i] = confIntent
		err = waitForMempoolTx(miner, txid)
		if err != nil {
			t.Fatalf("tx not relayed to miner: %v", err)
		}

	}

	initialConfHeight := uint32(currentHeight + 1)

	// Now, for each confirmation intent, generate the delta number of blocks
	// needed to trigger the confirmation notification. A goroutine is
	// spawned in order to verify the proper notification is triggered.
	for i, numConfs := range confSpread {
		var blocksToGen uint32

		// If this is the last instance, manually index to generate the
		// proper block delta in order to avoid a panic.
		if i == len(confSpread)-1 {
			blocksToGen = confSpread[len(confSpread)-1] - confSpread[len(confSpread)-2]
		} else {
			blocksToGen = confSpread[i+1] - confSpread[i]
		}

		// Generate the number of blocks necessary to trigger this
		// current confirmation notification.
		if _, err := miner.Node.Generate(blocksToGen); err != nil {
			t.Fatalf("unable to generate single block: %v", err)
		}

		select {
		case conf := <-confIntents[i].Confirmed:
			// All of the notifications above were originally
			// confirmed in the same block. The returned
			// notification should list the initial confirmation
			// height rather than the height they were _fully_
			// confirmed.
			if conf.BlockHeight != initialConfHeight {
				t.Fatalf("notification has incorrect initial "+
					"conf height: expected %v, got %v",
					initialConfHeight, conf.BlockHeight)
			}
			continue
		case <-time.After(20 * time.Second):
			t.Fatalf("confirmation notification never received: %v", numConfs)
		}
	}
}

func createSpendableOutput(miner *rpctest.Harness,
	t *testing.T) (*wire.OutPoint, []byte) {

	txid, err := getTestTxId(miner)
	if err != nil {
		t.Fatalf("unable to create test addr: %v", err)
	}

	err = waitForMempoolTx(miner, txid)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	// Mine a single block which should include that txid above.
	if _, err := miner.Node.Generate(1); err != nil {
		t.Fatalf("unable to generate single block: %v", err)
	}

	// Now that we have the txid, fetch the transaction itself.
	wrappedTx, err := miner.Node.GetRawTransaction(txid)
	if err != nil {
		t.Fatalf("unable to get new tx: %v", err)
	}
	tx := wrappedTx.MsgTx()

	// Locate the output index sent to us. We need this so we can construct
	// a spending txn below.
	outIndex := -1
	var pkScript []byte
	for i, txOut := range tx.TxOut {
		if bytes.Contains(txOut.PkScript, testAddr.ScriptAddress()) {
			pkScript = txOut.PkScript
			outIndex = i
			break
		}
	}
	if outIndex == -1 {
		t.Fatalf("unable to locate new output")
	}

	return wire.NewOutPoint(txid, uint32(outIndex)), pkScript
}

func createSpendTx(outpoint *wire.OutPoint, pkScript []byte,
	t *testing.T) *wire.MsgTx {

	spendingTx := wire.NewMsgTx(1)
	spendingTx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: *outpoint,
	})
	spendingTx.AddTxOut(&wire.TxOut{
		Value:    1e8,
		PkScript: pkScript,
	})
	sigScript, err := txscript.SignatureScript(spendingTx, 0, pkScript,
		txscript.SigHashAll, privKey, true)
	if err != nil {
		t.Fatalf("unable to sign tx: %v", err)
	}
	spendingTx.TxIn[0].SignatureScript = sigScript

	return spendingTx
}

func checkNotificationFields(ntfn *chainntnfs.SpendDetail,
	outpoint *wire.OutPoint, spenderSha *chainhash.Hash,
	height int32, t *testing.T) {

	if *ntfn.SpentOutPoint != *outpoint {
		t.Fatalf("ntfn includes wrong output, reports "+
			"%v instead of %v",
			ntfn.SpentOutPoint, outpoint)
	}
	if !bytes.Equal(ntfn.SpenderTxHash[:], spenderSha[:]) {
		t.Fatalf("ntfn includes wrong spender tx sha, "+
			"reports %v instead of %v",
			ntfn.SpenderTxHash[:], spenderSha[:])
	}
	if ntfn.SpenderInputIndex != 0 {
		t.Fatalf("ntfn includes wrong spending input "+
			"index, reports %v, should be %v",
			ntfn.SpenderInputIndex, 0)
	}
	if ntfn.SpendingHeight != height {
		t.Fatalf("ntfn has wrong spending height: "+
			"expected %v, got %v", height,
			ntfn.SpendingHeight)
	}
}

func testSpendNotification(miner *rpctest.Harness,
	notifier chainntnfs.ChainNotifier, t *testing.T) {

	// We'd like to test the spend notifications for all ChainNotifier
	// concrete implementations.
	//
	// To do so, we first create a new output to our test target address.
	outpoint, pkScript := createSpendableOutput(miner, t)

	_, currentHeight, err := miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	// Now that we have an output index and the pkScript, register for a
	// spentness notification for the newly created output with multiple
	// clients in order to ensure the implementation can support
	// multi-client spend notifications.
	const numClients = 5
	spendClients := make([]*chainntnfs.SpendEvent, numClients)
	for i := 0; i < numClients; i++ {
		spentIntent, err := notifier.RegisterSpendNtfn(outpoint,
			uint32(currentHeight), false)
		if err != nil {
			t.Fatalf("unable to register for spend ntfn: %v", err)
		}

		spendClients[i] = spentIntent
	}

	// Next, create a new transaction spending that output.
	spendingTx := createSpendTx(outpoint, pkScript, t)

	// Broadcast our spending transaction.
	spenderSha, err := miner.Node.SendRawTransaction(spendingTx, true)
	if err != nil {
		t.Fatalf("unable to broadcast tx: %v", err)
	}

	err = waitForMempoolTx(miner, spenderSha)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	// Make sure notifications are not yet sent.
	for _, c := range spendClients {
		select {
		case <-c.Spend:
			t.Fatalf("did not expect to get notification before " +
				"block was mined")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Now we mine a single block, which should include our spend. The
	// notification should also be sent off.
	if _, err := miner.Node.Generate(1); err != nil {
		t.Fatalf("unable to generate single block: %v", err)
	}

	_, currentHeight, err = miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	for _, c := range spendClients {
		select {
		case ntfn := <-c.Spend:
			// We've received the spend nftn. So now verify all the
			// fields have been set properly.
			checkNotificationFields(ntfn, outpoint, spenderSha,
				currentHeight, t)
		case <-time.After(30 * time.Second):
			t.Fatalf("spend ntfn never received")
		}
	}
}

func testSpendNotificationMempoolSpends(miner *rpctest.Harness,
	notifier chainntnfs.ChainNotifier, t *testing.T) {

	// Skip this test for neutrino and bitcoind backends, as they currently
	// don't support notifying about mempool spends.
	switch notifier.(type) {
	case *neutrinonotify.NeutrinoNotifier:
		return
	case *bitcoindnotify.BitcoindNotifier:
		return
	case *btcdnotify.BtcdNotifier:
		// Go on to test this implementation.
	default:
		t.Fatalf("unknown notifier type: %T", notifier)
	}

	// We first create a new output to our test target address.
	outpoint, pkScript := createSpendableOutput(miner, t)

	_, currentHeight, err := miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	// Now that we have a output index and the pkScript, register for a
	// spentness notification for the newly created output with multiple
	// clients in order to ensure the implementation can support
	// multi-client spend notifications.

	// We first create a list of clients that will be notified on mempool
	// spends.
	const numClients = 5
	spendClientsMempool := make([]*chainntnfs.SpendEvent, numClients)
	for i := 0; i < numClients; i++ {
		spentIntent, err := notifier.RegisterSpendNtfn(outpoint,
			uint32(currentHeight), true)
		if err != nil {
			t.Fatalf("unable to register for spend ntfn: %v", err)
		}

		spendClientsMempool[i] = spentIntent
	}

	// Next, create a new transaction spending that output.
	spendingTx := createSpendTx(outpoint, pkScript, t)

	// Broadcast our spending transaction.
	spenderSha, err := miner.Node.SendRawTransaction(spendingTx, true)
	if err != nil {
		t.Fatalf("unable to broadcast tx: %v", err)
	}

	err = waitForMempoolTx(miner, spenderSha)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	// Make sure the mempool spend clients are correctly notified.
	for _, client := range spendClientsMempool {
		select {
		case ntfn, ok := <-client.Spend:
			if !ok {
				t.Fatalf("channel closed unexpectedly")
			}

			checkNotificationFields(ntfn, outpoint, spenderSha,
				currentHeight+1, t)

		case <-time.After(5 * time.Second):
			t.Fatalf("did not receive notification")
		}
	}

	// Create new clients that register after the tx is in the mempool
	// already, but should still be notified.
	newSpendClientsMempool := make([]*chainntnfs.SpendEvent, numClients)
	for i := 0; i < numClients; i++ {
		spentIntent, err := notifier.RegisterSpendNtfn(outpoint,
			uint32(currentHeight), true)
		if err != nil {
			t.Fatalf("unable to register for spend ntfn: %v", err)
		}

		newSpendClientsMempool[i] = spentIntent
	}

	// Make sure the new mempool spend clients are correctly notified.
	for _, client := range newSpendClientsMempool {
		select {
		case ntfn, ok := <-client.Spend:
			if !ok {
				t.Fatalf("channel closed unexpectedly")
			}

			checkNotificationFields(ntfn, outpoint, spenderSha,
				currentHeight+1, t)

		case <-time.After(5 * time.Second):
			t.Fatalf("did not receive notification")
		}
	}

	// Now we mine a single block, which should include our spend. The
	// notification should not be sent off again.
	if _, err := miner.Node.Generate(1); err != nil {
		t.Fatalf("unable to generate single block: %v", err)
	}

	// When a block is mined, the mempool notifications we registered should
	// not be sent off again, and the channel should be closed.
	for _, c := range spendClientsMempool {
		select {
		case _, ok := <-c.Spend:
			if ok {
				t.Fatalf("channel should have been closed")
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("expected clients to be closed.")
		}
	}
	for _, c := range newSpendClientsMempool {
		select {
		case _, ok := <-c.Spend:
			if ok {
				t.Fatalf("channel should have been closed")
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("expected clients to be closed.")
		}
	}

}

func testBlockEpochNotification(miner *rpctest.Harness,
	notifier chainntnfs.ChainNotifier, t *testing.T) {

	// We'd like to test the case of multiple registered clients receiving
	// block epoch notifications.

	const numBlocks = 10
	const numClients = 5
	var wg sync.WaitGroup

	// Create numClients clients which will listen for block notifications. We
	// expect each client to receive 10 notifications for each of the ten
	// blocks we generate below. So we'll use a WaitGroup to synchronize the
	// test.
	for i := 0; i < numClients; i++ {
		epochClient, err := notifier.RegisterBlockEpochNtfn()
		if err != nil {
			t.Fatalf("unable to register for epoch notification")
		}

		wg.Add(numBlocks)
		go func() {
			for i := 0; i < numBlocks; i++ {
				<-epochClient.Epochs
				wg.Done()
			}
		}()
	}

	epochsSent := make(chan struct{})
	go func() {
		wg.Wait()
		close(epochsSent)
	}()

	// Now generate 10 blocks, the clients above should each receive 10
	// notifications, thereby unblocking the goroutine above.
	if _, err := miner.Node.Generate(numBlocks); err != nil {
		t.Fatalf("unable to generate blocks: %v", err)
	}

	select {
	case <-epochsSent:
	case <-time.After(30 * time.Second):
		t.Fatalf("all notifications not sent")
	}
}

func testMultiClientConfirmationNotification(miner *rpctest.Harness,
	notifier chainntnfs.ChainNotifier, t *testing.T) {

	// We'd like to test the case of a multiple clients registered to
	// receive a confirmation notification for the same transaction.

	txid, err := getTestTxId(miner)
	if err != nil {
		t.Fatalf("unable to create test tx: %v", err)
	}

	err = waitForMempoolTx(miner, txid)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	var wg sync.WaitGroup
	const (
		numConfsClients = 5
		numConfs        = 1
	)

	_, currentHeight, err := miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	// Register for a conf notification for the above generated txid with
	// numConfsClients distinct clients.
	for i := 0; i < numConfsClients; i++ {
		confClient, err := notifier.RegisterConfirmationsNtfn(txid,
			numConfs, uint32(currentHeight))
		if err != nil {
			t.Fatalf("unable to register for confirmation: %v", err)
		}

		wg.Add(1)
		go func() {
			<-confClient.Confirmed
			wg.Done()
		}()
	}

	confsSent := make(chan struct{})
	go func() {
		wg.Wait()
		close(confsSent)
	}()

	// Finally, generate a single block which should trigger the unblocking
	// of all numConfsClients blocked on the channel read above.
	if _, err := miner.Node.Generate(1); err != nil {
		t.Fatalf("unable to generate block: %v", err)
	}

	select {
	case <-confsSent:
	case <-time.After(30 * time.Second):
		t.Fatalf("all confirmation notifications not sent")
	}
}

// Tests the case in which a confirmation notification is requested for a
// transaction that has already been included in a block. In this case, the
// confirmation notification should be dispatched immediately.
func testTxConfirmedBeforeNtfnRegistration(miner *rpctest.Harness,
	notifier chainntnfs.ChainNotifier, t *testing.T) {

	// First, let's send some coins to "ourself", obtaining a txid.  We're
	// spending from a coinbase output here, so we use the dedicated
	// function.

	txid3, err := getTestTxId(miner)
	if err != nil {
		t.Fatalf("unable to create test tx: %v", err)
	}

	err = waitForMempoolTx(miner, txid3)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	// Generate another block containing tx 3, but we won't register conf
	// notifications for this tx until much later. The notifier must check
	// older blocks when the confirmation event is registered below to ensure
	// that the TXID hasn't already been included in the chain, otherwise the
	// notification will never be sent.
	_, err = miner.Node.Generate(1)
	if err != nil {
		t.Fatalf("unable to generate block: %v", err)
	}

	txid1, err := getTestTxId(miner)
	if err != nil {
		t.Fatalf("unable to create test tx: %v", err)
	}

	err = waitForMempoolTx(miner, txid1)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	txid2, err := getTestTxId(miner)
	if err != nil {
		t.Fatalf("unable to create test tx: %v", err)
	}

	err = waitForMempoolTx(miner, txid2)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	_, currentHeight, err := miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	// Now generate another block containing txs 1 & 2.
	blockHash, err := miner.Node.Generate(1)
	if err != nil {
		t.Fatalf("unable to generate block: %v", err)
	}

	// Register a confirmation notification with the chainntfn source for tx2,
	// which is included in the last block. The height hint is the height before
	// the block is included. This notification should fire immediately since
	// only 1 confirmation is required.
	ntfn1, err := notifier.RegisterConfirmationsNtfn(txid1, 1,
		uint32(currentHeight))
	if err != nil {
		t.Fatalf("unable to register ntfn: %v", err)
	}

	select {
	case confInfo := <-ntfn1.Confirmed:
		// Finally, we'll verify that the tx index returned is the exact same
		// as the tx index of the transaction within the block itself.
		msgBlock, err := miner.Node.GetBlock(blockHash[0])
		if err != nil {
			t.Fatalf("unable to fetch block: %v", err)
		}
		block := btcutil.NewBlock(msgBlock)
		specifiedTxHash, err := block.TxHash(int(confInfo.TxIndex))
		if err != nil {
			t.Fatalf("unable to index into block: %v", err)
		}
		if !specifiedTxHash.IsEqual(txid1) {
			t.Fatalf("mismatched tx indexes: expected %v, got %v",
				txid1, specifiedTxHash)
		}

		// We'll also ensure that the block height has been set
		// properly.
		if confInfo.BlockHeight != uint32(currentHeight+1) {
			t.Fatalf("incorrect block height: expected %v, got %v",
				confInfo.BlockHeight, currentHeight)
		}
		break
	case <-time.After(20 * time.Second):
		t.Fatalf("confirmation notification never received")
	}

	// Register a confirmation notification for tx2, requiring 3 confirmations.
	// This transaction is only partially confirmed, so the notification should
	// not fire yet.
	ntfn2, err := notifier.RegisterConfirmationsNtfn(txid2, 3,
		uint32(currentHeight))
	if err != nil {
		t.Fatalf("unable to register ntfn: %v", err)
	}

	// Fully confirm tx3.
	_, err = miner.Node.Generate(2)
	if err != nil {
		t.Fatalf("unable to generate block: %v", err)
	}

	select {
	case <-ntfn2.Confirmed:
	case <-time.After(10 * time.Second):
		t.Fatalf("confirmation notification never received")
	}

	select {
	case <-ntfn1.Confirmed:
		t.Fatalf("received multiple confirmations for tx")
	case <-time.After(1 * time.Second):
	}

	// Finally register a confirmation notification for tx3, requiring 1
	// confirmation. Ensure that conf notifications do not refire on txs
	// 1 or 2.
	ntfn3, err := notifier.RegisterConfirmationsNtfn(txid3, 1,
		uint32(currentHeight-1))
	if err != nil {
		t.Fatalf("unable to register ntfn: %v", err)
	}

	select {
	case <-ntfn3.Confirmed:
	case <-time.After(10 * time.Second):
		t.Fatalf("confirmation notification never received")
	}

	time.Sleep(1 * time.Second)

	select {
	case <-ntfn1.Confirmed:
		t.Fatalf("received multiple confirmations for tx")
	default:
	}

	select {
	case <-ntfn2.Confirmed:
		t.Fatalf("received multiple confirmations for tx")
	default:
	}
}

// Test the case of a notification consumer having forget or being delayed in
// checking for a confirmation. This should not cause the notifier to stop
// working
func testLazyNtfnConsumer(miner *rpctest.Harness,
	notifier chainntnfs.ChainNotifier, t *testing.T) {

	// Create a transaction to be notified about. We'll register for
	// notifications on this transaction but won't be prompt in checking them
	txid, err := getTestTxId(miner)
	if err != nil {
		t.Fatalf("unable to create test tx: %v", err)
	}

	err = waitForMempoolTx(miner, txid)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	_, currentHeight, err := miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	numConfs := uint32(3)

	// Add a block right before registering, this makes race conditions
	// between the historical dispatcher and the normal dispatcher more obvious
	if _, err := miner.Node.Generate(1); err != nil {
		t.Fatalf("unable to generate blocks: %v", err)
	}

	firstConfIntent, err := notifier.RegisterConfirmationsNtfn(txid, numConfs,
		uint32(currentHeight))
	if err != nil {
		t.Fatalf("unable to register ntfn: %v", err)
	}

	// Generate another 2 blocks, this should dispatch the confirm notification
	if _, err := miner.Node.Generate(2); err != nil {
		t.Fatalf("unable to generate blocks: %v", err)
	}

	// Now make another transaction, just because we haven't checked to see
	// if the first transaction has confirmed doesn't mean that we shouldn't
	// be able to see if this transaction confirms first
	txid, err = getTestTxId(miner)
	if err != nil {
		t.Fatalf("unable to create test tx: %v", err)
	}

	err = waitForMempoolTx(miner, txid)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	_, currentHeight, err = miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	numConfs = 1

	secondConfIntent, err := notifier.RegisterConfirmationsNtfn(txid, numConfs,
		uint32(currentHeight))

	if err != nil {
		t.Fatalf("unable to register ntfn: %v", err)
	}

	if _, err := miner.Node.Generate(1); err != nil {
		t.Fatalf("unable to generate blocks: %v", err)
	}

	select {
	case <-secondConfIntent.Confirmed:
		// Successfully receive the second notification
		break
	case <-time.After(30 * time.Second):
		t.Fatalf("Second confirmation notification never received")
	}

	// Make sure the first tx confirmed successfully
	select {
	case <-firstConfIntent.Confirmed:
		break
	case <-time.After(30 * time.Second):
		t.Fatalf("First confirmation notification never received")
	}
}

// Tests the case in which a spend notification is requested for a spend that
// has already been included in a block. In this case, the spend notification
// should be dispatched immediately.
func testSpendBeforeNtfnRegistration(miner *rpctest.Harness,
	notifier chainntnfs.ChainNotifier, t *testing.T) {

	// We'd like to test the spend notifications for all ChainNotifier
	// concrete implementations.
	//
	// To do so, we first create a new output to our test target address.
	txid, err := getTestTxId(miner)
	if err != nil {
		t.Fatalf("unable to create test addr: %v", err)
	}

	err = waitForMempoolTx(miner, txid)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	// Mine a single block which should include that txid above.
	if _, err := miner.Node.Generate(1); err != nil {
		t.Fatalf("unable to generate single block: %v", err)
	}

	// Now that we have the txid, fetch the transaction itself.
	wrappedTx, err := miner.Node.GetRawTransaction(txid)
	if err != nil {
		t.Fatalf("unable to get new tx: %v", err)
	}
	tx := wrappedTx.MsgTx()

	// Locate the output index sent to us. We need this so we can construct
	// a spending txn below.
	outIndex := -1
	var pkScript []byte
	for i, txOut := range tx.TxOut {
		if bytes.Contains(txOut.PkScript, testAddr.ScriptAddress()) {
			pkScript = txOut.PkScript
			outIndex = i
			break
		}
	}
	if outIndex == -1 {
		t.Fatalf("unable to locate new output")
	}

	// Now that we've found the output index, register for a spentness
	// notification for the newly created output.
	outpoint := wire.NewOutPoint(txid, uint32(outIndex))

	// Next, create a new transaction spending that output.
	spendingTx := wire.NewMsgTx(1)
	spendingTx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: *outpoint,
	})
	spendingTx.AddTxOut(&wire.TxOut{
		Value:    1e8,
		PkScript: pkScript,
	})
	sigScript, err := txscript.SignatureScript(spendingTx, 0, pkScript,
		txscript.SigHashAll, privKey, true)
	if err != nil {
		t.Fatalf("unable to sign tx: %v", err)
	}
	spendingTx.TxIn[0].SignatureScript = sigScript

	// Broadcast our spending transaction.
	spenderSha, err := miner.Node.SendRawTransaction(spendingTx, true)
	if err != nil {
		t.Fatalf("unable to broadcast tx: %v", err)
	}

	err = waitForMempoolTx(miner, spenderSha)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	// We create an epoch client we can use to make sure the notifier is
	// caught up to the mining node's chain.
	epochClient, err := notifier.RegisterBlockEpochNtfn()
	if err != nil {
		t.Fatalf("unable to register for block epoch: %v", err)
	}

	// Now we mine an additional block, which should include our spend.
	if _, err := miner.Node.Generate(1); err != nil {
		t.Fatalf("unable to generate single block: %v", err)
	}

	_, currentHeight, err := miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	// checkSpends registers two clients to be notified of a spend that has
	// already happened. The notifier should dispatch a spend notification
	// immediately. We register one that also listen for mempool spends,
	// both should be notified the same way, as the spend is already mined.
	checkSpends := func() {
		const numClients = 2
		spendClients := make([]*chainntnfs.SpendEvent, numClients)
		for i := 0; i < numClients; i++ {
			spentIntent, err := notifier.RegisterSpendNtfn(outpoint,
				uint32(currentHeight), i%2 == 0)
			if err != nil {
				t.Fatalf("unable to register for spend ntfn: %v",
					err)
			}

			spendClients[i] = spentIntent
		}

		for _, client := range spendClients {
			select {
			case ntfn := <-client.Spend:
				// We've received the spend nftn. So now verify
				// all the fields have been set properly.
				checkNotificationFields(ntfn, outpoint, spenderSha,
					currentHeight, t)
			case <-time.After(30 * time.Second):
				t.Fatalf("spend ntfn never received")
			}
		}
	}

	// Wait for the notifier to have caught up to the mined block.
	select {
	case _, ok := <-epochClient.Epochs:
		if !ok {
			t.Fatalf("epoch channel was closed")
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("did not receive block epoch")
	}

	// Check that the spend clients gets immediately notified for the spend
	// in the previous block.
	checkSpends()

	// Bury the spend even deeper, and do the same check.
	const numBlocks = 10
	if _, err := miner.Node.Generate(numBlocks); err != nil {
		t.Fatalf("unable to generate single block: %v", err)
	}

	// Wait for the notifier to have caught up with the new blocks.
	for i := 0; i < numBlocks; i++ {
		select {
		case _, ok := <-epochClient.Epochs:
			if !ok {
				t.Fatalf("epoch channel was closed")
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("did not receive block epoch")
		}
	}

	// The clients should still be notified immediately.
	checkSpends()
}

func testCancelSpendNtfn(node *rpctest.Harness,
	notifier chainntnfs.ChainNotifier, t *testing.T) {

	// We'd like to test that once a spend notification is registered, it
	// can be cancelled before the notification is dispatched.

	// First, we'll start by creating a new output that we can spend
	// ourselves.
	outpoint, pkScript := createSpendableOutput(node, t)

	_, currentHeight, err := node.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	// Create two clients that each registered to the spend notification.
	// We'll cancel the notification for the first client and leave the
	// notification for the second client enabled.
	const numClients = 2
	spendClients := make([]*chainntnfs.SpendEvent, numClients)
	for i := 0; i < numClients; i++ {
		spentIntent, err := notifier.RegisterSpendNtfn(outpoint,
			uint32(currentHeight), true)
		if err != nil {
			t.Fatalf("unable to register for spend ntfn: %v", err)
		}

		spendClients[i] = spentIntent
	}

	// Next, create a new transaction spending that output.
	spendingTx := createSpendTx(outpoint, pkScript, t)

	// Before we broadcast the spending transaction, we'll cancel the
	// notification of the first client.
	spendClients[1].Cancel()

	// Broadcast our spending transaction.
	spenderSha, err := node.Node.SendRawTransaction(spendingTx, true)
	if err != nil {
		t.Fatalf("unable to broadcast tx: %v", err)
	}

	err = waitForMempoolTx(node, spenderSha)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	// Now we mine a single block, which should include our spend. The
	// notification should also be sent off.
	if _, err := node.Node.Generate(1); err != nil {
		t.Fatalf("unable to generate single block: %v", err)
	}

	// However, the spend notification for the first client should have
	// been dispatched.
	select {
	case ntfn := <-spendClients[0].Spend:
		// We've received the spend nftn. So now verify all the
		// fields have been set properly.
		if *ntfn.SpentOutPoint != *outpoint {
			t.Fatalf("ntfn includes wrong output, reports "+
				"%v instead of %v",
				ntfn.SpentOutPoint, outpoint)
		}
		if !bytes.Equal(ntfn.SpenderTxHash[:], spenderSha[:]) {
			t.Fatalf("ntfn includes wrong spender tx sha, "+
				"reports %v instead of %v",
				ntfn.SpenderTxHash[:], spenderSha[:])
		}
		if ntfn.SpenderInputIndex != 0 {
			t.Fatalf("ntfn includes wrong spending input "+
				"index, reports %v, should be %v",
				ntfn.SpenderInputIndex, 0)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("spend ntfn never received")
	}

	// However, The spend notification of the second client should NOT have
	// been dispatched.
	select {
	case _, ok := <-spendClients[1].Spend:
		if ok {
			t.Fatalf("spend ntfn should have been cancelled")
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("spend ntfn never cancelled")
	}
}

func testCancelEpochNtfn(node *rpctest.Harness, notifier chainntnfs.ChainNotifier,
	t *testing.T) {

	// We'd like to ensure that once a client cancels their block epoch
	// notifications, no further notifications are sent over the channel
	// if/when new blocks come in.
	const numClients = 2

	epochClients := make([]*chainntnfs.BlockEpochEvent, numClients)
	for i := 0; i < numClients; i++ {
		epochClient, err := notifier.RegisterBlockEpochNtfn()
		if err != nil {
			t.Fatalf("unable to register for epoch notification")
		}
		epochClients[i] = epochClient
	}

	// Now before we mine any blocks, cancel the notification for the first
	// epoch client.
	epochClients[0].Cancel()

	// Now mine a single block, this should trigger the logic to dispatch
	// epoch notifications.
	if _, err := node.Node.Generate(1); err != nil {
		t.Fatalf("unable to generate blocks: %v", err)
	}

	// The epoch notification for the first client shouldn't have been
	// dispatched.
	select {
	case _, ok := <-epochClients[0].Epochs:
		if ok {
			t.Fatalf("epoch notification should have been cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("epoch notification not sent")
	}

	// However, the epoch notification for the second client should have
	// been dispatched as normal.
	select {
	case _, ok := <-epochClients[1].Epochs:
		if !ok {
			t.Fatalf("epoch was cancelled")
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("epoch notification not sent")
	}
}

func testReorgConf(miner *rpctest.Harness, notifier chainntnfs.ChainNotifier,
	t *testing.T) {

	// Set up a new miner that we can use to cause a reorg.
	miner2, err := rpctest.New(netParams, nil, nil)
	if err != nil {
		t.Fatalf("unable to create mining node: %v", err)
	}
	if err := miner2.SetUp(false, 0); err != nil {
		t.Fatalf("unable to set up mining node: %v", err)
	}
	defer miner2.TearDown()

	// We start by connecting the new miner to our original miner,
	// such that it will sync to our original chain.
	if err := rpctest.ConnectNode(miner, miner2); err != nil {
		t.Fatalf("unable to connect harnesses: %v", err)
	}
	nodeSlice := []*rpctest.Harness{miner, miner2}
	if err := rpctest.JoinNodes(nodeSlice, rpctest.Blocks); err != nil {
		t.Fatalf("unable to join node on blocks: %v", err)
	}

	// The two should be on the same blockheight.
	_, nodeHeight1, err := miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current blockheight %v", err)
	}

	_, nodeHeight2, err := miner2.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current blockheight %v", err)
	}

	if nodeHeight1 != nodeHeight2 {
		t.Fatalf("expected both miners to be on the same height: %v vs %v",
			nodeHeight1, nodeHeight2)
	}

	// We disconnect the two nodes, such that we can start mining on them
	// individually without the other one learning about the new blocks.
	err = miner.Node.AddNode(miner2.P2PAddress(), rpcclient.ANRemove)
	if err != nil {
		t.Fatalf("unable to remove node: %v", err)
	}

	txid, err := getTestTxId(miner)
	if err != nil {
		t.Fatalf("unable to create test tx: %v", err)
	}

	err = waitForMempoolTx(miner, txid)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	_, currentHeight, err := miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current height: %v", err)
	}

	// Now that we have a txid, register a confirmation notification with
	// the chainntfn source.
	numConfs := uint32(2)
	confIntent, err := notifier.RegisterConfirmationsNtfn(txid, numConfs,
		uint32(currentHeight))
	if err != nil {
		t.Fatalf("unable to register ntfn: %v", err)
	}

	// Now generate a single block, the transaction should be included.
	_, err = miner.Node.Generate(1)
	if err != nil {
		t.Fatalf("unable to generate single block: %v", err)
	}

	// Transaction only has one confirmation, and the notification is registered
	// with 2 confirmations, so we should not be notified yet.
	select {
	case <-confIntent.Confirmed:
		t.Fatal("tx was confirmed unexpectedly")
	case <-time.After(1 * time.Second):
	}

	// Reorganize transaction out of the chain by generating a longer fork
	// from the other miner. The transaction is not included in this fork.
	miner2.Node.Generate(2)

	// Reconnect nodes to reach consensus on the longest chain. miner2's chain
	// should win and become active on miner1.
	if err := rpctest.ConnectNode(miner, miner2); err != nil {
		t.Fatalf("unable to connect harnesses: %v", err)
	}
	nodeSlice = []*rpctest.Harness{miner, miner2}
	if err := rpctest.JoinNodes(nodeSlice, rpctest.Blocks); err != nil {
		t.Fatalf("unable to join node on blocks: %v", err)
	}

	_, nodeHeight1, err = miner.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current blockheight %v", err)
	}

	_, nodeHeight2, err = miner2.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get current blockheight %v", err)
	}

	if nodeHeight1 != nodeHeight2 {
		t.Fatalf("expected both miners to be on the same height: %v vs %v",
			nodeHeight1, nodeHeight2)
	}

	// Even though there is one block above the height of the block that the
	// transaction was included in, it is not the active chain so the
	// notification should not be sent.
	select {
	case <-confIntent.Confirmed:
		t.Fatal("tx was confirmed unexpectedly")
	case <-time.After(1 * time.Second):
	}

	// Now confirm the transaction on the longest chain and verify that we
	// receive the notification.
	tx, err := miner.Node.GetRawTransaction(txid)
	if err != nil {
		t.Fatalf("unable to get raw tx: %v", err)
	}

	txid, err = miner2.Node.SendRawTransaction(tx.MsgTx(), false)
	if err != nil {
		t.Fatalf("unable to get send tx: %v", err)
	}

	err = waitForMempoolTx(miner, txid)
	if err != nil {
		t.Fatalf("tx not relayed to miner: %v", err)
	}

	_, err = miner.Node.Generate(3)
	if err != nil {
		t.Fatalf("unable to generate single block: %v", err)
	}

	select {
	case <-confIntent.Confirmed:
	case <-time.After(20 * time.Second):
		t.Fatalf("confirmation notification never received")
	}
}

type testCase struct {
	name string

	test func(node *rpctest.Harness, notifier chainntnfs.ChainNotifier, t *testing.T)
}

var ntfnTests = []testCase{
	{
		name: "single conf ntfn",
		test: testSingleConfirmationNotification,
	},
	{
		name: "multi conf ntfn",
		test: testMultiConfirmationNotification,
	},
	{
		name: "batch conf ntfn",
		test: testBatchConfirmationNotification,
	},
	{
		name: "multi client conf",
		test: testMultiClientConfirmationNotification,
	},
	{
		name: "spend ntfn",
		test: testSpendNotification,
	},
	{
		name: "spend ntfn mempool",
		test: testSpendNotificationMempoolSpends,
	},
	{
		name: "block epoch",
		test: testBlockEpochNotification,
	},
	{
		name: "historical conf dispatch",
		test: testTxConfirmedBeforeNtfnRegistration,
	},
	{
		name: "historical spend dispatch",
		test: testSpendBeforeNtfnRegistration,
	},
	{
		name: "cancel spend ntfn",
		test: testCancelSpendNtfn,
	},
	{
		name: "cancel epoch ntfn",
		test: testCancelEpochNtfn,
	},
	{
		name: "lazy ntfn consumer",
		test: testLazyNtfnConsumer,
	},
	{
		name: "reorg conf",
		test: testReorgConf,
	},
}

// TestInterfaces tests all registered interfaces with a unified set of tests
// which exercise each of the required methods found within the ChainNotifier
// interface.
//
// NOTE: In the future, when additional implementations of the ChainNotifier
// interface have been implemented, in order to ensure the new concrete
// implementation is automatically tested, two steps must be undertaken. First,
// one needs add a "non-captured" (_) import from the new sub-package. This
// import should trigger an init() method within the package which registers
// the interface. Second, an additional case in the switch within the main loop
// below needs to be added which properly initializes the interface.
func TestInterfaces(t *testing.T) {
	// Initialize the harness around a btcd node which will serve as our
	// dedicated miner to generate blocks, cause re-orgs, etc. We'll set up
	// this node with a chain length of 125, so we have plenty of BTC to
	// play around with.
	miner, err := rpctest.New(netParams, nil, nil)
	if err != nil {
		t.Fatalf("unable to create mining node: %v", err)
	}
	defer miner.TearDown()
	if err := miner.SetUp(true, 25); err != nil {
		t.Fatalf("unable to set up mining node: %v", err)
	}

	rpcConfig := miner.RPCConfig()
	p2pAddr := miner.P2PAddress()

	log.Printf("Running %v ChainNotifier interface tests\n", len(ntfnTests))
	var (
		notifier chainntnfs.ChainNotifier
		cleanUp  func()
	)
	for _, notifierDriver := range chainntnfs.RegisteredNotifiers() {
		notifierType := notifierDriver.NotifierType

		switch notifierType {

		case "bitcoind":
			// Start a bitcoind instance.
			tempBitcoindDir, err := ioutil.TempDir("", "bitcoind")
			if err != nil {
				t.Fatalf("Unable to create temp dir: %v", err)
			}
			zmqPath := "ipc:///" + tempBitcoindDir + "/weks.socket"
			cleanUp1 := func() {
				os.RemoveAll(tempBitcoindDir)
			}
			cleanUp = cleanUp1
			rpcPort := rand.Int()%(65536-1024) + 1024
			bitcoind := exec.Command(
				"bitcoind",
				"-datadir="+tempBitcoindDir,
				"-regtest",
				"-connect="+p2pAddr,
				"-txindex",
				"-rpcauth=weks:469e9bb14ab2360f8e226efed5ca6f"+
					"d$507c670e800a95284294edb5773b05544b"+
					"220110063096c221be9933c82d38e1",
				fmt.Sprintf("-rpcport=%d", rpcPort),
				"-disablewallet",
				"-zmqpubrawblock="+zmqPath,
				"-zmqpubrawtx="+zmqPath,
			)
			err = bitcoind.Start()
			if err != nil {
				cleanUp1()
				t.Fatalf("Couldn't start bitcoind: %v", err)
			}
			cleanUp2 := func() {
				bitcoind.Process.Kill()
				bitcoind.Wait()
				cleanUp1()
			}
			cleanUp = cleanUp2

			// Wait for the bitcoind instance to start up.
			time.Sleep(time.Second)

			// Start the FilteredChainView implementation instance.
			config := rpcclient.ConnConfig{
				Host: fmt.Sprintf(
					"127.0.0.1:%d", rpcPort),
				User:                 "weks",
				Pass:                 "weks",
				DisableAutoReconnect: false,
				DisableConnectOnNew:  true,
				DisableTLS:           true,
				HTTPPostMode:         true,
			}

			notifier, err = notifierDriver.New(&config, zmqPath,
				*netParams)
			if err != nil {
				t.Fatalf("unable to create %v notifier: %v",
					notifierType, err)
			}

		case "btcd":
			notifier, err = notifierDriver.New(&rpcConfig)
			if err != nil {
				t.Fatalf("unable to create %v notifier: %v",
					notifierType, err)
			}
			cleanUp = func() {}

		case "neutrino":
			spvDir, err := ioutil.TempDir("", "neutrino")
			if err != nil {
				t.Fatalf("unable to create temp dir: %v", err)
			}

			dbName := filepath.Join(spvDir, "neutrino.db")
			spvDatabase, err := walletdb.Create("bdb", dbName)
			if err != nil {
				t.Fatalf("unable to create walletdb: %v", err)
			}

			// Create an instance of neutrino connected to the
			// running btcd instance.
			spvConfig := neutrino.Config{
				DataDir:      spvDir,
				Database:     spvDatabase,
				ChainParams:  *netParams,
				ConnectPeers: []string{p2pAddr},
			}
			neutrino.WaitForMoreCFHeaders = 250 * time.Millisecond
			spvNode, err := neutrino.NewChainService(spvConfig)
			if err != nil {
				t.Fatalf("unable to create neutrino: %v", err)
			}
			spvNode.Start()

			cleanUp = func() {
				spvNode.Stop()
				spvDatabase.Close()
				os.RemoveAll(spvDir)
			}

			// We'll also wait for the instance to sync up fully to
			// the chain generated by the btcd instance.
			for !spvNode.IsCurrent() {
				time.Sleep(time.Millisecond * 100)
			}

			notifier, err = notifierDriver.New(spvNode)
			if err != nil {
				t.Fatalf("unable to create %v notifier: %v",
					notifierType, err)
			}
		}

		t.Logf("Running ChainNotifier interface tests for: %v", notifierType)

		if err := notifier.Start(); err != nil {
			t.Fatalf("unable to start notifier %v: %v",
				notifierType, err)
		}

		for _, ntfnTest := range ntfnTests {
			testName := fmt.Sprintf("%v: %v", notifierType,
				ntfnTest.name)

			success := t.Run(testName, func(t *testing.T) {
				ntfnTest.test(miner, notifier, t)
			})

			if !success {
				break
			}
		}

		notifier.Stop()
		if cleanUp != nil {
			cleanUp()
		}
		cleanUp = nil
	}
}
