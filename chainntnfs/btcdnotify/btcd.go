package btcdnotify

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/breez/lightninglib/chainntnfs"
	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
)

const (

	// notifierType uniquely identifies this concrete implementation of the
	// ChainNotifier interface.
	notifierType = "btcd"

	// reorgSafetyLimit is assumed maximum depth of a chain reorganization.
	// After this many confirmation, transaction confirmation info will be
	// pruned.
	reorgSafetyLimit = 100
)

var (
	// ErrChainNotifierShuttingDown is used when we are trying to
	// measure a spend notification when notifier is already stopped.
	ErrChainNotifierShuttingDown = errors.New("chainntnfs: system interrupt " +
		"while attempting to register for spend notification.")
)

// chainUpdate encapsulates an update to the current main chain. This struct is
// used as an element within an unbounded queue in order to avoid blocking the
// main rpc dispatch rule.
type chainUpdate struct {
	blockHash   *chainhash.Hash
	blockHeight int32

	// connected is true if this update is a new block and false if it is a
	// disconnected block.
	connect bool
}

// txUpdate encapsulates a transaction related notification sent from btcd to
// the registered RPC client. This struct is used as an element within an
// unbounded queue in order to avoid blocking the main rpc dispatch rule.
type txUpdate struct {
	tx      *btcutil.Tx
	details *btcjson.BlockDetails
}

// TODO(roasbeef): generalize struct below:
//  * move chans to config, allow outside callers to handle send conditions

// BtcdNotifier implements the ChainNotifier interface using btcd's websockets
// notifications. Multiple concurrent clients are supported. All notifications
// are achieved via non-blocking sends on client channels.
type BtcdNotifier struct {
	spendClientCounter uint64 // To be used atomically.
	epochClientCounter uint64 // To be used atomically.

	started int32 // To be used atomically.
	stopped int32 // To be used atomically.

	chainConn *rpcclient.Client

	notificationCancels  chan interface{}
	notificationRegistry chan interface{}

	spendNotifications map[wire.OutPoint]map[uint64]*spendNotification

	txConfNotifier *chainntnfs.TxConfNotifier

	blockEpochClients map[uint64]*blockEpochRegistration

	chainUpdates *chainntnfs.ConcurrentQueue
	txUpdates    *chainntnfs.ConcurrentQueue

	wg   sync.WaitGroup
	quit chan struct{}
}

// Ensure BtcdNotifier implements the ChainNotifier interface at compile time.
var _ chainntnfs.ChainNotifier = (*BtcdNotifier)(nil)

// New returns a new BtcdNotifier instance. This function assumes the btcd node
// detailed in the passed configuration is already running, and willing to
// accept new websockets clients.
func New(config *rpcclient.ConnConfig) (*BtcdNotifier, error) {
	notifier := &BtcdNotifier{
		notificationCancels:  make(chan interface{}),
		notificationRegistry: make(chan interface{}),

		blockEpochClients: make(map[uint64]*blockEpochRegistration),

		spendNotifications: make(map[wire.OutPoint]map[uint64]*spendNotification),

		chainUpdates: chainntnfs.NewConcurrentQueue(10),
		txUpdates:    chainntnfs.NewConcurrentQueue(10),

		quit: make(chan struct{}),
	}

	ntfnCallbacks := &rpcclient.NotificationHandlers{
		OnBlockConnected:    notifier.onBlockConnected,
		OnBlockDisconnected: notifier.onBlockDisconnected,
		OnRedeemingTx:       notifier.onRedeemingTx,
	}

	// Disable connecting to btcd within the rpcclient.New method. We
	// defer establishing the connection to our .Start() method.
	config.DisableConnectOnNew = true
	config.DisableAutoReconnect = false
	chainConn, err := rpcclient.New(config, ntfnCallbacks)
	if err != nil {
		return nil, err
	}
	notifier.chainConn = chainConn

	return notifier, nil
}

// Start connects to the running btcd node over websockets, registers for block
// notifications, and finally launches all related helper goroutines.
func (b *BtcdNotifier) Start() error {
	// Already started?
	if atomic.AddInt32(&b.started, 1) != 1 {
		return nil
	}

	// Connect to btcd, and register for notifications on connected, and
	// disconnected blocks.
	if err := b.chainConn.Connect(20); err != nil {
		return err
	}
	if err := b.chainConn.NotifyBlocks(); err != nil {
		return err
	}

	_, currentHeight, err := b.chainConn.GetBestBlock()
	if err != nil {
		return err
	}

	b.txConfNotifier = chainntnfs.NewTxConfNotifier(
		uint32(currentHeight), reorgSafetyLimit)

	b.chainUpdates.Start()
	b.txUpdates.Start()

	b.wg.Add(1)
	go b.notificationDispatcher(currentHeight)

	return nil
}

// Stop shutsdown the BtcdNotifier.
func (b *BtcdNotifier) Stop() error {
	// Already shutting down?
	if atomic.AddInt32(&b.stopped, 1) != 1 {
		return nil
	}

	// Shutdown the rpc client, this gracefully disconnects from btcd, and
	// cleans up all related resources.
	b.chainConn.Shutdown()

	close(b.quit)
	b.wg.Wait()

	b.chainUpdates.Stop()
	b.txUpdates.Stop()

	// Notify all pending clients of our shutdown by closing the related
	// notification channels.
	for _, spendClients := range b.spendNotifications {
		for _, spendClient := range spendClients {
			close(spendClient.spendChan)
		}
	}
	for _, epochClient := range b.blockEpochClients {
		close(epochClient.cancelChan)
		epochClient.wg.Wait()

		close(epochClient.epochChan)
	}
	b.txConfNotifier.TearDown()

	return nil
}

// onBlockConnected implements on OnBlockConnected callback for rpcclient.
// Ingesting a block updates the wallet's internal utxo state based on the
// outputs created and destroyed within each block.
func (b *BtcdNotifier) onBlockConnected(hash *chainhash.Hash, height int32, t time.Time) {
	// Append this new chain update to the end of the queue of new chain
	// updates.
	b.chainUpdates.ChanIn() <- &chainUpdate{
		blockHash:   hash,
		blockHeight: height,
		connect:     true,
	}
}

// filteredBlock represents a new block which has been connected to the main
// chain. The slice of transactions will only be populated if the block
// includes a transaction that confirmed one of our watched txids, or spends
// one of the outputs currently being watched.
// TODO(halseth): this is currently used for complete blocks. Change to use
// onFilteredBlockConnected and onFilteredBlockDisconnected, making it easier
// to unify with the Neutrino implementation.
type filteredBlock struct {
	hash   chainhash.Hash
	height uint32
	txns   []*btcutil.Tx

	// connected is true if this update is a new block and false if it is a
	// disconnected block.
	connect bool
}

// onBlockDisconnected implements on OnBlockDisconnected callback for rpcclient.
func (b *BtcdNotifier) onBlockDisconnected(hash *chainhash.Hash, height int32, t time.Time) {
	// Append this new chain update to the end of the queue of new chain
	// updates.
	b.chainUpdates.ChanIn() <- &chainUpdate{
		blockHash:   hash,
		blockHeight: height,
		connect:     false,
	}
}

// onRedeemingTx implements on OnRedeemingTx callback for rpcclient.
func (b *BtcdNotifier) onRedeemingTx(tx *btcutil.Tx, details *btcjson.BlockDetails) {
	// Append this new transaction update to the end of the queue of new
	// chain updates.
	b.txUpdates.ChanIn() <- &txUpdate{tx, details}
}

// notificationDispatcher is the primary goroutine which handles client
// notification registrations, as well as notification dispatches.
func (b *BtcdNotifier) notificationDispatcher(currentHeight int32) {
out:
	for {
		select {
		case cancelMsg := <-b.notificationCancels:
			switch msg := cancelMsg.(type) {
			case *spendCancel:
				chainntnfs.Log.Infof("Cancelling spend "+
					"notification for out_point=%v, "+
					"spend_id=%v", msg.op, msg.spendID)

				// Before we attempt to close the spendChan,
				// ensure that the notification hasn't already
				// yet been dispatched.
				if outPointClients, ok := b.spendNotifications[msg.op]; ok {
					close(outPointClients[msg.spendID].spendChan)
					delete(b.spendNotifications[msg.op], msg.spendID)
				}

			case *epochCancel:
				chainntnfs.Log.Infof("Cancelling epoch "+
					"notification, epoch_id=%v", msg.epochID)

				// First, we'll lookup the original
				// registration in order to stop the active
				// queue goroutine.
				reg := b.blockEpochClients[msg.epochID]
				reg.epochQueue.Stop()

				// Next, close the cancel channel for this
				// specific client, and wait for the client to
				// exit.
				close(b.blockEpochClients[msg.epochID].cancelChan)
				b.blockEpochClients[msg.epochID].wg.Wait()

				// Once the client has exited, we can then
				// safely close the channel used to send epoch
				// notifications, in order to notify any
				// listeners that the intent has been
				// cancelled.
				close(b.blockEpochClients[msg.epochID].epochChan)
				delete(b.blockEpochClients, msg.epochID)
			}
		case registerMsg := <-b.notificationRegistry:
			switch msg := registerMsg.(type) {
			case *spendNotification:
				chainntnfs.Log.Infof("New spend subscription: "+
					"utxo=%v", msg.targetOutpoint)
				op := *msg.targetOutpoint

				if _, ok := b.spendNotifications[op]; !ok {
					b.spendNotifications[op] = make(map[uint64]*spendNotification)
				}
				b.spendNotifications[op][msg.spendID] = msg
			case *confirmationNotification:
				chainntnfs.Log.Infof("New confirmation "+
					"subscription: txid=%v, numconfs=%v",
					msg.TxID, msg.NumConfirmations)

				// Lookup whether the transaction is already included in the
				// active chain.
				txConf, err := b.historicalConfDetails(
					msg.TxID, msg.heightHint, uint32(currentHeight),
				)
				if err != nil {
					chainntnfs.Log.Error(err)
				}

				err = b.txConfNotifier.Register(&msg.ConfNtfn, txConf)
				if err != nil {
					chainntnfs.Log.Error(err)
				}
			case *blockEpochRegistration:
				chainntnfs.Log.Infof("New block epoch subscription")
				b.blockEpochClients[msg.epochID] = msg
			}

		case item := <-b.chainUpdates.ChanOut():
			update := item.(*chainUpdate)
			if update.connect {
				if update.blockHeight != currentHeight+1 {
					chainntnfs.Log.Warnf("Received blocks out of order: "+
						"current height=%d, new height=%d",
						currentHeight, update.blockHeight)
					continue
				}

				currentHeight = update.blockHeight

				rawBlock, err := b.chainConn.GetBlock(update.blockHash)
				if err != nil {
					chainntnfs.Log.Errorf("Unable to get block: %v", err)
					continue
				}

				chainntnfs.Log.Infof("New block: height=%v, sha=%v",
					update.blockHeight, update.blockHash)

				txns := btcutil.NewBlock(rawBlock).Transactions()

				block := &filteredBlock{
					hash:    *update.blockHash,
					height:  uint32(update.blockHeight),
					txns:    txns,
					connect: true,
				}
				if err := b.handleBlockConnected(block); err != nil {
					chainntnfs.Log.Error(err)
				}
				continue
			}

			if update.blockHeight != currentHeight {
				chainntnfs.Log.Warnf("Received blocks out of order: "+
					"current height=%d, disconnected height=%d",
					currentHeight, update.blockHeight)
				continue
			}

			currentHeight = update.blockHeight - 1

			chainntnfs.Log.Infof("Block disconnected from main chain: "+
				"height=%v, sha=%v", update.blockHeight, update.blockHash)

			err := b.txConfNotifier.DisconnectTip(uint32(update.blockHeight))
			if err != nil {
				chainntnfs.Log.Error(err)
			}

		// NOTE: we currently only use txUpdates for mempool spends and
		// rescan spends. It might get removed entirely in the future.
		case item := <-b.txUpdates.ChanOut():
			newSpend := item.(*txUpdate)

			// We only care about notifying on confirmed spends, so
			// in case this is a mempool spend, we can continue,
			// and wait for the spend to appear in chain.
			if newSpend.details == nil {
				continue
			}

			spendingTx := newSpend.tx

			// First, check if this transaction spends an output
			// that has an existing spend notification for it.
			for i, txIn := range spendingTx.MsgTx().TxIn {
				prevOut := txIn.PreviousOutPoint

				// If this transaction indeed does spend an
				// output which we have a registered
				// notification for, then create a spend
				// summary, finally sending off the details to
				// the notification subscriber.
				if clients, ok := b.spendNotifications[prevOut]; ok {
					spenderSha := newSpend.tx.Hash()
					spendDetails := &chainntnfs.SpendDetail{
						SpentOutPoint:     &prevOut,
						SpenderTxHash:     spenderSha,
						SpendingTx:        spendingTx.MsgTx(),
						SpenderInputIndex: uint32(i),
					}
					spendDetails.SpendingHeight = newSpend.details.Height

					for _, ntfn := range clients {
						chainntnfs.Log.Infof("Dispatching "+
							"confirmed spend "+
							"notification for "+
							"outpoint=%v at height %v",
							ntfn.targetOutpoint,
							spendDetails.SpendingHeight)
						ntfn.spendChan <- spendDetails

						// Close spendChan to ensure
						// that any calls to Cancel
						// will not block. This is safe
						// to do since the channel is
						// buffered, and the message
						// can still be read by the
						// receiver.
						close(ntfn.spendChan)
					}
					delete(b.spendNotifications, prevOut)
				}
			}

		case <-b.quit:
			break out
		}
	}
	b.wg.Done()
}

// historicalConfDetails looks up whether a transaction is already included in a
// block in the active chain and, if so, returns details about the confirmation.
func (b *BtcdNotifier) historicalConfDetails(txid *chainhash.Hash,
	heightHint, currentHeight uint32) (*chainntnfs.TxConfirmation, error) {

	// First, we'll attempt to retrieve the transaction details using the
	// backend node's transaction index.
	txConf, err := b.confDetailsFromTxIndex(txid)
	if err != nil {
		return nil, err
	}

	if txConf != nil {
		return txConf, nil
	}

	// If the backend node's transaction index is not enabled, then we'll
	// fall back to manually scanning the chain's blocks, looking for the
	// block where the transaction was included in.
	return b.confDetailsManually(txid, heightHint, currentHeight)
}

// confDetailsFromTxIndex looks up whether a transaction is already included
// in a block in the active chain by using the backend node's transaction index.
// If the transaction is found, its confirmation details are returned.
// Otherwise, nil is returned.
func (b *BtcdNotifier) confDetailsFromTxIndex(txid *chainhash.Hash,
) (*chainntnfs.TxConfirmation, error) {

	// If the transaction has some or all of its confirmations required,
	// then we may be able to dispatch it immediately.
	tx, err := b.chainConn.GetRawTransactionVerbose(txid)
	if err != nil {
		// Avoid returning an error if the transaction index is not
		// enabled to proceed with fallback methods.
		jsonErr, ok := err.(*btcjson.RPCError)
		if !ok || jsonErr.Code != btcjson.ErrRPCNoTxInfo {
			return nil, fmt.Errorf("unable to query for txid "+
				"%v: %v", txid, err)
		}
	}

	// Make sure we actually retrieved a transaction that is included in a
	// block. Without this, we won't be able to retrieve its confirmation
	// details.
	if tx == nil || tx.BlockHash == "" {
		return nil, nil
	}

	// As we need to fully populate the returned TxConfirmation struct,
	// grab the block in which the transaction was confirmed so we can
	// locate its exact index within the block.
	blockHash, err := chainhash.NewHashFromStr(tx.BlockHash)
	if err != nil {
		return nil, fmt.Errorf("unable to get block hash %v for "+
			"historical dispatch: %v", tx.BlockHash, err)
	}

	block, err := b.chainConn.GetBlockVerbose(blockHash)
	if err != nil {
		return nil, fmt.Errorf("unable to get block with hash %v for "+
			"historical dispatch: %v", blockHash, err)
	}

	// If the block was obtained, locate the transaction's index within the
	// block so we can give the subscriber full confirmation details.
	targetTxidStr := txid.String()
	for txIndex, txHash := range block.Tx {
		if txHash == targetTxidStr {
			return &chainntnfs.TxConfirmation{
				BlockHash:   blockHash,
				BlockHeight: uint32(block.Height),
				TxIndex:     uint32(txIndex),
			}, nil
		}
	}

	// We return an error because we should have found the transaction
	// within the block, but didn't.
	return nil, fmt.Errorf("unable to locate tx %v in block %v", txid,
		blockHash)
}

// confDetailsManually looks up whether a transaction is already included in a
// block in the active chain by scanning the chain's blocks, starting from the
// earliest height the transaction could have been included in, to the current
// height in the chain. If the transaction is found, its confirmation details
// are returned. Otherwise, nil is returned.
func (b *BtcdNotifier) confDetailsManually(txid *chainhash.Hash,
	heightHint, currentHeight uint32) (*chainntnfs.TxConfirmation, error) {

	targetTxidStr := txid.String()

	// Begin scanning blocks at every height to determine where the
	// transaction was included in.
	for height := heightHint; height <= currentHeight; height++ {
		blockHash, err := b.chainConn.GetBlockHash(int64(height))
		if err != nil {
			return nil, fmt.Errorf("unable to get hash from block "+
				"with height %d", height)
		}

		// TODO: fetch the neutrino filters instead.
		block, err := b.chainConn.GetBlockVerbose(blockHash)
		if err != nil {
			return nil, fmt.Errorf("unable to get block with hash "+
				"%v: %v", blockHash, err)
		}

		for txIndex, txHash := range block.Tx {
			// If we're able to find the transaction in this block,
			// return its confirmation details.
			if txHash == targetTxidStr {
				return &chainntnfs.TxConfirmation{
					BlockHash:   blockHash,
					BlockHeight: height,
					TxIndex:     uint32(txIndex),
				}, nil
			}
		}
	}

	// If we reach here, then we were not able to find the transaction
	// within a block, so we avoid returning an error.
	return nil, nil
}

// handleBlocksConnected applies a chain update for a new block. Any watched
// transactions included this block will processed to either send notifications
// now or after numConfirmations confs.
// TODO(halseth): this is reusing the neutrino notifier implementation, unify
// them.
func (b *BtcdNotifier) handleBlockConnected(newBlock *filteredBlock) error {
	// First we'll notify any subscribed clients of the block.
	b.notifyBlockEpochs(int32(newBlock.height), &newBlock.hash)

	// Next, we'll scan over the list of relevant transactions and possibly
	// dispatch notifications for confirmations and spends.
	for _, tx := range newBlock.txns {
		mtx := tx.MsgTx()
		txSha := mtx.TxHash()

		for i, txIn := range mtx.TxIn {
			prevOut := txIn.PreviousOutPoint

			// If this transaction indeed does spend an output which we have a
			// registered notification for, then create a spend summary, finally
			// sending off the details to the notification subscriber.
			clients, ok := b.spendNotifications[prevOut]
			if !ok {
				continue
			}

			spendDetails := &chainntnfs.SpendDetail{
				SpentOutPoint:     &prevOut,
				SpenderTxHash:     &txSha,
				SpendingTx:        mtx,
				SpenderInputIndex: uint32(i),
				SpendingHeight:    int32(newBlock.height),
			}

			for _, ntfn := range clients {
				chainntnfs.Log.Infof("Dispatching spend notification for "+
					"outpoint=%v", ntfn.targetOutpoint)
				ntfn.spendChan <- spendDetails

				// Close spendChan to ensure that any calls to Cancel will not
				// block. This is safe to do since the channel is buffered, and
				// the message can still be read by the receiver.
				close(ntfn.spendChan)
			}

			delete(b.spendNotifications, prevOut)
		}
	}

	// A new block has been connected to the main chain.
	// Send out any N confirmation notifications which may
	// have been triggered by this new block.
	b.txConfNotifier.ConnectTip(&newBlock.hash, newBlock.height, newBlock.txns)

	return nil
}

// notifyBlockEpochs notifies all registered block epoch clients of the newly
// connected block to the main chain.
func (b *BtcdNotifier) notifyBlockEpochs(newHeight int32, newSha *chainhash.Hash) {
	epoch := &chainntnfs.BlockEpoch{
		Height: newHeight,
		Hash:   newSha,
	}

	for _, epochClient := range b.blockEpochClients {
		select {

		case epochClient.epochQueue.ChanIn() <- epoch:

		case <-epochClient.cancelChan:

		case <-b.quit:
		}
	}
}

// spendNotification couples a target outpoint along with the channel used for
// notifications once a spend of the outpoint has been detected.
type spendNotification struct {
	targetOutpoint *wire.OutPoint

	spendChan chan *chainntnfs.SpendDetail

	spendID uint64

	heightHint uint32
}

// spendCancel is a message sent to the BtcdNotifier when a client wishes to
// cancel an outstanding spend notification that has yet to be dispatched.
type spendCancel struct {
	// op is the target outpoint of the notification to be cancelled.
	op wire.OutPoint

	// spendID the ID of the notification to cancel.
	spendID uint64
}

// RegisterSpendNtfn registers an intent to be notified once the target
// outpoint has been spent by a transaction on-chain. Once a spend of the target
// outpoint has been detected, the details of the spending event will be sent
// across the 'Spend' channel. The heightHint should represent the earliest
// height in the chain where the transaction could have been spent in.
func (b *BtcdNotifier) RegisterSpendNtfn(outpoint *wire.OutPoint,
	pkScript []byte, heightHint uint32) (*chainntnfs.SpendEvent, error) {

	ntfn := &spendNotification{
		targetOutpoint: outpoint,
		spendChan:      make(chan *chainntnfs.SpendDetail, 1),
		spendID:        atomic.AddUint64(&b.spendClientCounter, 1),
		heightHint:     heightHint,
	}

	select {
	case <-b.quit:
		return nil, ErrChainNotifierShuttingDown
	case b.notificationRegistry <- ntfn:
	}

	// TODO(roasbeef): update btcd rescan logic to also use both
	if err := b.chainConn.NotifySpent([]*wire.OutPoint{outpoint}); err != nil {
		return nil, err
	}

	// The following conditional checks to ensure that when a spend
	// notification is registered, the output hasn't already been spent. If
	// the output is no longer in the UTXO set, the chain will be rescanned
	// from the point where the output was added. The rescan will dispatch
	// the notification.
	txOut, err := b.chainConn.GetTxOut(&outpoint.Hash, outpoint.Index, true)
	if err != nil {
		return nil, err
	}

	if txOut == nil {
		// First, we'll attempt to retrieve the transaction's block hash
		// using the backend's transaction index.
		tx, err := b.chainConn.GetRawTransactionVerbose(&outpoint.Hash)
		if err != nil {
			// Avoid returning an error if the transaction was not
			// found to proceed with fallback methods.
			jsonErr, ok := err.(*btcjson.RPCError)
			if !ok || jsonErr.Code != btcjson.ErrRPCNoTxInfo {
				return nil, fmt.Errorf("unable to query for "+
					"txid %v: %v", outpoint.Hash, err)
			}
		}

		var blockHash *chainhash.Hash
		if tx != nil && tx.BlockHash != "" {
			// If we're able to retrieve a valid block hash from the
			// transaction, then we'll use it as our rescan starting
			// point.
			blockHash, err = chainhash.NewHashFromStr(tx.BlockHash)
			if err != nil {
				return nil, err
			}
		} else {
			// Otherwise, we'll attempt to retrieve the hash for the
			// block at the heightHint.
			blockHash, err = b.chainConn.GetBlockHash(
				int64(heightHint),
			)
			if err != nil {
				return nil, err
			}
		}

		// We'll only request a rescan if the transaction has actually
		// been included within a block. Otherwise, we'll encounter an
		// error when scanning for blocks. This can happen in the case
		// of a race condition, wherein the output itself is unspent,
		// and only arrives in the mempool after the getxout call.
		if blockHash != nil {
			ops := []*wire.OutPoint{outpoint}

			// In order to ensure that we don't block the caller on
			// what may be a long rescan, we'll launch a new
			// goroutine to handle the async result of the rescan.
			asyncResult := b.chainConn.RescanAsync(
				blockHash, nil, ops,
			)
			go func() {
				rescanErr := asyncResult.Receive()
				if rescanErr != nil {
					chainntnfs.Log.Errorf("Rescan for spend "+
						"notification txout(%x) "+
						"failed: %v", outpoint, rescanErr)
				}
			}()
		}
	}

	return &chainntnfs.SpendEvent{
		Spend: ntfn.spendChan,
		Cancel: func() {
			cancel := &spendCancel{
				op:      *outpoint,
				spendID: ntfn.spendID,
			}

			// Submit spend cancellation to notification dispatcher.
			select {
			case b.notificationCancels <- cancel:
				// Cancellation is being handled, drain the spend chan until it is
				// closed before yielding to the caller.
				for {
					select {
					case _, ok := <-ntfn.spendChan:
						if !ok {
							return
						}
					case <-b.quit:
						return
					}
				}
			case <-b.quit:
			}
		},
	}, nil
}

// confirmationNotification represents a client's intent to receive a
// notification once the target txid reaches numConfirmations confirmations.
type confirmationNotification struct {
	chainntnfs.ConfNtfn
	heightHint uint32
}

// RegisterConfirmationsNtfn registers a notification with BtcdNotifier
// which will be triggered once the txid reaches numConfs number of
// confirmations.
func (b *BtcdNotifier) RegisterConfirmationsNtfn(txid *chainhash.Hash, _ []byte,
	numConfs, heightHint uint32) (*chainntnfs.ConfirmationEvent, error) {

	ntfn := &confirmationNotification{
		ConfNtfn: chainntnfs.ConfNtfn{
			TxID:             txid,
			NumConfirmations: numConfs,
			Event:            chainntnfs.NewConfirmationEvent(numConfs),
		},
		heightHint: heightHint,
	}

	select {
	case <-b.quit:
		return nil, ErrChainNotifierShuttingDown
	case b.notificationRegistry <- ntfn:
		return ntfn.Event, nil
	}
}

// blockEpochRegistration represents a client's intent to receive a
// notification with each newly connected block.
type blockEpochRegistration struct {
	epochID uint64

	epochChan chan *chainntnfs.BlockEpoch

	epochQueue *chainntnfs.ConcurrentQueue

	cancelChan chan struct{}

	wg sync.WaitGroup
}

// epochCancel is a message sent to the BtcdNotifier when a client wishes to
// cancel an outstanding epoch notification that has yet to be dispatched.
type epochCancel struct {
	epochID uint64
}

// RegisterBlockEpochNtfn returns a BlockEpochEvent which subscribes the
// caller to receive notifications, of each new block connected to the main
// chain.
func (b *BtcdNotifier) RegisterBlockEpochNtfn() (*chainntnfs.BlockEpochEvent, error) {
	reg := &blockEpochRegistration{
		epochQueue: chainntnfs.NewConcurrentQueue(20),
		epochChan:  make(chan *chainntnfs.BlockEpoch, 20),
		cancelChan: make(chan struct{}),
		epochID:    atomic.AddUint64(&b.epochClientCounter, 1),
	}
	reg.epochQueue.Start()

	// Before we send the request to the main goroutine, we'll launch a new
	// goroutine to proxy items added to our queue to the client itself.
	// This ensures that all notifications are received *in order*.
	reg.wg.Add(1)
	go func() {
		defer reg.wg.Done()

		for {
			select {
			case ntfn := <-reg.epochQueue.ChanOut():
				blockNtfn := ntfn.(*chainntnfs.BlockEpoch)
				select {
				case reg.epochChan <- blockNtfn:

				case <-reg.cancelChan:
					return

				case <-b.quit:
					return
				}

			case <-reg.cancelChan:
				return

			case <-b.quit:
				return
			}
		}
	}()

	select {
	case <-b.quit:
		// As we're exiting before the registration could be sent,
		// we'll stop the queue now ourselves.
		reg.epochQueue.Stop()

		return nil, errors.New("chainntnfs: system interrupt while " +
			"attempting to register for block epoch notification.")
	case b.notificationRegistry <- reg:
		return &chainntnfs.BlockEpochEvent{
			Epochs: reg.epochChan,
			Cancel: func() {
				cancel := &epochCancel{
					epochID: reg.epochID,
				}

				// Submit epoch cancellation to notification dispatcher.
				select {
				case b.notificationCancels <- cancel:
					// Cancellation is being handled, drain
					// the epoch channel until it is closed
					// before yielding to caller.
					for {
						select {
						case _, ok := <-reg.epochChan:
							if !ok {
								return
							}
						case <-b.quit:
							return
						}
					}
				case <-b.quit:
				}
			},
		}, nil
	}
}
