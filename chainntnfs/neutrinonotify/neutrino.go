package neutrinonotify

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/breez/lightninglib/chainntnfs"
	"github.com/breez/lightninglib/queue"
	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/btcsuite/btcwallet/waddrmgr"
	"github.com/lightninglabs/neutrino"
)

const (
	// notifierType uniquely identifies this concrete implementation of the
	// ChainNotifier interface.
	notifierType = "neutrino"
)

// NeutrinoNotifier is a version of ChainNotifier that's backed by the neutrino
// Bitcoin light client. Unlike other implementations, this implementation
// speaks directly to the p2p network. As a result, this implementation of the
// ChainNotifier interface is much more light weight that other implementation
// which rely of receiving notification over an RPC interface backed by a
// running full node.
//
// TODO(roasbeef): heavily consolidate with NeutrinoNotifier code
//  * maybe combine into single package?
type NeutrinoNotifier struct {
	confClientCounter  uint64 // To be used atomically.
	spendClientCounter uint64 // To be used atomically.
	epochClientCounter uint64 // To be used atomically.

	started int32 // To be used atomically.
	stopped int32 // To be used atomically.

	bestBlockMtx sync.RWMutex
	bestBlock    chainntnfs.BlockEpoch

	p2pNode   *neutrino.ChainService
	chainView *neutrino.Rescan

	chainConn *NeutrinoChainConn

	notificationCancels  chan interface{}
	notificationRegistry chan interface{}

	txNotifier *chainntnfs.TxNotifier

	blockEpochClients map[uint64]*blockEpochRegistration

	rescanErr <-chan error

	chainUpdates *queue.ConcurrentQueue
	txUpdates    *queue.ConcurrentQueue

	// spendHintCache is a cache used to query and update the latest height
	// hints for an outpoint. Each height hint represents the earliest
	// height at which the outpoint could have been spent within the chain.
	spendHintCache chainntnfs.SpendHintCache

	// confirmHintCache is a cache used to query the latest height hints for
	// a transaction. Each height hint represents the earliest height at
	// which the transaction could have confirmed within the chain.
	confirmHintCache chainntnfs.ConfirmHintCache

	wg   sync.WaitGroup
	quit chan struct{}
}

// Ensure NeutrinoNotifier implements the ChainNotifier interface at compile time.
var _ chainntnfs.ChainNotifier = (*NeutrinoNotifier)(nil)

// New creates a new instance of the NeutrinoNotifier concrete implementation
// of the ChainNotifier interface.
//
// NOTE: The passed neutrino node should already be running and active before
// being passed into this function.
func New(node *neutrino.ChainService, spendHintCache chainntnfs.SpendHintCache,
	confirmHintCache chainntnfs.ConfirmHintCache) *NeutrinoNotifier {

	return &NeutrinoNotifier{
		notificationCancels:  make(chan interface{}),
		notificationRegistry: make(chan interface{}),

		blockEpochClients: make(map[uint64]*blockEpochRegistration),

		p2pNode:   node,
		chainConn: &NeutrinoChainConn{node},

		rescanErr: make(chan error),

		chainUpdates: queue.NewConcurrentQueue(10),
		txUpdates:    queue.NewConcurrentQueue(10),

		spendHintCache:   spendHintCache,
		confirmHintCache: confirmHintCache,

		quit: make(chan struct{}),
	}
}

// Start contacts the running neutrino light client and kicks off an initial
// empty rescan.
func (n *NeutrinoNotifier) Start() error {
	// Already started?
	if atomic.AddInt32(&n.started, 1) != 1 {
		return nil
	}

	// First, we'll obtain the latest block height of the p2p node. We'll
	// start the auto-rescan from this point. Once a caller actually wishes
	// to register a chain view, the rescan state will be rewound
	// accordingly.
	startingPoint, err := n.p2pNode.BestBlock()
	if err != nil {
		return err
	}
	n.bestBlock.Hash = &startingPoint.Hash
	n.bestBlock.Height = startingPoint.Height

	n.txNotifier = chainntnfs.NewTxNotifier(
		uint32(n.bestBlock.Height), chainntnfs.ReorgSafetyLimit,
		n.confirmHintCache, n.spendHintCache,
	)

	// Next, we'll create our set of rescan options. Currently it's
	// required that a user MUST set an addr/outpoint/txid when creating a
	// rescan. To get around this, we'll add a "zero" outpoint, that won't
	// actually be matched.
	var zeroInput neutrino.InputWithScript
	rescanOptions := []neutrino.RescanOption{
		neutrino.StartBlock(startingPoint),
		neutrino.QuitChan(n.quit),
		neutrino.NotificationHandlers(
			rpcclient.NotificationHandlers{
				OnFilteredBlockConnected:    n.onFilteredBlockConnected,
				OnFilteredBlockDisconnected: n.onFilteredBlockDisconnected,
				OnRedeemingTx:               n.onRelevantTx,
			},
		),
		neutrino.WatchInputs(zeroInput),
	}

	// Finally, we'll create our rescan struct, start it, and launch all
	// the goroutines we need to operate this ChainNotifier instance.
	n.chainView = neutrino.NewRescan(
		&neutrino.RescanChainSource{
			ChainService: n.p2pNode,
		},
		rescanOptions...,
	)
	n.rescanErr = n.chainView.Start()

	n.chainUpdates.Start()
	n.txUpdates.Start()

	n.wg.Add(1)
	go n.notificationDispatcher()

	return nil
}

// Stop shuts down the NeutrinoNotifier.
func (n *NeutrinoNotifier) Stop() error {
	// Already shutting down?
	if atomic.AddInt32(&n.stopped, 1) != 1 {
		return nil
	}

	close(n.quit)
	n.wg.Wait()

	n.chainUpdates.Stop()
	n.txUpdates.Stop()

	// Notify all pending clients of our shutdown by closing the related
	// notification channels.
	for _, epochClient := range n.blockEpochClients {
		close(epochClient.cancelChan)
		epochClient.wg.Wait()

		close(epochClient.epochChan)
	}
	n.txNotifier.TearDown()

	return nil
}

// filteredBlock represents a new block which has been connected to the main
// chain. The slice of transactions will only be populated if the block
// includes a transaction that confirmed one of our watched txids, or spends
// one of the outputs currently being watched.
type filteredBlock struct {
	hash   chainhash.Hash
	height uint32
	txns   []*btcutil.Tx

	// connected is true if this update is a new block and false if it is a
	// disconnected block.
	connect bool
}

// rescanFilterUpdate represents a request that will be sent to the
// notificaionRegistry in order to prevent race conditions between the filter
// update and new block notifications.
type rescanFilterUpdate struct {
	updateOptions []neutrino.UpdateOption
	errChan       chan error
}

// onFilteredBlockConnected is a callback which is executed each a new block is
// connected to the end of the main chain.
func (n *NeutrinoNotifier) onFilteredBlockConnected(height int32,
	header *wire.BlockHeader, txns []*btcutil.Tx) {

	// Append this new chain update to the end of the queue of new chain
	// updates.
	select {
	case n.chainUpdates.ChanIn() <- &filteredBlock{
		hash:    header.BlockHash(),
		height:  uint32(height),
		txns:    txns,
		connect: true,
	}:
	case <-n.quit:
	}
}

// onFilteredBlockDisconnected is a callback which is executed each time a new
// block has been disconnected from the end of the mainchain due to a re-org.
func (n *NeutrinoNotifier) onFilteredBlockDisconnected(height int32,
	header *wire.BlockHeader) {

	// Append this new chain update to the end of the queue of new chain
	// disconnects.
	select {
	case n.chainUpdates.ChanIn() <- &filteredBlock{
		hash:    header.BlockHash(),
		height:  uint32(height),
		connect: false,
	}:
	case <-n.quit:
	}
}

// relevantTx represents a relevant transaction to the notifier that fulfills
// any outstanding spend requests.
type relevantTx struct {
	tx      *btcutil.Tx
	details *btcjson.BlockDetails
}

// onRelevantTx is a callback that proxies relevant transaction notifications
// from the backend to the notifier's main event handler.
func (n *NeutrinoNotifier) onRelevantTx(tx *btcutil.Tx, details *btcjson.BlockDetails) {
	select {
	case n.txUpdates.ChanIn() <- &relevantTx{tx, details}:
	case <-n.quit:
	}
}

// notificationDispatcher is the primary goroutine which handles client
// notification registrations, as well as notification dispatches.
func (n *NeutrinoNotifier) notificationDispatcher() {
	defer n.wg.Done()
out:
	for {
		select {
		case cancelMsg := <-n.notificationCancels:
			switch msg := cancelMsg.(type) {
			case *epochCancel:
				chainntnfs.Log.Infof("Cancelling epoch "+
					"notification, epoch_id=%v", msg.epochID)

				// First, we'll lookup the original
				// registration in order to stop the active
				// queue goroutine.
				reg := n.blockEpochClients[msg.epochID]
				reg.epochQueue.Stop()

				// Next, close the cancel channel for this
				// specific client, and wait for the client to
				// exit.
				close(n.blockEpochClients[msg.epochID].cancelChan)
				n.blockEpochClients[msg.epochID].wg.Wait()

				// Once the client has exited, we can then
				// safely close the channel used to send epoch
				// notifications, in order to notify any
				// listeners that the intent has been
				// cancelled.
				close(n.blockEpochClients[msg.epochID].epochChan)
				delete(n.blockEpochClients, msg.epochID)
			}

		case registerMsg := <-n.notificationRegistry:
			switch msg := registerMsg.(type) {
			case *chainntnfs.HistoricalConfDispatch:
				// We'll start a historical rescan chain of the
				// chain asynchronously to prevent blocking
				// potentially long rescans.
				n.wg.Add(1)
				go func() {
					defer n.wg.Done()

					confDetails, err := n.historicalConfDetails(
						msg.ConfRequest,
						msg.StartHeight, msg.EndHeight,
					)
					if err != nil {
						chainntnfs.Log.Error(err)
						return
					}

					// If the historical dispatch finished
					// without error, we will invoke
					// UpdateConfDetails even if none were
					// found. This allows the notifier to
					// begin safely updating the height hint
					// cache at tip, since any pending
					// rescans have now completed.
					err = n.txNotifier.UpdateConfDetails(
						msg.ConfRequest, confDetails,
					)
					if err != nil {
						chainntnfs.Log.Error(err)
					}
				}()

			case *blockEpochRegistration:
				chainntnfs.Log.Infof("New block epoch subscription")

				n.blockEpochClients[msg.epochID] = msg

				// If the client did not provide their best
				// known block, then we'll immediately dispatch
				// a notification for the current tip.
				if msg.bestBlock == nil {
					n.notifyBlockEpochClient(
						msg, n.bestBlock.Height,
						n.bestBlock.Hash,
					)

					msg.errorChan <- nil
					continue
				}

				// Otherwise, we'll attempt to deliver the
				// backlog of notifications from their best
				// known block.
				n.bestBlockMtx.Lock()
				bestHeight := n.bestBlock.Height
				n.bestBlockMtx.Unlock()

				missedBlocks, err := chainntnfs.GetClientMissedBlocks(
					n.chainConn, msg.bestBlock, bestHeight,
					false,
				)
				if err != nil {
					msg.errorChan <- err
					continue
				}

				for _, block := range missedBlocks {
					n.notifyBlockEpochClient(
						msg, block.Height, block.Hash,
					)
				}

				msg.errorChan <- nil

			case *rescanFilterUpdate:
				err := n.chainView.Update(msg.updateOptions...)
				if err != nil {
					chainntnfs.Log.Errorf("Unable to "+
						"update rescan filter: %v", err)
				}
				msg.errChan <- err
			}

		case item := <-n.chainUpdates.ChanOut():
			update := item.(*filteredBlock)
			if update.connect {
				n.bestBlockMtx.Lock()
				// Since neutrino has no way of knowing what
				// height to rewind to in the case of a reorged
				// best known height, there is no point in
				// checking that the previous hash matches the
				// the hash from our best known height the way
				// the other notifiers do when they receive
				// a new connected block. Therefore, we just
				// compare the heights.
				if update.height != uint32(n.bestBlock.Height+1) {
					// Handle the case where the notifier
					// missed some blocks from its chain
					// backend
					chainntnfs.Log.Infof("Missed blocks, " +
						"attempting to catch up")

					_, missedBlocks, err :=
						chainntnfs.HandleMissedBlocks(
							n.chainConn,
							n.txNotifier,
							n.bestBlock,
							int32(update.height),
							false,
						)
					if err != nil {
						chainntnfs.Log.Error(err)
						n.bestBlockMtx.Unlock()
						continue
					}

					for _, block := range missedBlocks {
						filteredBlock, err :=
							n.getFilteredBlock(block)
						if err != nil {
							chainntnfs.Log.Error(err)
							n.bestBlockMtx.Unlock()
							continue out
						}
						err = n.handleBlockConnected(filteredBlock)
						if err != nil {
							chainntnfs.Log.Error(err)
							n.bestBlockMtx.Unlock()
							continue out
						}
					}

				}

				err := n.handleBlockConnected(update)
				if err != nil {
					chainntnfs.Log.Error(err)
				}

				n.bestBlockMtx.Unlock()
				continue
			}

			n.bestBlockMtx.Lock()
			if update.height != uint32(n.bestBlock.Height) {
				chainntnfs.Log.Infof("Missed disconnected " +
					"blocks, attempting to catch up")
			}
			newBestBlock, err := chainntnfs.RewindChain(
				n.chainConn, n.txNotifier, n.bestBlock,
				int32(update.height-1),
			)
			if err != nil {
				chainntnfs.Log.Errorf("Unable to rewind chain "+
					"from height %d to height %d: %v",
					n.bestBlock.Height, update.height-1, err)
			}

			// Set the bestHeight here in case a chain rewind
			// partially completed.
			n.bestBlock = newBestBlock
			n.bestBlockMtx.Unlock()

		case txUpdate := <-n.txUpdates.ChanOut():
			// A new relevant transaction notification has been
			// received from the backend. We'll attempt to process
			// it to determine if it fulfills any outstanding
			// confirmation and/or spend requests and dispatch
			// notifications for them.
			update := txUpdate.(*relevantTx)
			err := n.txNotifier.ProcessRelevantSpendTx(
				update.tx, uint32(update.details.Height),
			)
			if err != nil {
				chainntnfs.Log.Errorf("Unable to process "+
					"transaction %v: %v", update.tx.Hash(),
					err)
			}

		case err := <-n.rescanErr:
			chainntnfs.Log.Errorf("Error during rescan: %v", err)

		case <-n.quit:
			return

		}
	}
}

// historicalConfDetails looks up whether a confirmation request (txid/output
// script) has already been included in a block in the active chain and, if so,
// returns details about said block.
func (n *NeutrinoNotifier) historicalConfDetails(confRequest chainntnfs.ConfRequest,
	startHeight, endHeight uint32) (*chainntnfs.TxConfirmation, error) {

	// Starting from the height hint, we'll walk forwards in the chain to
	// see if this transaction/output script has already been confirmed.
	for scanHeight := endHeight; scanHeight >= startHeight && scanHeight > 0; scanHeight-- {
		// Ensure we haven't been requested to shut down before
		// processing the next height.
		select {
		case <-n.quit:
			return nil, chainntnfs.ErrChainNotifierShuttingDown
		default:
		}

		// First, we'll fetch the block header for this height so we
		// can compute the current block hash.
		blockHash, err := n.p2pNode.GetBlockHash(int64(scanHeight))
		if err != nil {
			return nil, fmt.Errorf("unable to get header for height=%v: %v",
				scanHeight, err)
		}

		// With the hash computed, we can now fetch the basic filter
		// for this height.
		regFilter, err := n.p2pNode.GetCFilter(
			*blockHash, wire.GCSFilterRegular,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to retrieve regular filter for "+
				"height=%v: %v", scanHeight, err)
		}

		// If the block has no transactions other than the Coinbase
		// transaction, then the filter may be nil, so we'll continue
		// forward int that case.
		if regFilter == nil {
			continue
		}

		// In the case that the filter exists, we'll attempt to see if
		// any element in it matches our target public key script.
		key := builder.DeriveKey(blockHash)
		match, err := regFilter.Match(key, confRequest.PkScript.Script())
		if err != nil {
			return nil, fmt.Errorf("unable to query filter: %v", err)
		}

		// If there's no match, then we can continue forward to the
		// next block.
		if !match {
			continue
		}

		// In the case that we do have a match, we'll fetch the block
		// from the network so we can find the positional data required
		// to send the proper response.
		block, err := n.p2pNode.GetBlock(*blockHash)
		if err != nil {
			return nil, fmt.Errorf("unable to get block from network: %v", err)
		}

		// For every transaction in the block, check which one matches
		// our request. If we find one that does, we can dispatch its
		// confirmation details.
		for i, tx := range block.Transactions() {
			if !confRequest.MatchesTx(tx.MsgTx()) {
				continue
			}

			return &chainntnfs.TxConfirmation{
				Tx:          tx.MsgTx(),
				BlockHash:   blockHash,
				BlockHeight: scanHeight,
				TxIndex:     uint32(i),
			}, nil
		}
	}

	return nil, nil
}

// handleBlockConnected applies a chain update for a new block. Any watched
// transactions included this block will processed to either send notifications
// now or after numConfirmations confs.
//
// NOTE: This method must be called with the bestBlockMtx lock held.
func (n *NeutrinoNotifier) handleBlockConnected(newBlock *filteredBlock) error {
	// We'll extend the txNotifier's height with the information of this new
	// block, which will handle all of the notification logic for us.
	err := n.txNotifier.ConnectTip(
		&newBlock.hash, newBlock.height, newBlock.txns,
	)
	if err != nil {
		return fmt.Errorf("unable to connect tip: %v", err)
	}

	chainntnfs.Log.Infof("New block: height=%v, sha=%v", newBlock.height,
		newBlock.hash)

	// Now that we've guaranteed the new block extends the txNotifier's
	// current tip, we'll proceed to dispatch notifications to all of our
	// registered clients whom have had notifications fulfilled. Before
	// doing so, we'll make sure update our in memory state in order to
	// satisfy any client requests based upon the new block.
	n.bestBlock.Hash = &newBlock.hash
	n.bestBlock.Height = int32(newBlock.height)

	n.notifyBlockEpochs(int32(newBlock.height), &newBlock.hash)
	return n.txNotifier.NotifyHeight(newBlock.height)
}

// getFilteredBlock is a utility to retrieve the full filtered block from a block epoch.
func (n *NeutrinoNotifier) getFilteredBlock(epoch chainntnfs.BlockEpoch) (*filteredBlock, error) {
	rawBlock, err := n.p2pNode.GetBlock(*epoch.Hash)
	if err != nil {
		return nil, fmt.Errorf("unable to get block: %v", err)
	}

	txns := rawBlock.Transactions()

	block := &filteredBlock{
		hash:    *epoch.Hash,
		height:  uint32(epoch.Height),
		txns:    txns,
		connect: true,
	}
	return block, nil
}

// notifyBlockEpochs notifies all registered block epoch clients of the newly
// connected block to the main chain.
func (n *NeutrinoNotifier) notifyBlockEpochs(newHeight int32, newSha *chainhash.Hash) {
	for _, client := range n.blockEpochClients {
		n.notifyBlockEpochClient(client, newHeight, newSha)
	}
}

// notifyBlockEpochClient sends a registered block epoch client a notification
// about a specific block.
func (n *NeutrinoNotifier) notifyBlockEpochClient(epochClient *blockEpochRegistration,
	height int32, sha *chainhash.Hash) {

	epoch := &chainntnfs.BlockEpoch{
		Height: height,
		Hash:   sha,
	}

	select {
	case epochClient.epochQueue.ChanIn() <- epoch:
	case <-epochClient.cancelChan:
	case <-n.quit:
	}
}

// RegisterSpendNtfn registers an intent to be notified once the target
// outpoint/output script has been spent by a transaction on-chain. When
// intending to be notified of the spend of an output script, a nil outpoint
// must be used. The heightHint should represent the earliest height in the
// chain of the transaction that spent the outpoint/output script.
//
// Once a spend of has been detected, the details of the spending event will be
// sent across the 'Spend' channel.
func (n *NeutrinoNotifier) RegisterSpendNtfn(outpoint *wire.OutPoint,
	pkScript []byte, heightHint uint32) (*chainntnfs.SpendEvent, error) {

	// First, we'll construct a spend notification request and hand it off
	// to the txNotifier.
	spendID := atomic.AddUint64(&n.spendClientCounter, 1)
	spendRequest, err := chainntnfs.NewSpendRequest(outpoint, pkScript)
	if err != nil {
		return nil, err
	}
	ntfn := &chainntnfs.SpendNtfn{
		SpendID:      spendID,
		SpendRequest: spendRequest,
		Event: chainntnfs.NewSpendEvent(func() {
			n.txNotifier.CancelSpend(spendRequest, spendID)
		}),
		HeightHint: heightHint,
	}

	historicalDispatch, txNotifierTip, err := n.txNotifier.RegisterSpend(ntfn)
	if err != nil {
		return nil, err
	}

	// To determine whether this outpoint has been spent on-chain, we'll
	// update our filter to watch for the transaction at tip and we'll also
	// dispatch a historical rescan to determine if it has been spent in the
	// past.
	//
	// We'll update our filter first to ensure we can immediately detect the
	// spend at tip.
	inputToWatch := neutrino.InputWithScript{
		OutPoint: spendRequest.OutPoint,
		PkScript: spendRequest.PkScript.Script(),
	}
	updateOptions := []neutrino.UpdateOption{
		neutrino.AddInputs(inputToWatch),
		neutrino.DisableDisconnectedNtfns(true),
	}

	// We'll use the txNotifier's tip as the starting point of our filter
	// update. In the case of an output script spend request, we'll check if
	// we should perform a historical rescan and start from there, as we
	// cannot do so with GetUtxo since it matches outpoints.
	rewindHeight := txNotifierTip
	if historicalDispatch != nil &&
		spendRequest.OutPoint == chainntnfs.ZeroOutPoint {
		rewindHeight = historicalDispatch.StartHeight
	}
	updateOptions = append(updateOptions, neutrino.Rewind(rewindHeight))

	errChan := make(chan error, 1)
	select {
	case n.notificationRegistry <- &rescanFilterUpdate{
		updateOptions: updateOptions,
		errChan:       errChan,
	}:
	case <-n.quit:
		return nil, chainntnfs.ErrChainNotifierShuttingDown
	}

	select {
	case err = <-errChan:
	case <-n.quit:
		return nil, chainntnfs.ErrChainNotifierShuttingDown
	}
	if err != nil {
		return nil, fmt.Errorf("unable to update filter: %v", err)
	}

	// If the txNotifier didn't return any details to perform a historical
	// scan of the chain, or if we already performed one like in the case of
	// output script spend requests, then we can return early as there's
	// nothing left for us to do.
	if historicalDispatch == nil ||
		spendRequest.OutPoint == chainntnfs.ZeroOutPoint {
		return ntfn.Event, nil
	}

	// With the filter updated, we'll dispatch our historical rescan to
	// ensure we detect the spend if it happened in the past. We'll ensure
	// that neutrino is caught up to the starting height before we attempt
	// to fetch the UTXO from the chain. If we're behind, then we may miss a
	// notification dispatch.
	for {
		n.bestBlockMtx.RLock()
		currentHeight := uint32(n.bestBlock.Height)
		n.bestBlockMtx.RUnlock()

		if currentHeight >= historicalDispatch.StartHeight {
			break
		}

		time.Sleep(time.Millisecond * 200)
	}

	spendReport, err := n.p2pNode.GetUtxo(
		neutrino.WatchInputs(inputToWatch),
		neutrino.StartBlock(&waddrmgr.BlockStamp{
			Height: int32(historicalDispatch.StartHeight),
		}),
		neutrino.EndBlock(&waddrmgr.BlockStamp{
			Height: int32(historicalDispatch.EndHeight),
		}),
	)
	if err != nil && !strings.Contains(err.Error(), "not found") {
		return nil, err
	}

	// If a spend report was returned, and the transaction is present, then
	// this means that the output is already spent.
	var spendDetails *chainntnfs.SpendDetail
	if spendReport != nil && spendReport.SpendingTx != nil {
		spendingTxHash := spendReport.SpendingTx.TxHash()
		spendDetails = &chainntnfs.SpendDetail{
			SpentOutPoint:     &spendRequest.OutPoint,
			SpenderTxHash:     &spendingTxHash,
			SpendingTx:        spendReport.SpendingTx,
			SpenderInputIndex: spendReport.SpendingInputIndex,
			SpendingHeight:    int32(spendReport.SpendingTxHeight),
		}
	}

	// Finally, no matter whether the rescan found a spend in the past or
	// not, we'll mark our historical rescan as complete to ensure the
	// outpoint's spend hint gets updated upon connected/disconnected
	// blocks.
	err = n.txNotifier.UpdateSpendDetails(spendRequest, spendDetails)
	if err != nil {
		return nil, err
	}

	return ntfn.Event, nil
}

// RegisterConfirmationsNtfn registers an intent to be notified once the target
// txid/output script has reached numConfs confirmations on-chain. When
// intending to be notified of the confirmation of an output script, a nil txid
// must be used. The heightHint should represent the earliest height at which
// the txid/output script could have been included in the chain.
//
// Progress on the number of confirmations left can be read from the 'Updates'
// channel. Once it has reached all of its confirmations, a notification will be
// sent across the 'Confirmed' channel.
func (n *NeutrinoNotifier) RegisterConfirmationsNtfn(txid *chainhash.Hash,
	pkScript []byte,
	numConfs, heightHint uint32) (*chainntnfs.ConfirmationEvent, error) {

	// Construct a notification request for the transaction and send it to
	// the main event loop.
	confID := atomic.AddUint64(&n.confClientCounter, 1)
	confRequest, err := chainntnfs.NewConfRequest(txid, pkScript)
	if err != nil {
		return nil, err
	}
	ntfn := &chainntnfs.ConfNtfn{
		ConfID:           confID,
		ConfRequest:      confRequest,
		NumConfirmations: numConfs,
		Event: chainntnfs.NewConfirmationEvent(numConfs, func() {
			n.txNotifier.CancelConf(confRequest, confID)
		}),
		HeightHint: heightHint,
	}

	chainntnfs.Log.Infof("New confirmation subscription: %v, num_confs=%v",
		confRequest, numConfs)

	// Register the conf notification with the TxNotifier. A non-nil value
	// for `dispatch` will be returned if we are required to perform a
	// manual scan for the confirmation. Otherwise the notifier will begin
	// watching at tip for the transaction to confirm.
	dispatch, txNotifierTip, err := n.txNotifier.RegisterConf(ntfn)
	if err != nil {
		return nil, err
	}

	// To determine whether this transaction has confirmed on-chain, we'll
	// update our filter to watch for the transaction at tip and we'll also
	// dispatch a historical rescan to determine if it has confirmed in the
	// past.
	//
	// We'll update our filter first to ensure we can immediately detect the
	// confirmation at tip. To do so, we'll map the script into an address
	// type so we can instruct neutrino to match if the transaction
	// containing the script is found in a block.
	params := n.p2pNode.ChainParams()
	_, addrs, _, err := txscript.ExtractPkScriptAddrs(
		confRequest.PkScript.Script(), &params,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to extract script: %v", err)
	}

	// We'll send the filter update request to the notifier's main event
	// handler and wait for its response.
	errChan := make(chan error, 1)
	select {
	case n.notificationRegistry <- &rescanFilterUpdate{
		updateOptions: []neutrino.UpdateOption{
			neutrino.AddAddrs(addrs...),
			neutrino.Rewind(txNotifierTip),
			neutrino.DisableDisconnectedNtfns(true),
		},
		errChan: errChan,
	}:
	case <-n.quit:
		return nil, chainntnfs.ErrChainNotifierShuttingDown
	}

	select {
	case err = <-errChan:
	case <-n.quit:
		return nil, chainntnfs.ErrChainNotifierShuttingDown
	}
	if err != nil {
		return nil, fmt.Errorf("unable to update filter: %v", err)
	}

	// If a historical rescan was not requested by the txNotifier, then we
	// can return to the caller.
	if dispatch == nil {
		return ntfn.Event, nil
	}

	// Finally, with the filter updated, we can dispatch the historical
	// rescan to ensure we can detect if the event happened in the past.
	select {
	case n.notificationRegistry <- dispatch:
	case <-n.quit:
		return nil, chainntnfs.ErrChainNotifierShuttingDown
	}

	return ntfn.Event, nil
}

// blockEpochRegistration represents a client's intent to receive a
// notification with each newly connected block.
type blockEpochRegistration struct {
	epochID uint64

	epochChan chan *chainntnfs.BlockEpoch

	epochQueue *queue.ConcurrentQueue

	cancelChan chan struct{}

	bestBlock *chainntnfs.BlockEpoch

	errorChan chan error

	wg sync.WaitGroup
}

// epochCancel is a message sent to the NeutrinoNotifier when a client wishes
// to cancel an outstanding epoch notification that has yet to be dispatched.
type epochCancel struct {
	epochID uint64
}

// RegisterBlockEpochNtfn returns a BlockEpochEvent which subscribes the
// caller to receive notifications, of each new block connected to the main
// chain. Clients have the option of passing in their best known block, which
// the notifier uses to check if they are behind on blocks and catch them up. If
// they do not provide one, then a notification will be dispatched immediately
// for the current tip of the chain upon a successful registration.
func (n *NeutrinoNotifier) RegisterBlockEpochNtfn(
	bestBlock *chainntnfs.BlockEpoch) (*chainntnfs.BlockEpochEvent, error) {

	reg := &blockEpochRegistration{
		epochQueue: queue.NewConcurrentQueue(20),
		epochChan:  make(chan *chainntnfs.BlockEpoch, 20),
		cancelChan: make(chan struct{}),
		epochID:    atomic.AddUint64(&n.epochClientCounter, 1),
		bestBlock:  bestBlock,
		errorChan:  make(chan error, 1),
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

				case <-n.quit:
					return
				}

			case <-reg.cancelChan:
				return

			case <-n.quit:
				return
			}
		}
	}()

	select {
	case <-n.quit:
		// As we're exiting before the registration could be sent,
		// we'll stop the queue now ourselves.
		reg.epochQueue.Stop()

		return nil, errors.New("chainntnfs: system interrupt while " +
			"attempting to register for block epoch notification.")
	case n.notificationRegistry <- reg:
		return &chainntnfs.BlockEpochEvent{
			Epochs: reg.epochChan,
			Cancel: func() {
				cancel := &epochCancel{
					epochID: reg.epochID,
				}

				// Submit epoch cancellation to notification dispatcher.
				select {
				case n.notificationCancels <- cancel:
					// Cancellation is being handled, drain the epoch channel until it is
					// closed before yielding to caller.
					for {
						select {
						case _, ok := <-reg.epochChan:
							if !ok {
								return
							}
						case <-n.quit:
							return
						}
					}
				case <-n.quit:
				}
			},
		}, nil
	}
}

// NeutrinoChainConn is a wrapper around neutrino's chain backend in order
// to satisfy the chainntnfs.ChainConn interface.
type NeutrinoChainConn struct {
	p2pNode *neutrino.ChainService
}

// GetBlockHeader returns the block header for a hash.
func (n *NeutrinoChainConn) GetBlockHeader(blockHash *chainhash.Hash) (*wire.BlockHeader, error) {
	return n.p2pNode.GetBlockHeader(blockHash)
}

// GetBlockHeaderVerbose returns a verbose block header result for a hash. This
// result only contains the height with a nil hash.
func (n *NeutrinoChainConn) GetBlockHeaderVerbose(blockHash *chainhash.Hash) (
	*btcjson.GetBlockHeaderVerboseResult, error) {

	height, err := n.p2pNode.GetBlockHeight(blockHash)
	if err != nil {
		return nil, err
	}
	// Since only the height is used from the result, leave the hash nil.
	return &btcjson.GetBlockHeaderVerboseResult{Height: int32(height)}, nil
}

// GetBlockHash returns the hash from a block height.
func (n *NeutrinoChainConn) GetBlockHash(blockHeight int64) (*chainhash.Hash, error) {
	return n.p2pNode.GetBlockHash(blockHeight)
}
