package neutrinonotify

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/breez/lightninglib/chainntnfs"
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

	// reorgSafetyLimit is the chain depth beyond which it is assumed a block
	// will not be reorganized out of the chain. This is used to determine when
	// to prune old confirmation requests so that reorgs are handled correctly.
	// The coinbase maturity period is a reasonable value to use.
	reorgSafetyLimit = 100
)

var (
	// ErrChainNotifierShuttingDown is used when we are trying to
	// measure a spend notification when notifier is already stopped.
	ErrChainNotifierShuttingDown = errors.New("chainntnfs: system interrupt " +
		"while attempting to register for spend notification.")
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
	started int32 // To be used atomically.
	stopped int32 // To be used atomically.

	spendClientCounter uint64 // To be used atomically.
	epochClientCounter uint64 // To be used atomically.

	heightMtx  sync.RWMutex
	bestHeight uint32

	p2pNode   *neutrino.ChainService
	chainView *neutrino.Rescan

	notificationCancels  chan interface{}
	notificationRegistry chan interface{}

	spendNotifications map[wire.OutPoint]map[uint64]*spendNotification

	txConfNotifier *chainntnfs.TxConfNotifier

	blockEpochClients map[uint64]*blockEpochRegistration

	rescanErr <-chan error

	chainUpdates *chainntnfs.ConcurrentQueue

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
func New(node *neutrino.ChainService) (*NeutrinoNotifier, error) {
	notifier := &NeutrinoNotifier{
		notificationCancels:  make(chan interface{}),
		notificationRegistry: make(chan interface{}),

		blockEpochClients: make(map[uint64]*blockEpochRegistration),

		spendNotifications: make(map[wire.OutPoint]map[uint64]*spendNotification),

		p2pNode: node,

		rescanErr: make(chan error),

		chainUpdates: chainntnfs.NewConcurrentQueue(10),

		quit: make(chan struct{}),
	}

	return notifier, nil
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
	bestHeader, bestHeight, err := n.p2pNode.BlockHeaders.ChainTip()
	if err != nil {
		return err
	}
	startingPoint := &waddrmgr.BlockStamp{
		Height: int32(bestHeight),
		Hash:   bestHeader.BlockHash(),
	}
	n.bestHeight = bestHeight

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
			},
		),
		neutrino.WatchInputs(zeroInput),
	}

	n.txConfNotifier = chainntnfs.NewTxConfNotifier(
		bestHeight, reorgSafetyLimit,
	)

	// Finally, we'll create our rescan struct, start it, and launch all
	// the goroutines we need to operate this ChainNotifier instance.
	n.chainView = n.p2pNode.NewRescan(rescanOptions...)
	n.rescanErr = n.chainView.Start()

	n.chainUpdates.Start()

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

	// Notify all pending clients of our shutdown by closing the related
	// notification channels.
	for _, spendClients := range n.spendNotifications {
		for _, spendClient := range spendClients {
			close(spendClient.spendChan)
		}
	}
	for _, epochClient := range n.blockEpochClients {
		close(epochClient.cancelChan)
		epochClient.wg.Wait()

		close(epochClient.epochChan)
	}
	n.txConfNotifier.TearDown()

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

// onFilteredBlockConnected is a callback which is executed each a new block is
// connected to the end of the main chain.
func (n *NeutrinoNotifier) onFilteredBlockConnected(height int32,
	header *wire.BlockHeader, txns []*btcutil.Tx) {

	// Append this new chain update to the end of the queue of new chain
	// updates.
	n.chainUpdates.ChanIn() <- &filteredBlock{
		hash:    header.BlockHash(),
		height:  uint32(height),
		txns:    txns,
		connect: true,
	}
}

// onFilteredBlockDisconnected is a callback which is executed each time a new
// block has been disconnected from the end of the mainchain due to a re-org.
func (n *NeutrinoNotifier) onFilteredBlockDisconnected(height int32,
	header *wire.BlockHeader) {

	// Append this new chain update to the end of the queue of new chain
	// disconnects.
	n.chainUpdates.ChanIn() <- &filteredBlock{
		hash:    header.BlockHash(),
		height:  uint32(height),
		connect: false,
	}
}

// notificationDispatcher is the primary goroutine which handles client
// notification registrations, as well as notification dispatches.
func (n *NeutrinoNotifier) notificationDispatcher() {
	defer n.wg.Done()

	for {
		select {
		case cancelMsg := <-n.notificationCancels:
			switch msg := cancelMsg.(type) {
			case *spendCancel:
				chainntnfs.Log.Infof("Cancelling spend "+
					"notification for out_point=%v, "+
					"spend_id=%v", msg.op, msg.spendID)

				// Before we attempt to close the spendChan,
				// ensure that the notification hasn't already
				// yet been dispatched.
				if outPointClients, ok := n.spendNotifications[msg.op]; ok {
					close(outPointClients[msg.spendID].spendChan)
					delete(n.spendNotifications[msg.op], msg.spendID)
				}

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
			case *spendNotification:
				chainntnfs.Log.Infof("New spend subscription: "+
					"utxo=%v, height_hint=%v",
					msg.targetOutpoint, msg.heightHint)
				op := *msg.targetOutpoint

				if _, ok := n.spendNotifications[op]; !ok {
					n.spendNotifications[op] = make(map[uint64]*spendNotification)
				}
				n.spendNotifications[op][msg.spendID] = msg

			case *confirmationsNotification:
				chainntnfs.Log.Infof("New confirmations subscription: "+
					"txid=%v, numconfs=%v, height_hint=%v",
					msg.TxID, msg.NumConfirmations, msg.heightHint)

				// If the notification can be partially or
				// fully dispatched, then we can skip the first
				// phase for ntfns.
				n.heightMtx.RLock()
				currentHeight := n.bestHeight
				n.heightMtx.RUnlock()

				// Lookup whether the transaction is already included in the
				// active chain.
				txConf, err := n.historicalConfDetails(
					msg.TxID, msg.pkScript, currentHeight, msg.heightHint,
				)
				if err != nil {
					chainntnfs.Log.Error(err)
				}

				// We'll map the script into an address type so
				// we can instruct neutrino to match if the
				// transaction containing the script is found
				// in a block.
				params := n.p2pNode.ChainParams()
				_, addrs, _, err := txscript.ExtractPkScriptAddrs(
					msg.pkScript, &params,
				)
				if err != nil {
					chainntnfs.Log.Error(err)
				}

				if txConf == nil {
					// If we can't fully dispatch confirmation,
					// then we'll update our filter so we can be
					// notified of its future initial confirmation.
					rescanUpdate := []neutrino.UpdateOption{
						neutrino.AddAddrs(addrs...),
						neutrino.Rewind(currentHeight),
					}
					if err := n.chainView.Update(rescanUpdate...); err != nil {
						chainntnfs.Log.Errorf("unable to update rescan: %v", err)
					}
				}

				err = n.txConfNotifier.Register(&msg.ConfNtfn, txConf)
				if err != nil {
					chainntnfs.Log.Error(err)
				}

			case *blockEpochRegistration:
				chainntnfs.Log.Infof("New block epoch subscription")
				n.blockEpochClients[msg.epochID] = msg
			}

		case item := <-n.chainUpdates.ChanOut():
			update := item.(*filteredBlock)
			if update.connect {
				n.heightMtx.Lock()
				if update.height != n.bestHeight+1 {
					chainntnfs.Log.Warnf("Received blocks out of order: "+
						"current height=%d, new height=%d",
						n.bestHeight, update.height)
					n.heightMtx.Unlock()
					continue
				}

				n.bestHeight = update.height
				n.heightMtx.Unlock()

				chainntnfs.Log.Infof("New block: height=%v, sha=%v",
					update.height, update.hash)

				err := n.handleBlockConnected(update)
				if err != nil {
					chainntnfs.Log.Error(err)
				}
				continue
			}

			n.heightMtx.Lock()
			if update.height != n.bestHeight {
				chainntnfs.Log.Warnf("Received blocks out of order: "+
					"current height=%d, disconnected height=%d",
					n.bestHeight, update.height)
				n.heightMtx.Unlock()
				continue
			}

			n.bestHeight = update.height - 1
			n.heightMtx.Unlock()

			chainntnfs.Log.Infof("Block disconnected from main chain: "+
				"height=%v, sha=%v", update.height, update.hash)

			err := n.txConfNotifier.DisconnectTip(update.height)
			if err != nil {
				chainntnfs.Log.Error(err)
			}

		case err := <-n.rescanErr:
			chainntnfs.Log.Errorf("Error during rescan: %v", err)

		case <-n.quit:
			return

		}
	}
}

// historicalConfDetails looks up whether a transaction is already included in
// a block in the active chain and, if so, returns details about the
// confirmation.
func (n *NeutrinoNotifier) historicalConfDetails(targetHash *chainhash.Hash,
	pkScript []byte,
	currentHeight, heightHint uint32) (*chainntnfs.TxConfirmation, error) {

	// Starting from the height hint, we'll walk forwards in the chain to
	// see if this transaction has already been confirmed.
	for scanHeight := heightHint; scanHeight <= currentHeight; scanHeight++ {
		// First, we'll fetch the block header for this height so we
		// can compute the current block hash.
		header, err := n.p2pNode.BlockHeaders.FetchHeaderByHeight(scanHeight)
		if err != nil {
			return nil, fmt.Errorf("unable to get header for height=%v: %v",
				scanHeight, err)
		}
		blockHash := header.BlockHash()

		// With the hash computed, we can now fetch the basic filter
		// for this height.
		regFilter, err := n.p2pNode.GetCFilter(
			blockHash, wire.GCSFilterRegular,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to retrieve regular filter for "+
				"height=%v: %v", scanHeight, err)
		}

		// If the block has no transactions other than the coinbase
		// transaction, then the filter may be nil, so we'll continue
		// forward int that case.
		if regFilter == nil {
			continue
		}

		// In the case that the filter exists, we'll attempt to see if
		// any element in it match our target public key script.
		key := builder.DeriveKey(&blockHash)
		match, err := regFilter.Match(key, pkScript)
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
		block, err := n.p2pNode.GetBlockFromNetwork(blockHash)
		if err != nil {
			return nil, fmt.Errorf("unable to get block from network: %v", err)
		}
		for j, tx := range block.Transactions() {
			txHash := tx.Hash()
			if txHash.IsEqual(targetHash) {
				confDetails := chainntnfs.TxConfirmation{
					BlockHash:   &blockHash,
					BlockHeight: scanHeight,
					TxIndex:     uint32(j),
				}
				return &confDetails, nil
			}
		}
	}

	return nil, nil
}

// handleBlocksConnected applies a chain update for a new block. Any watched
// transactions included this block will processed to either send notifications
// now or after numConfirmations confs.
func (n *NeutrinoNotifier) handleBlockConnected(newBlock *filteredBlock) error {
	// First we'll notify any subscribed clients of the block.
	n.notifyBlockEpochs(int32(newBlock.height), &newBlock.hash)

	// Next, we'll scan over the list of relevant transactions and possibly
	// dispatch notifications for confirmations and spends.
	for _, tx := range newBlock.txns {
		mtx := tx.MsgTx()
		txSha := mtx.TxHash()

		for i, txIn := range mtx.TxIn {
			prevOut := txIn.PreviousOutPoint

			// If this transaction indeed does spend an output
			// which we have a registered notification for, then
			// create a spend summary, finally
			// sending off the details to the notification subscriber.
			clients, ok := n.spendNotifications[prevOut]
			if !ok {
				continue
			}

			// TODO(roasbeef): many integration tests expect spend
			// to be notified within the mempool.
			spendDetails := &chainntnfs.SpendDetail{
				SpentOutPoint:     &prevOut,
				SpenderTxHash:     &txSha,
				SpendingTx:        mtx,
				SpenderInputIndex: uint32(i),
				SpendingHeight:    int32(newBlock.height),
			}

			for _, ntfn := range clients {
				chainntnfs.Log.Infof("Dispatching spend "+
					"notification for outpoint=%v",
					ntfn.targetOutpoint)

				ntfn.spendChan <- spendDetails

				// Close spendChan to ensure that any calls to
				// Cancel will not block. This is safe to do
				// since the channel is buffered, and the
				// message can still be read by the receiver.
				close(ntfn.spendChan)
			}

			delete(n.spendNotifications, prevOut)
		}
	}

	// A new block has been connected to the main chain.  Send out any N
	// confirmation notifications which may have been triggered by this new
	// block.
	n.txConfNotifier.ConnectTip(
		&newBlock.hash, newBlock.height, newBlock.txns,
	)

	return nil
}

// notifyBlockEpochs notifies all registered block epoch clients of the newly
// connected block to the main chain.
func (n *NeutrinoNotifier) notifyBlockEpochs(newHeight int32, newSha *chainhash.Hash) {
	epoch := &chainntnfs.BlockEpoch{
		Height: newHeight,
		Hash:   newSha,
	}

	for _, epochClient := range n.blockEpochClients {
		select {

		case epochClient.epochQueue.ChanIn() <- epoch:

		case <-epochClient.cancelChan:

		case <-n.quit:
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

// spendCancel is a message sent to the NeutrinoNotifier when a client wishes
// to cancel an outstanding spend notification that has yet to be dispatched.
type spendCancel struct {
	// op is the target outpoint of the notification to be cancelled.
	op wire.OutPoint

	// spendID the ID of the notification to cancel.
	spendID uint64
}

// RegisterSpendNtfn registers an intent to be notified once the target
// outpoint has been spent by a transaction on-chain. Once a spend of the
// target outpoint has been detected, the details of the spending event will be
// sent across the 'Spend' channel.
func (n *NeutrinoNotifier) RegisterSpendNtfn(outpoint *wire.OutPoint,
	pkScript []byte, heightHint uint32) (*chainntnfs.SpendEvent, error) {

	n.heightMtx.RLock()
	currentHeight := n.bestHeight
	n.heightMtx.RUnlock()

	chainntnfs.Log.Infof("New spend notification for outpoint=%v, "+
		"height_hint=%v", outpoint, heightHint)

	ntfn := &spendNotification{
		targetOutpoint: outpoint,
		spendChan:      make(chan *chainntnfs.SpendDetail, 1),
		spendID:        atomic.AddUint64(&n.spendClientCounter, 1),
		heightHint:     heightHint,
	}
	spendEvent := &chainntnfs.SpendEvent{
		Spend: ntfn.spendChan,
		Cancel: func() {
			cancel := &spendCancel{
				op:      *outpoint,
				spendID: ntfn.spendID,
			}

			// Submit spend cancellation to notification dispatcher.
			select {
			case n.notificationCancels <- cancel:
				// Cancellation is being handled, drain the spend chan until it is
				// closed before yielding to the caller.
				for {
					select {
					case _, ok := <-ntfn.spendChan:
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
	}

	// Ensure that neutrino is caught up to the height hint before we
	// attempt to fetch the utxo fromt the chain. If we're behind, then we
	// may miss a notification dispatch.
	for {
		n.heightMtx.RLock()
		currentHeight := n.bestHeight
		n.heightMtx.RUnlock()

		if currentHeight < heightHint {
			time.Sleep(time.Millisecond * 200)
			continue
		}

		break
	}

	inputToWatch := neutrino.InputWithScript{
		OutPoint: *outpoint,
		PkScript: pkScript,
	}

	// Before sending off the notification request, we'll attempt to see if
	// this output is still spent or not at this point in the chain.
	spendReport, err := n.p2pNode.GetUtxo(
		neutrino.WatchInputs(inputToWatch),
		neutrino.StartBlock(&waddrmgr.BlockStamp{
			Height: int32(heightHint),
		}),
	)
	if err != nil && !strings.Contains(err.Error(), "not found") {
		return nil, err
	}

	// If a spend report was returned, and the transaction is present, then
	// this means that the output is already spent.
	if spendReport != nil && spendReport.SpendingTx != nil {
		// As a result, we'll launch a goroutine to immediately
		// dispatch the notification with a normal response.
		go func() {
			txSha := spendReport.SpendingTx.TxHash()
			select {
			case ntfn.spendChan <- &chainntnfs.SpendDetail{
				SpentOutPoint:     outpoint,
				SpenderTxHash:     &txSha,
				SpendingTx:        spendReport.SpendingTx,
				SpenderInputIndex: spendReport.SpendingInputIndex,
				SpendingHeight:    int32(spendReport.SpendingTxHeight),
			}:
			case <-n.quit:
				return
			}

		}()

		return spendEvent, nil
	}

	// If the output is still unspent, then we'll update our rescan's
	// filter, and send the request to the dispatcher goroutine.
	rescanUpdate := []neutrino.UpdateOption{
		neutrino.AddInputs(inputToWatch),
		neutrino.Rewind(currentHeight),
	}

	if err := n.chainView.Update(rescanUpdate...); err != nil {
		return nil, err
	}

	select {
	case n.notificationRegistry <- ntfn:
	case <-n.quit:
		return nil, ErrChainNotifierShuttingDown
	}

	return spendEvent, nil
}

// confirmationNotification represents a client's intent to receive a
// notification once the target txid reaches numConfirmations confirmations.
type confirmationsNotification struct {
	chainntnfs.ConfNtfn
	heightHint uint32
	pkScript   []byte
}

// RegisterConfirmationsNtfn registers a notification with NeutrinoNotifier
// which will be triggered once the txid reaches numConfs number of
// confirmations.
func (n *NeutrinoNotifier) RegisterConfirmationsNtfn(txid *chainhash.Hash,
	pkScript []byte,
	numConfs, heightHint uint32) (*chainntnfs.ConfirmationEvent, error) {

	ntfn := &confirmationsNotification{
		ConfNtfn: chainntnfs.ConfNtfn{
			TxID:             txid,
			NumConfirmations: numConfs,
			Event:            chainntnfs.NewConfirmationEvent(numConfs),
		},
		heightHint: heightHint,
		pkScript:   pkScript,
	}

	select {
	case <-n.quit:
		return nil, ErrChainNotifierShuttingDown
	case n.notificationRegistry <- ntfn:
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

// epochCancel is a message sent to the NeutrinoNotifier when a client wishes
// to cancel an outstanding epoch notification that has yet to be dispatched.
type epochCancel struct {
	epochID uint64
}

// RegisterBlockEpochNtfn returns a BlockEpochEvent which subscribes the caller
// to receive notifications, of each new block connected to the main chain.
func (n *NeutrinoNotifier) RegisterBlockEpochNtfn() (*chainntnfs.BlockEpochEvent, error) {
	reg := &blockEpochRegistration{
		epochQueue: chainntnfs.NewConcurrentQueue(20),
		epochChan:  make(chan *chainntnfs.BlockEpoch, 20),
		cancelChan: make(chan struct{}),
		epochID:    atomic.AddUint64(&n.epochClientCounter, 1),
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
