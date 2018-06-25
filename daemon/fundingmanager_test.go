// +build !rpctest

package main

import (
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/btcsuite/btclog"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/contractcourt"
	"github.com/lightningnetwork/lnd/htlcswitch"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/roasbeef/btcd/chaincfg"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
	_ "github.com/roasbeef/btcwallet/walletdb/bdb"

	"github.com/roasbeef/btcd/btcec"
	"github.com/roasbeef/btcd/wire"
	"github.com/roasbeef/btcutil"
)

const (
	// testPollNumTries is the number of times we attempt to query
	// for a certain expected database state before we give up and
	// consider the test failed. Since it sometimes can take a
	// while to update the database, we poll a certain amount of
	// times, until it gets into the state we expect, or we are out
	// of tries.
	testPollNumTries = 10

	// testPollSleepMs is the number of milliseconds to sleep between
	// each attempt to access the database to check its state.
	testPollSleepMs = 500
)

var (
	privPass = []byte("dummy-pass")

	// Use hard-coded keys for Alice and Bob, the two FundingManagers that
	// we will test the interaction between.
	alicePrivKeyBytes = [32]byte{
		0xb7, 0x94, 0x38, 0x5f, 0x2d, 0x1e, 0xf7, 0xab,
		0x4d, 0x92, 0x73, 0xd1, 0x90, 0x63, 0x81, 0xb4,
		0x4f, 0x2f, 0x6f, 0x25, 0x88, 0xa3, 0xef, 0xb9,
		0x6a, 0x49, 0x18, 0x83, 0x31, 0x98, 0x47, 0x53,
	}

	alicePrivKey, alicePubKey = btcec.PrivKeyFromBytes(btcec.S256(),
		alicePrivKeyBytes[:])

	aliceTCPAddr, _ = net.ResolveTCPAddr("tcp", "10.0.0.2:9001")

	aliceAddr = &lnwire.NetAddress{
		IdentityKey: alicePubKey,
		Address:     aliceTCPAddr,
	}

	bobPrivKeyBytes = [32]byte{
		0x81, 0xb6, 0x37, 0xd8, 0xfc, 0xd2, 0xc6, 0xda,
		0x63, 0x59, 0xe6, 0x96, 0x31, 0x13, 0xa1, 0x17,
		0xd, 0xe7, 0x95, 0xe4, 0xb7, 0x25, 0xb8, 0x4d,
		0x1e, 0xb, 0x4c, 0xfd, 0x9e, 0xc5, 0x8c, 0xe9,
	}

	bobPrivKey, bobPubKey = btcec.PrivKeyFromBytes(btcec.S256(),
		bobPrivKeyBytes[:])

	bobTCPAddr, _ = net.ResolveTCPAddr("tcp", "10.0.0.2:9000")

	bobAddr = &lnwire.NetAddress{
		IdentityKey: bobPubKey,
		Address:     bobTCPAddr,
	}

	testSig = &btcec.Signature{
		R: new(big.Int),
		S: new(big.Int),
	}
	_, _ = testSig.R.SetString("63724406601629180062774974542967536251589935445068131219452686511677818569431", 10)
	_, _ = testSig.S.SetString("18801056069249825825291287104931333862866033135609736119018462340006816851118", 10)
)

type mockNotifier struct {
	oneConfChannel chan *chainntnfs.TxConfirmation
	sixConfChannel chan *chainntnfs.TxConfirmation
	epochChan      chan *chainntnfs.BlockEpoch
}

func (m *mockNotifier) RegisterConfirmationsNtfn(txid *chainhash.Hash, numConfs,
	heightHint uint32) (*chainntnfs.ConfirmationEvent, error) {
	if numConfs == 6 {
		return &chainntnfs.ConfirmationEvent{
			Confirmed: m.sixConfChannel,
		}, nil
	}
	return &chainntnfs.ConfirmationEvent{
		Confirmed: m.oneConfChannel,
	}, nil
}

func (m *mockNotifier) RegisterBlockEpochNtfn() (*chainntnfs.BlockEpochEvent, error) {
	return &chainntnfs.BlockEpochEvent{
		Epochs: m.epochChan,
		Cancel: func() {},
	}, nil
}

func (m *mockNotifier) Start() error {
	return nil
}

func (m *mockNotifier) Stop() error {
	return nil
}

func (m *mockNotifier) RegisterSpendNtfn(outpoint *wire.OutPoint,
	heightHint uint32, _ bool) (*chainntnfs.SpendEvent, error) {
	return &chainntnfs.SpendEvent{
		Spend:  make(chan *chainntnfs.SpendDetail),
		Cancel: func() {},
	}, nil
}

type testNode struct {
	privKey         *btcec.PrivateKey
	msgChan         chan lnwire.Message
	announceChan    chan lnwire.Message
	publTxChan      chan *wire.MsgTx
	fundingMgr      *fundingManager
	peer            *peer
	mockNotifier    *mockNotifier
	testDir         string
	shutdownChannel chan struct{}
}

func init() {
	channeldb.UseLogger(btclog.Disabled)
	lnwallet.UseLogger(btclog.Disabled)
	contractcourt.UseLogger(btclog.Disabled)
	fndgLog = btclog.Disabled
}

func createTestWallet(cdb *channeldb.DB, netParams *chaincfg.Params,
	notifier chainntnfs.ChainNotifier, wc lnwallet.WalletController,
	signer lnwallet.Signer, keyRing keychain.SecretKeyRing,
	bio lnwallet.BlockChainIO,
	estimator lnwallet.FeeEstimator) (*lnwallet.LightningWallet, error) {

	wallet, err := lnwallet.NewLightningWallet(lnwallet.Config{
		Database:           cdb,
		Notifier:           notifier,
		SecretKeyRing:      keyRing,
		WalletController:   wc,
		Signer:             signer,
		ChainIO:            bio,
		FeeEstimator:       estimator,
		NetParams:          *netParams,
		DefaultConstraints: defaultBtcChannelConstraints,
	})
	if err != nil {
		return nil, err
	}

	if err := wallet.Startup(); err != nil {
		return nil, err
	}

	return wallet, nil
}

func createTestFundingManager(t *testing.T, privKey *btcec.PrivateKey,
	tempTestDir string) (*testNode, error) {

	netParams := activeNetParams.Params
	estimator := lnwallet.StaticFeeEstimator{FeeRate: 250}

	chainNotifier := &mockNotifier{
		oneConfChannel: make(chan *chainntnfs.TxConfirmation, 1),
		sixConfChannel: make(chan *chainntnfs.TxConfirmation, 1),
		epochChan:      make(chan *chainntnfs.BlockEpoch, 1),
	}

	newChannelsChan := make(chan *newChannelMsg)
	p := &peer{
		newChannels: newChannelsChan,
	}

	sentMessages := make(chan lnwire.Message)
	sentAnnouncements := make(chan lnwire.Message)
	publTxChan := make(chan *wire.MsgTx, 1)
	shutdownChan := make(chan struct{})

	wc := &mockWalletController{
		rootKey:               alicePrivKey,
		publishedTransactions: publTxChan,
	}
	signer := &mockSigner{
		key: alicePrivKey,
	}
	bio := &mockChainIO{}

	dbDir := filepath.Join(tempTestDir, "cdb")
	cdb, err := channeldb.Open(dbDir)
	if err != nil {
		return nil, err
	}

	keyRing := &mockSecretKeyRing{
		rootKey: alicePrivKey,
	}

	lnw, err := createTestWallet(
		cdb, netParams, chainNotifier, wc, signer, keyRing, bio,
		estimator,
	)
	if err != nil {
		t.Fatalf("unable to create test ln wallet: %v", err)
	}

	var chanIDSeed [32]byte

	f, err := newFundingManager(fundingConfig{
		IDKey:        privKey.PubKey(),
		Wallet:       lnw,
		Notifier:     chainNotifier,
		FeeEstimator: estimator,
		SignMessage: func(pubKey *btcec.PublicKey, msg []byte) (*btcec.Signature, error) {
			return testSig, nil
		},
		SendAnnouncement: func(msg lnwire.Message) error {
			select {
			case sentAnnouncements <- msg:
			case <-shutdownChan:
				return fmt.Errorf("shutting down")
			}
			return nil
		},
		CurrentNodeAnnouncement: func() (lnwire.NodeAnnouncement, error) {
			return lnwire.NodeAnnouncement{}, nil
		},
		SendToPeer: func(target *btcec.PublicKey, msgs ...lnwire.Message) error {
			select {
			case sentMessages <- msgs[0]:
			case <-shutdownChan:
				return fmt.Errorf("shutting down")
			}
			return nil
		},
		NotifyWhenOnline: func(peer *btcec.PublicKey, connectedChan chan<- struct{}) {
			t.Fatalf("did not expect fundingManager to call NotifyWhenOnline")
		},
		FindPeer: func(peerKey *btcec.PublicKey) (*peer, error) {
			return p, nil
		},
		TempChanIDSeed: chanIDSeed,
		FindChannel: func(chanID lnwire.ChannelID) (*lnwallet.LightningChannel, error) {
			dbChannels, err := cdb.FetchAllChannels()
			if err != nil {
				return nil, err
			}

			for _, channel := range dbChannels {
				if chanID.IsChanPoint(&channel.FundingOutpoint) {
					return lnwallet.NewLightningChannel(
						signer,
						nil,
						channel)
				}
			}

			return nil, fmt.Errorf("unable to find channel")
		},
		DefaultRoutingPolicy: htlcswitch.ForwardingPolicy{
			MinHTLC:       5,
			BaseFee:       100,
			FeeRate:       1000,
			TimeLockDelta: 10,
		},
		NumRequiredConfs: func(chanAmt btcutil.Amount,
			pushAmt lnwire.MilliSatoshi) uint16 {
			return 3
		},
		RequiredRemoteDelay: func(amt btcutil.Amount) uint16 {
			return 4
		},
		RequiredRemoteChanReserve: func(chanAmt,
			dustLimit btcutil.Amount) btcutil.Amount {

			reserve := chanAmt / 100
			if reserve < dustLimit {
				reserve = dustLimit
			}

			return reserve
		},
		RequiredRemoteMaxValue: func(chanAmt btcutil.Amount) lnwire.MilliSatoshi {
			reserve := lnwire.NewMSatFromSatoshis(chanAmt / 100)
			return lnwire.NewMSatFromSatoshis(chanAmt) - reserve
		},
		RequiredRemoteMaxHTLCs: func(chanAmt btcutil.Amount) uint16 {
			return uint16(lnwallet.MaxHTLCNumber / 2)
		},
		WatchNewChannel: func(*channeldb.OpenChannel, *lnwire.NetAddress) error {
			return nil
		},
		ReportShortChanID: func(wire.OutPoint) error {
			return nil
		},
		ZombieSweeperInterval: 1 * time.Hour,
		ReservationTimeout:    1 * time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("failed creating fundingManager: %v", err)
	}

	if err = f.Start(); err != nil {
		t.Fatalf("failed starting fundingManager: %v", err)
	}

	return &testNode{
		privKey:         privKey,
		msgChan:         sentMessages,
		announceChan:    sentAnnouncements,
		publTxChan:      publTxChan,
		fundingMgr:      f,
		peer:            p,
		mockNotifier:    chainNotifier,
		testDir:         tempTestDir,
		shutdownChannel: shutdownChan,
	}, nil
}

func recreateAliceFundingManager(t *testing.T, alice *testNode) {
	// Stop the old fundingManager before creating a new one.
	close(alice.shutdownChannel)
	if err := alice.fundingMgr.Stop(); err != nil {
		t.Fatalf("unable to stop old fundingManager: %v", err)
	}

	aliceMsgChan := make(chan lnwire.Message)
	aliceAnnounceChan := make(chan lnwire.Message)
	shutdownChan := make(chan struct{})
	publishChan := make(chan *wire.MsgTx, 10)

	oldCfg := alice.fundingMgr.cfg

	f, err := newFundingManager(fundingConfig{
		IDKey:        oldCfg.IDKey,
		Wallet:       oldCfg.Wallet,
		Notifier:     oldCfg.Notifier,
		FeeEstimator: oldCfg.FeeEstimator,
		SignMessage: func(pubKey *btcec.PublicKey,
			msg []byte) (*btcec.Signature, error) {
			return testSig, nil
		},
		SendAnnouncement: func(msg lnwire.Message) error {
			select {
			case aliceAnnounceChan <- msg:
			case <-shutdownChan:
				return fmt.Errorf("shutting down")
			}
			return nil
		},
		CurrentNodeAnnouncement: func() (lnwire.NodeAnnouncement, error) {
			return lnwire.NodeAnnouncement{}, nil
		},
		SendToPeer: func(target *btcec.PublicKey,
			msgs ...lnwire.Message) error {
			select {
			case aliceMsgChan <- msgs[0]:
			case <-shutdownChan:
				return fmt.Errorf("shutting down")
			}
			return nil
		},
		NotifyWhenOnline: func(peer *btcec.PublicKey, connectedChan chan<- struct{}) {
			t.Fatalf("did not expect fundingManager to call NotifyWhenOnline")
		},
		FindPeer:       oldCfg.FindPeer,
		TempChanIDSeed: oldCfg.TempChanIDSeed,
		FindChannel:    oldCfg.FindChannel,
		PublishTransaction: func(txn *wire.MsgTx) error {
			publishChan <- txn
			return nil
		},
		ZombieSweeperInterval: oldCfg.ZombieSweeperInterval,
		ReservationTimeout:    oldCfg.ReservationTimeout,
	})
	if err != nil {
		t.Fatalf("failed recreating aliceFundingManager: %v", err)
	}

	alice.fundingMgr = f
	alice.msgChan = aliceMsgChan
	alice.announceChan = aliceAnnounceChan
	alice.publTxChan = publishChan
	alice.shutdownChannel = shutdownChan

	if err = f.Start(); err != nil {
		t.Fatalf("failed starting fundingManager: %v", err)
	}
}

func setupFundingManagers(t *testing.T) (*testNode, *testNode) {
	// We need to set the global config, as fundingManager uses
	// MaxPendingChannels, and it is usually set in lndMain().
	cfg = &config{
		MaxPendingChannels: defaultMaxPendingChannels,
	}

	aliceTestDir, err := ioutil.TempDir("", "alicelnwallet")
	if err != nil {
		t.Fatalf("unable to create temp directory: %v", err)
	}

	alice, err := createTestFundingManager(t, alicePrivKey, aliceTestDir)
	if err != nil {
		t.Fatalf("failed creating fundingManager: %v", err)
	}

	bobTestDir, err := ioutil.TempDir("", "boblnwallet")
	if err != nil {
		t.Fatalf("unable to create temp directory: %v", err)
	}

	bob, err := createTestFundingManager(t, bobPrivKey, bobTestDir)
	if err != nil {
		t.Fatalf("failed creating fundingManager: %v", err)
	}

	return alice, bob
}

func tearDownFundingManagers(t *testing.T, a, b *testNode) {
	close(a.shutdownChannel)
	close(b.shutdownChannel)

	if err := a.fundingMgr.Stop(); err != nil {
		t.Fatalf("unable to stop fundingManager: %v", err)
	}
	if err := b.fundingMgr.Stop(); err != nil {
		t.Fatalf("unable to stop fundingManager: %v", err)
	}
	os.RemoveAll(a.testDir)
	os.RemoveAll(b.testDir)
}

// openChannel takes the funding process to the point where the funding
// transaction is confirmed on-chain. Returns the funding out point.
func openChannel(t *testing.T, alice, bob *testNode, localFundingAmt,
	pushAmt btcutil.Amount, numConfs uint32,
	updateChan chan *lnrpc.OpenStatusUpdate, announceChan bool) *wire.OutPoint {
	// Create a funding request and start the workflow.
	errChan := make(chan error, 1)
	initReq := &openChanReq{
		targetPubkey:    bob.privKey.PubKey(),
		chainHash:       *activeNetParams.GenesisHash,
		localFundingAmt: localFundingAmt,
		pushAmt:         lnwire.NewMSatFromSatoshis(pushAmt),
		private:         !announceChan,
		updates:         updateChan,
		err:             errChan,
	}

	alice.fundingMgr.initFundingWorkflow(bobAddr, initReq)

	// Alice should have sent the OpenChannel message to Bob.
	var aliceMsg lnwire.Message
	select {
	case aliceMsg = <-alice.msgChan:
	case err := <-initReq.err:
		t.Fatalf("error init funding workflow: %v", err)
	case <-time.After(time.Second * 5):
		t.Fatalf("alice did not send OpenChannel message")
	}

	openChannelReq, ok := aliceMsg.(*lnwire.OpenChannel)
	if !ok {
		errorMsg, gotError := aliceMsg.(*lnwire.Error)
		if gotError {
			t.Fatalf("expected OpenChannel to be sent "+
				"from bob, instead got error: %v",
				lnwire.ErrorCode(errorMsg.Data[0]))
		}
		t.Fatalf("expected OpenChannel to be sent from "+
			"alice, instead got %T", aliceMsg)
	}

	// Let Bob handle the init message.
	bob.fundingMgr.processFundingOpen(openChannelReq, aliceAddr)

	// Bob should answer with an AcceptChannel message.
	acceptChannelResponse := assertFundingMsgSent(
		t, bob.msgChan, "AcceptChannel",
	).(*lnwire.AcceptChannel)

	// Forward the response to Alice.
	alice.fundingMgr.processFundingAccept(acceptChannelResponse, bobAddr)

	// Alice responds with a FundingCreated message.
	fundingCreated := assertFundingMsgSent(
		t, alice.msgChan, "FundingCreated",
	).(*lnwire.FundingCreated)

	// Give the message to Bob.
	bob.fundingMgr.processFundingCreated(fundingCreated, aliceAddr)

	// Finally, Bob should send the FundingSigned message.
	fundingSigned := assertFundingMsgSent(
		t, bob.msgChan, "FundingSigned",
	).(*lnwire.FundingSigned)

	// Forward the signature to Alice.
	alice.fundingMgr.processFundingSigned(fundingSigned, bobAddr)

	// After Alice processes the singleFundingSignComplete message, she will
	// broadcast the funding transaction to the network. We expect to get a
	// channel update saying the channel is pending.
	var pendingUpdate *lnrpc.OpenStatusUpdate
	select {
	case pendingUpdate = <-updateChan:
	case <-time.After(time.Second * 5):
		t.Fatalf("alice did not send OpenStatusUpdate_ChanPending")
	}

	_, ok = pendingUpdate.Update.(*lnrpc.OpenStatusUpdate_ChanPending)
	if !ok {
		t.Fatal("OpenStatusUpdate was not OpenStatusUpdate_ChanPending")
	}

	// Get and return the transaction Alice published to the network.
	var publ *wire.MsgTx
	select {
	case publ = <-alice.publTxChan:
	case <-time.After(time.Second * 5):
		t.Fatalf("alice did not publish funding tx")
	}

	fundingOutPoint := &wire.OutPoint{
		Hash:  publ.TxHash(),
		Index: 0,
	}
	return fundingOutPoint
}

func assertErrorNotSent(t *testing.T, msgChan chan lnwire.Message) {
	select {
	case <-msgChan:
		t.Fatalf("error sent unexpectedly")
	case <-time.After(100 * time.Millisecond):
		// Expected, return.
	}
}

func assertErrorSent(t *testing.T, msgChan chan lnwire.Message) {
	var msg lnwire.Message
	select {
	case msg = <-msgChan:
	case <-time.After(time.Second * 5):
		t.Fatalf("node did not send Error message")
	}
	_, ok := msg.(*lnwire.Error)
	if !ok {
		t.Fatalf("expected Error to be sent from "+
			"node, instead got %T", msg)
	}
}

func assertFundingMsgSent(t *testing.T, msgChan chan lnwire.Message,
	msgType string) lnwire.Message {
	var msg lnwire.Message
	select {
	case msg = <-msgChan:
	case <-time.After(time.Second * 5):
		t.Fatalf("peer did not send %s message", msgType)
	}

	var (
		sentMsg lnwire.Message
		ok      bool
	)
	switch msgType {
	case "AcceptChannel":
		sentMsg, ok = msg.(*lnwire.AcceptChannel)
	case "FundingCreated":
		sentMsg, ok = msg.(*lnwire.FundingCreated)
	case "FundingSigned":
		sentMsg, ok = msg.(*lnwire.FundingSigned)
	case "FundingLocked":
		sentMsg, ok = msg.(*lnwire.FundingLocked)
	default:
		t.Fatalf("unknown message type: %s", msgType)
	}

	if !ok {
		errorMsg, gotError := msg.(*lnwire.Error)
		if gotError {
			t.Fatalf("expected %s to be sent, instead got error: %v",
				msgType, lnwire.ErrorCode(errorMsg.Data[0]))
		}
		t.Fatalf("expected %s to be sent, instead got %T",
			msgType, msg)
	}

	return sentMsg
}

func assertNumPendingReservations(t *testing.T, node *testNode,
	peerPubKey *btcec.PublicKey, expectedNum int) {
	serializedPubKey := newSerializedKey(peerPubKey)
	actualNum := len(node.fundingMgr.activeReservations[serializedPubKey])
	if actualNum == expectedNum {
		// Success, return.
		return
	}

	t.Fatalf("Expected node to have %d pending reservations, had %v",
		expectedNum, actualNum)
}

func assertNumPendingChannelsBecomes(t *testing.T, node *testNode, expectedNum int) {
	var numPendingChans int
	for i := 0; i < testPollNumTries; i++ {
		// If this is not the first try, sleep before retrying.
		if i > 0 {
			time.Sleep(testPollSleepMs * time.Millisecond)
		}
		pendingChannels, err := node.fundingMgr.
			cfg.Wallet.Cfg.Database.FetchPendingChannels()
		if err != nil {
			t.Fatalf("unable to fetch pending channels: %v", err)
		}

		numPendingChans = len(pendingChannels)
		if numPendingChans == expectedNum {
			// Success, return.
			return
		}
	}

	t.Fatalf("Expected node to have %d pending channels, had %v",
		expectedNum, numPendingChans)
}

func assertNumPendingChannelsRemains(t *testing.T, node *testNode, expectedNum int) {
	var numPendingChans int
	for i := 0; i < 5; i++ {
		// If this is not the first try, sleep before retrying.
		if i > 0 {
			time.Sleep(200 * time.Millisecond)
		}
		pendingChannels, err := node.fundingMgr.
			cfg.Wallet.Cfg.Database.FetchPendingChannels()
		if err != nil {
			t.Fatalf("unable to fetch pending channels: %v", err)
		}

		numPendingChans = len(pendingChannels)
		if numPendingChans != expectedNum {

			t.Fatalf("Expected node to have %d pending channels, had %v",
				expectedNum, numPendingChans)
		}
	}
}

func assertDatabaseState(t *testing.T, node *testNode,
	fundingOutPoint *wire.OutPoint, expectedState channelOpeningState) {

	var state channelOpeningState
	var err error
	for i := 0; i < testPollNumTries; i++ {
		// If this is not the first try, sleep before retrying.
		if i > 0 {
			time.Sleep(testPollSleepMs * time.Millisecond)
		}
		state, _, err = node.fundingMgr.getChannelOpeningState(
			fundingOutPoint)
		if err != nil && err != ErrChannelNotFound {
			t.Fatalf("unable to get channel state: %v", err)
		}

		// If we found the channel, check if it had the expected state.
		if err != ErrChannelNotFound && state == expectedState {
			// Got expected state, return with success.
			return
		}
	}

	// 10 tries without success.
	if err != nil {
		t.Fatalf("error getting channelOpeningState: %v", err)
	} else {
		t.Fatalf("expected state to be %v, was %v", expectedState,
			state)
	}
}

func assertMarkedOpen(t *testing.T, alice, bob *testNode,
	fundingOutPoint *wire.OutPoint) {
	assertDatabaseState(t, alice, fundingOutPoint, markedOpen)
	assertDatabaseState(t, bob, fundingOutPoint, markedOpen)
}

func assertFundingLockedSent(t *testing.T, alice, bob *testNode,
	fundingOutPoint *wire.OutPoint) {
	assertDatabaseState(t, alice, fundingOutPoint, fundingLockedSent)
	assertDatabaseState(t, bob, fundingOutPoint, fundingLockedSent)
}

func assertAddedToRouterGraph(t *testing.T, alice, bob *testNode,
	fundingOutPoint *wire.OutPoint) {
	assertDatabaseState(t, alice, fundingOutPoint, addedToRouterGraph)
	assertDatabaseState(t, bob, fundingOutPoint, addedToRouterGraph)
}

func assertChannelAnnouncements(t *testing.T, alice, bob *testNode) {
	// After the FundingLocked message is sent, Alice and Bob will each
	// send the following messages to their gossiper:
	//	1) ChannelAnnouncement
	//	2) ChannelUpdate
	// The ChannelAnnouncement is kept locally, while the ChannelUpdate
	// is sent directly to the other peer, so the edge policies are
	// known to both peers.
	for j, node := range []*testNode{alice, bob} {
		announcements := make([]lnwire.Message, 2)
		for i := 0; i < len(announcements); i++ {
			select {
			case announcements[i] = <-node.announceChan:
			case <-time.After(time.Second * 5):
				t.Fatalf("node did not send announcement: %v", i)
			}
		}

		gotChannelAnnouncement := false
		gotChannelUpdate := false
		for _, msg := range announcements {
			switch msg.(type) {
			case *lnwire.ChannelAnnouncement:
				gotChannelAnnouncement = true
			case *lnwire.ChannelUpdate:
				gotChannelUpdate = true
			}
		}

		if !gotChannelAnnouncement {
			t.Fatalf("did not get ChannelAnnouncement from node %d",
				j)
		}
		if !gotChannelUpdate {
			t.Fatalf("did not get ChannelUpdate from node %d", j)
		}

		// Make sure no other message is sent.
		select {
		case <-node.announceChan:
			t.Fatalf("received unexpected announcement")
		case <-time.After(300 * time.Millisecond):
			// Expected
		}
	}
}

func assertAnnouncementSignatures(t *testing.T, alice, bob *testNode) {
	// After the FundingLocked message is sent and six confirmations have
	// been reached, the channel will be announced to the greater network
	// by having the nodes exchange announcement signatures.
	// Two distinct messages will be sent:
	//	1) AnnouncementSignatures
	//	2) NodeAnnouncement
	// These may arrive in no particular order.
	// Note that sending the NodeAnnouncement at this point is an
	// implementation detail, and not something required by the LN spec.
	for j, node := range []*testNode{alice, bob} {
		announcements := make([]lnwire.Message, 2)
		for i := 0; i < len(announcements); i++ {
			select {
			case announcements[i] = <-node.announceChan:
			case <-time.After(time.Second * 5):
				t.Fatalf("node did not send announcement %v", i)
			}
		}

		gotAnnounceSignatures := false
		gotNodeAnnouncement := false
		for _, msg := range announcements {
			switch msg.(type) {
			case *lnwire.AnnounceSignatures:
				gotAnnounceSignatures = true
			case *lnwire.NodeAnnouncement:
				gotNodeAnnouncement = true
			}
		}

		if !gotAnnounceSignatures {
			t.Fatalf("did not get AnnounceSignatures from node %d",
				j)
		}
		if !gotNodeAnnouncement {
			t.Fatalf("did not get NodeAnnouncement from node %d", j)
		}
	}
}

func waitForOpenUpdate(t *testing.T, updateChan chan *lnrpc.OpenStatusUpdate) {
	var openUpdate *lnrpc.OpenStatusUpdate
	select {
	case openUpdate = <-updateChan:
	case <-time.After(time.Second * 5):
		t.Fatalf("alice did not send OpenStatusUpdate")
	}

	_, ok := openUpdate.Update.(*lnrpc.OpenStatusUpdate_ChanOpen)
	if !ok {
		t.Fatal("OpenStatusUpdate was not OpenStatusUpdate_ChanOpen")
	}
}

func assertNoChannelState(t *testing.T, alice, bob *testNode,
	fundingOutPoint *wire.OutPoint) {

	assertErrChannelNotFound(t, alice, fundingOutPoint)
	assertErrChannelNotFound(t, bob, fundingOutPoint)
}

func assertErrChannelNotFound(t *testing.T, node *testNode,
	fundingOutPoint *wire.OutPoint) {

	var state channelOpeningState
	var err error
	for i := 0; i < testPollNumTries; i++ {
		// If this is not the first try, sleep before retrying.
		if i > 0 {
			time.Sleep(testPollSleepMs * time.Millisecond)
		}
		state, _, err = node.fundingMgr.getChannelOpeningState(
			fundingOutPoint)
		if err == ErrChannelNotFound {
			// Got expected state, return with success.
			return
		} else if err != nil {
			t.Fatalf("unable to get channel state: %v", err)
		}
	}

	// 10 tries without success.
	t.Fatalf("expected to not find state, found state %v", state)
}

func assertHandleFundingLocked(t *testing.T, alice, bob *testNode) {
	// They should both send the new channel state to their peer.
	select {
	case c := <-alice.peer.newChannels:
		close(c.done)
	case <-time.After(time.Second * 15):
		t.Fatalf("alice did not send new channel to peer")
	}

	select {
	case c := <-bob.peer.newChannels:
		close(c.done)
	case <-time.After(time.Second * 15):
		t.Fatalf("bob did not send new channel to peer")
	}
}

func TestFundingManagerNormalWorkflow(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// We will consume the channel updates as we go, so no buffering is needed.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)

	// Run through the process of opening the channel, up until the funding
	// transaction is broadcasted.
	fundingOutPoint := openChannel(t, alice, bob, 500000, 0, 1, updateChan,
		true)

	// Make sure both reservations time out and then run both zombie sweepers.
	time.Sleep(1 * time.Millisecond)
	go alice.fundingMgr.pruneZombieReservations()
	go bob.fundingMgr.pruneZombieReservations()

	// Check that neither Alice nor Bob sent an error message.
	assertErrorNotSent(t, alice.msgChan)
	assertErrorNotSent(t, bob.msgChan)

	// Check that neither reservation has been pruned.
	assertNumPendingReservations(t, alice, bobPubKey, 1)
	assertNumPendingReservations(t, bob, alicePubKey, 1)

	// Notify that transaction was mined.
	alice.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}

	// The funding transaction was mined, so assert that both funding
	// managers now have the state of this channel 'markedOpen' in their
	// internal state machine.
	assertMarkedOpen(t, alice, bob, fundingOutPoint)

	// After the funding transaction is mined, Alice will send
	// fundingLocked to Bob.
	fundingLockedAlice := assertFundingMsgSent(
		t, alice.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// And similarly Bob will send funding locked to Alice.
	fundingLockedBob := assertFundingMsgSent(
		t, bob.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// Check that the state machine is updated accordingly
	assertFundingLockedSent(t, alice, bob, fundingOutPoint)

	// Make sure both fundingManagers send the expected channel
	// announcements.
	assertChannelAnnouncements(t, alice, bob)

	// Check that the state machine is updated accordingly
	assertAddedToRouterGraph(t, alice, bob, fundingOutPoint)

	// The funding transaction is now confirmed, wait for the
	// OpenStatusUpdate_ChanOpen update
	waitForOpenUpdate(t, updateChan)

	// Exchange the fundingLocked messages.
	alice.fundingMgr.processFundingLocked(fundingLockedBob, bobAddr)
	bob.fundingMgr.processFundingLocked(fundingLockedAlice, aliceAddr)

	// Check that they notify the breach arbiter and peer about the new
	// channel.
	assertHandleFundingLocked(t, alice, bob)

	// Notify that six confirmations has been reached on funding transaction.
	alice.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}

	// Make sure the fundingManagers exchange announcement signatures.
	assertAnnouncementSignatures(t, alice, bob)

	// The internal state-machine should now have deleted the channelStates
	// from the database, as the channel is announced.
	assertNoChannelState(t, alice, bob, fundingOutPoint)
}

func TestFundingManagerRestartBehavior(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// Run through the process of opening the channel, up until the funding
	// transaction is broadcasted.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)
	fundingOutPoint := openChannel(t, alice, bob, 500000, 0, 1, updateChan,
		true)

	// After the funding transaction gets mined, both nodes will send the
	// fundingLocked message to the other peer. If the funding node fails
	// before this message has been successfully sent, it should retry
	// sending it on restart. We mimic this behavior by letting the
	// SendToPeer method return an error, as if the message was not
	// successfully sent. We then recreate the fundingManager and make sure
	// it continues the process as expected.
	alice.fundingMgr.cfg.SendToPeer = func(target *btcec.PublicKey,
		msgs ...lnwire.Message) error {
		return fmt.Errorf("intentional error in SendToPeer")
	}
	alice.fundingMgr.cfg.NotifyWhenOnline = func(peer *btcec.PublicKey, con chan<- struct{}) {
		// Intentionally empty.
	}

	// Notify that transaction was mined
	alice.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}

	// The funding transaction was mined, so assert that both funding
	// managers now have the state of this channel 'markedOpen' in their
	// internal state machine.
	assertMarkedOpen(t, alice, bob, fundingOutPoint)

	// After the funding transaction was mined, Bob should have successfully
	// sent the fundingLocked message, while Alice failed sending it. In
	// Alice's case this means that there should be no messages for Bob, and
	// the channel should still be in state 'markedOpen'
	select {
	case msg := <-alice.msgChan:
		t.Fatalf("did not expect any message from Alice: %v", msg)
	default:
		// Expected.
	}

	// Bob will send funding locked to Alice.
	fundingLockedBob := assertFundingMsgSent(
		t, bob.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// Alice should still be markedOpen
	assertDatabaseState(t, alice, fundingOutPoint, markedOpen)

	// While Bob successfully sent fundingLocked.
	assertDatabaseState(t, bob, fundingOutPoint, fundingLockedSent)

	// We now recreate Alice's fundingManager, and expect it to retry
	// sending the fundingLocked message.
	recreateAliceFundingManager(t, alice)

	// Intentionally make the channel announcements fail
	alice.fundingMgr.cfg.SendAnnouncement = func(msg lnwire.Message) error {
		return fmt.Errorf("intentional error in SendAnnouncement")
	}

	fundingLockedAlice := assertFundingMsgSent(
		t, alice.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// The state should now be fundingLockedSent
	assertDatabaseState(t, alice, fundingOutPoint, fundingLockedSent)

	// Check that the channel announcements were never sent
	select {
	case ann := <-alice.announceChan:
		t.Fatalf("unexpectedly got channel announcement message: %v",
			ann)
	default:
		// Expected
	}

	// Exchange the fundingLocked messages.
	alice.fundingMgr.processFundingLocked(fundingLockedBob, bobAddr)
	bob.fundingMgr.processFundingLocked(fundingLockedAlice, aliceAddr)

	// Check that they notify the breach arbiter and peer about the new
	// channel.
	assertHandleFundingLocked(t, alice, bob)

	// Next up, we check that Alice rebroadcasts the announcement
	// messages on restart. Bob should as expected send announcements.
	recreateAliceFundingManager(t, alice)
	time.Sleep(300 * time.Millisecond)

	// Make sure both fundingManagers send the expected channel
	// announcements.
	assertChannelAnnouncements(t, alice, bob)

	// Check that the state machine is updated accordingly
	assertAddedToRouterGraph(t, alice, bob, fundingOutPoint)

	// Next, we check that Alice sends the announcement signatures
	// on restart after six confirmations. Bob should as expected send
	// them as well.
	recreateAliceFundingManager(t, alice)
	time.Sleep(300 * time.Millisecond)

	// Notify that six confirmations has been reached on funding transaction.
	alice.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}

	// Make sure the fundingManagers exchange announcement signatures.
	assertAnnouncementSignatures(t, alice, bob)

	// The internal state-machine should now have deleted the channelStates
	// from the database, as the channel is announced.
	assertNoChannelState(t, alice, bob, fundingOutPoint)
}

// TestFundingManagerOfflinePeer checks that the fundingManager waits for the
// server to notify when the peer comes online, in case sending the
// fundingLocked message fails the first time.
func TestFundingManagerOfflinePeer(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// Run through the process of opening the channel, up until the funding
	// transaction is broadcasted.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)
	fundingOutPoint := openChannel(t, alice, bob, 500000, 0, 1, updateChan,
		true)

	// After the funding transaction gets mined, both nodes will send the
	// fundingLocked message to the other peer. If the funding node fails
	// to send the fundingLocked message to the peer, it should wait for
	// the server to notify it that the peer is back online, and try again.
	alice.fundingMgr.cfg.SendToPeer = func(target *btcec.PublicKey,
		msgs ...lnwire.Message) error {
		return fmt.Errorf("intentional error in SendToPeer")
	}
	peerChan := make(chan *btcec.PublicKey, 1)
	conChan := make(chan chan<- struct{}, 1)
	alice.fundingMgr.cfg.NotifyWhenOnline = func(peer *btcec.PublicKey, connected chan<- struct{}) {
		peerChan <- peer
		conChan <- connected
	}

	// Notify that transaction was mined
	alice.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}

	// The funding transaction was mined, so assert that both funding
	// managers now have the state of this channel 'markedOpen' in their
	// internal state machine.
	assertMarkedOpen(t, alice, bob, fundingOutPoint)

	// After the funding transaction was mined, Bob should have successfully
	// sent the fundingLocked message, while Alice failed sending it. In
	// Alice's case this means that there should be no messages for Bob, and
	// the channel should still be in state 'markedOpen'
	select {
	case msg := <-alice.msgChan:
		t.Fatalf("did not expect any message from Alice: %v", msg)
	default:
		// Expected.
	}

	// Bob will send funding locked to Alice
	fundingLockedBob := assertFundingMsgSent(
		t, bob.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// Alice should still be markedOpen
	assertDatabaseState(t, alice, fundingOutPoint, markedOpen)

	// While Bob successfully sent fundingLocked.
	assertDatabaseState(t, bob, fundingOutPoint, fundingLockedSent)

	// Alice should be waiting for the server to notify when Bob comes back online.
	var peer *btcec.PublicKey
	var con chan<- struct{}
	select {
	case peer = <-peerChan:
		// Expected
	case <-time.After(time.Second * 3):
		t.Fatalf("alice did not register peer with server")
	}

	select {
	case con = <-conChan:
		// Expected
	case <-time.After(time.Second * 3):
		t.Fatalf("alice did not register connectedChan with server")
	}

	if !peer.IsEqual(bobPubKey) {
		t.Fatalf("expected to receive Bob's pubkey (%v), instead got %v",
			bobPubKey, peer)
	}

	// Fix Alice's SendToPeer, and notify that Bob is back online.
	alice.fundingMgr.cfg.SendToPeer = func(target *btcec.PublicKey,
		msgs ...lnwire.Message) error {
		select {
		case alice.msgChan <- msgs[0]:
		case <-alice.shutdownChannel:
			return fmt.Errorf("shutting down")
		}
		return nil
	}
	close(con)

	// This should make Alice send the fundingLocked.
	fundingLockedAlice := assertFundingMsgSent(
		t, alice.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// The state should now be fundingLockedSent
	assertDatabaseState(t, alice, fundingOutPoint, fundingLockedSent)

	// Make sure both fundingManagers send the expected channel
	// announcements.
	assertChannelAnnouncements(t, alice, bob)

	// Check that the state machine is updated accordingly
	assertAddedToRouterGraph(t, alice, bob, fundingOutPoint)

	// The funding transaction is now confirmed, wait for the
	// OpenStatusUpdate_ChanOpen update
	waitForOpenUpdate(t, updateChan)

	// Exchange the fundingLocked messages.
	alice.fundingMgr.processFundingLocked(fundingLockedBob, bobAddr)
	bob.fundingMgr.processFundingLocked(fundingLockedAlice, aliceAddr)

	// Check that they notify the breach arbiter and peer about the new
	// channel.
	assertHandleFundingLocked(t, alice, bob)

	// Notify that six confirmations has been reached on funding transaction.
	alice.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}

	// Make sure both fundingManagers send the expected announcement
	// signatures.
	assertAnnouncementSignatures(t, alice, bob)

	// The internal state-machine should now have deleted the channelStates
	// from the database, as the channel is announced.
	assertNoChannelState(t, alice, bob, fundingOutPoint)
}

// TestFundingManagerPeerTimeoutAfterInitFunding checks that the zombie sweeper
// will properly clean up a zombie reservation that times out after the
// initFundingMsg has been handled.
func TestFundingManagerPeerTimeoutAfterInitFunding(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// We will consume the channel updates as we go, so no buffering is needed.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)

	// Create a funding request and start the workflow.
	errChan := make(chan error, 1)
	initReq := &openChanReq{
		targetPubkey:    bob.privKey.PubKey(),
		chainHash:       *activeNetParams.GenesisHash,
		localFundingAmt: 500000,
		pushAmt:         lnwire.NewMSatFromSatoshis(0),
		private:         false,
		updates:         updateChan,
		err:             errChan,
	}

	alice.fundingMgr.initFundingWorkflow(bobAddr, initReq)

	// Alice should have sent the OpenChannel message to Bob.
	var aliceMsg lnwire.Message
	select {
	case aliceMsg = <-alice.msgChan:
	case err := <-initReq.err:
		t.Fatalf("error init funding workflow: %v", err)
	case <-time.After(time.Second * 5):
		t.Fatalf("alice did not send OpenChannel message")
	}

	_, ok := aliceMsg.(*lnwire.OpenChannel)
	if !ok {
		errorMsg, gotError := aliceMsg.(*lnwire.Error)
		if gotError {
			t.Fatalf("expected OpenChannel to be sent "+
				"from bob, instead got error: %v",
				lnwire.ErrorCode(errorMsg.Data[0]))
		}
		t.Fatalf("expected OpenChannel to be sent from "+
			"alice, instead got %T", aliceMsg)
	}

	// Alice should have a new pending reservation.
	assertNumPendingReservations(t, alice, bobPubKey, 1)

	// Make sure Alice's reservation times out and then run her zombie sweeper.
	time.Sleep(1 * time.Millisecond)
	go alice.fundingMgr.pruneZombieReservations()

	// Alice should have sent an Error message to Bob.
	assertErrorSent(t, alice.msgChan)

	// Alice's zombie reservation should have been pruned.
	assertNumPendingReservations(t, alice, bobPubKey, 0)
}

// TestFundingManagerPeerTimeoutAfterFundingOpen checks that the zombie sweeper
// will properly clean up a zombie reservation that times out after the
// fundingOpenMsg has been handled.
func TestFundingManagerPeerTimeoutAfterFundingOpen(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// We will consume the channel updates as we go, so no buffering is needed.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)

	// Create a funding request and start the workflow.
	errChan := make(chan error, 1)
	initReq := &openChanReq{
		targetPubkey:    bob.privKey.PubKey(),
		chainHash:       *activeNetParams.GenesisHash,
		localFundingAmt: 500000,
		pushAmt:         lnwire.NewMSatFromSatoshis(0),
		private:         false,
		updates:         updateChan,
		err:             errChan,
	}

	alice.fundingMgr.initFundingWorkflow(bobAddr, initReq)

	// Alice should have sent the OpenChannel message to Bob.
	var aliceMsg lnwire.Message
	select {
	case aliceMsg = <-alice.msgChan:
	case err := <-initReq.err:
		t.Fatalf("error init funding workflow: %v", err)
	case <-time.After(time.Second * 5):
		t.Fatalf("alice did not send OpenChannel message")
	}

	openChannelReq, ok := aliceMsg.(*lnwire.OpenChannel)
	if !ok {
		errorMsg, gotError := aliceMsg.(*lnwire.Error)
		if gotError {
			t.Fatalf("expected OpenChannel to be sent "+
				"from bob, instead got error: %v",
				lnwire.ErrorCode(errorMsg.Data[0]))
		}
		t.Fatalf("expected OpenChannel to be sent from "+
			"alice, instead got %T", aliceMsg)
	}

	// Alice should have a new pending reservation.
	assertNumPendingReservations(t, alice, bobPubKey, 1)

	// Let Bob handle the init message.
	bob.fundingMgr.processFundingOpen(openChannelReq, aliceAddr)

	// Bob should answer with an AcceptChannel.
	assertFundingMsgSent(t, bob.msgChan, "AcceptChannel")

	// Bob should have a new pending reservation.
	assertNumPendingReservations(t, bob, alicePubKey, 1)

	// Make sure Bob's reservation times out and then run his zombie sweeper.
	time.Sleep(1 * time.Millisecond)
	go bob.fundingMgr.pruneZombieReservations()

	// Bob should have sent an Error message to Alice.
	assertErrorSent(t, bob.msgChan)

	// Bob's zombie reservation should have been pruned.
	assertNumPendingReservations(t, bob, alicePubKey, 0)
}

// TestFundingManagerPeerTimeoutAfterFundingAccept checks that the zombie sweeper
// will properly clean up a zombie reservation that times out after the
// fundingAcceptMsg has been handled.
func TestFundingManagerPeerTimeoutAfterFundingAccept(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// We will consume the channel updates as we go, so no buffering is needed.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)

	// Create a funding request and start the workflow.
	errChan := make(chan error, 1)
	initReq := &openChanReq{
		targetPubkey:    bob.privKey.PubKey(),
		chainHash:       *activeNetParams.GenesisHash,
		localFundingAmt: 500000,
		pushAmt:         lnwire.NewMSatFromSatoshis(0),
		private:         false,
		updates:         updateChan,
		err:             errChan,
	}

	alice.fundingMgr.initFundingWorkflow(bobAddr, initReq)

	// Alice should have sent the OpenChannel message to Bob.
	var aliceMsg lnwire.Message
	select {
	case aliceMsg = <-alice.msgChan:
	case err := <-initReq.err:
		t.Fatalf("error init funding workflow: %v", err)
	case <-time.After(time.Second * 5):
		t.Fatalf("alice did not send OpenChannel message")
	}

	openChannelReq, ok := aliceMsg.(*lnwire.OpenChannel)
	if !ok {
		errorMsg, gotError := aliceMsg.(*lnwire.Error)
		if gotError {
			t.Fatalf("expected OpenChannel to be sent "+
				"from bob, instead got error: %v",
				lnwire.ErrorCode(errorMsg.Data[0]))
		}
		t.Fatalf("expected OpenChannel to be sent from "+
			"alice, instead got %T", aliceMsg)
	}

	// Alice should have a new pending reservation.
	assertNumPendingReservations(t, alice, bobPubKey, 1)

	// Let Bob handle the init message.
	bob.fundingMgr.processFundingOpen(openChannelReq, aliceAddr)

	// Bob should answer with an AcceptChannel.
	acceptChannelResponse := assertFundingMsgSent(
		t, bob.msgChan, "AcceptChannel",
	).(*lnwire.AcceptChannel)

	// Bob should have a new pending reservation.
	assertNumPendingReservations(t, bob, alicePubKey, 1)

	// Forward the response to Alice.
	alice.fundingMgr.processFundingAccept(acceptChannelResponse, bobAddr)

	// Alice responds with a FundingCreated messages.
	assertFundingMsgSent(t, alice.msgChan, "FundingCreated")

	// Make sure Alice's reservation times out and then run her zombie sweeper.
	time.Sleep(1 * time.Millisecond)
	go alice.fundingMgr.pruneZombieReservations()

	// Alice should have sent an Error message to Bob.
	assertErrorSent(t, alice.msgChan)

	// Alice's zombie reservation should have been pruned.
	assertNumPendingReservations(t, alice, bobPubKey, 0)
}

func TestFundingManagerFundingTimeout(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// We will consume the channel updates as we go, so no buffering is needed.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)

	// Run through the process of opening the channel, up until the funding
	// transaction is broadcasted.
	_ = openChannel(t, alice, bob, 500000, 0, 1, updateChan, true)

	// Bob will at this point be waiting for the funding transaction to be
	// confirmed, so the channel should be considered pending.
	pendingChannels, err := bob.fundingMgr.cfg.Wallet.Cfg.Database.FetchPendingChannels()
	if err != nil {
		t.Fatalf("unable to fetch pending channels: %v", err)
	}
	if len(pendingChannels) != 1 {
		t.Fatalf("Expected Bob to have 1 pending channel, had  %v",
			len(pendingChannels))
	}

	// We expect Bob to forget the channel after 288 blocks (48 hours), so
	// mine 287, and check that it is still pending.
	bob.mockNotifier.epochChan <- &chainntnfs.BlockEpoch{
		Height: fundingBroadcastHeight + 287,
	}

	// Bob should still be waiting for the channel to open.
	assertNumPendingChannelsRemains(t, bob, 1)

	bob.mockNotifier.epochChan <- &chainntnfs.BlockEpoch{
		Height: fundingBroadcastHeight + 288,
	}

	// Bob should have sent an Error message to Alice.
	assertErrorSent(t, bob.msgChan)

	// Should not be pending anymore.
	assertNumPendingChannelsBecomes(t, bob, 0)
}

// TestFundingManagerFundingNotTimeoutInitiator checks that if the user was
// the channel initiator, that it does not timeout when the lnd restarts.
func TestFundingManagerFundingNotTimeoutInitiator(t *testing.T) {

	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// We will consume the channel updates as we go, so no buffering is needed.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)

	// Run through the process of opening the channel, up until the funding
	// transaction is broadcasted.
	_ = openChannel(t, alice, bob, 500000, 0, 1, updateChan, true)

	// Alice will at this point be waiting for the funding transaction to be
	// confirmed, so the channel should be considered pending.
	pendingChannels, err := alice.fundingMgr.cfg.Wallet.Cfg.Database.FetchPendingChannels()
	if err != nil {
		t.Fatalf("unable to fetch pending channels: %v", err)
	}
	if len(pendingChannels) != 1 {
		t.Fatalf("Expected Alice to have 1 pending channel, had  %v",
			len(pendingChannels))
	}

	recreateAliceFundingManager(t, alice)

	// We should receive the rebroadcasted funding txn.
	select {
	case <-alice.publTxChan:
	case <-time.After(time.Second * 5):
		t.Fatalf("alice did not publish funding tx")
	}

	// Increase the height to 1 minus the maxWaitNumBlocksFundingConf height
	alice.mockNotifier.epochChan <- &chainntnfs.BlockEpoch{
		Height: fundingBroadcastHeight + maxWaitNumBlocksFundingConf - 1,
	}

	bob.mockNotifier.epochChan <- &chainntnfs.BlockEpoch{
		Height: fundingBroadcastHeight + maxWaitNumBlocksFundingConf - 1,
	}

	// Assert both and Alice and Bob still have 1 pending channels
	assertNumPendingChannelsRemains(t, alice, 1)

	assertNumPendingChannelsRemains(t, bob, 1)

	// Increase both Alice and Bob to maxWaitNumBlocksFundingConf height
	alice.mockNotifier.epochChan <- &chainntnfs.BlockEpoch{
		Height: fundingBroadcastHeight + maxWaitNumBlocksFundingConf,
	}

	bob.mockNotifier.epochChan <- &chainntnfs.BlockEpoch{
		Height: fundingBroadcastHeight + maxWaitNumBlocksFundingConf,
	}

	// Since Alice was the initiator, the channel should not have timed out
	assertNumPendingChannelsRemains(t, alice, 1)

	// Bob should have sent an Error message to Alice.
	assertErrorSent(t, bob.msgChan)

	// Since Bob was not the initiator, the channel should timeout
	assertNumPendingChannelsBecomes(t, bob, 0)
}

// TestFundingManagerReceiveFundingLockedTwice checks that the fundingManager
// continues to operate as expected in case we receive a duplicate fundingLocked
// message.
func TestFundingManagerReceiveFundingLockedTwice(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// We will consume the channel updates as we go, so no buffering is needed.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)

	// Run through the process of opening the channel, up until the funding
	// transaction is broadcasted.
	fundingOutPoint := openChannel(t, alice, bob, 500000, 0, 1, updateChan,
		true)

	// Notify that transaction was mined
	alice.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}

	// The funding transaction was mined, so assert that both funding
	// managers now have the state of this channel 'markedOpen' in their
	// internal state machine.
	assertMarkedOpen(t, alice, bob, fundingOutPoint)

	// After the funding transaction is mined, Alice will send
	// fundingLocked to Bob.
	fundingLockedAlice := assertFundingMsgSent(
		t, alice.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// And similarly Bob will send funding locked to Alice.
	fundingLockedBob := assertFundingMsgSent(
		t, bob.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// Check that the state machine is updated accordingly
	assertFundingLockedSent(t, alice, bob, fundingOutPoint)

	// Make sure both fundingManagers send the expected channel
	// announcements.
	assertChannelAnnouncements(t, alice, bob)

	// Check that the state machine is updated accordingly
	assertAddedToRouterGraph(t, alice, bob, fundingOutPoint)

	// The funding transaction is now confirmed, wait for the
	// OpenStatusUpdate_ChanOpen update
	waitForOpenUpdate(t, updateChan)

	// Send the fundingLocked message twice to Alice, and once to Bob.
	alice.fundingMgr.processFundingLocked(fundingLockedBob, bobAddr)
	alice.fundingMgr.processFundingLocked(fundingLockedBob, bobAddr)
	bob.fundingMgr.processFundingLocked(fundingLockedAlice, aliceAddr)

	// Check that they notify the breach arbiter and peer about the new
	// channel.
	assertHandleFundingLocked(t, alice, bob)

	// Alice should not send the channel state the second time, as the
	// second funding locked should just be ignored.
	select {
	case <-alice.peer.newChannels:
		t.Fatalf("alice sent new channel to peer a second time")
	case <-time.After(time.Millisecond * 300):
		// Expected
	}

	// Another fundingLocked should also be ignored, since Alice should
	// have updated her database at this point.
	alice.fundingMgr.processFundingLocked(fundingLockedBob, bobAddr)
	select {
	case <-alice.peer.newChannels:
		t.Fatalf("alice sent new channel to peer a second time")
	case <-time.After(time.Millisecond * 300):
		// Expected
	}

	// Notify that six confirmations has been reached on funding transaction.
	alice.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}

	// Make sure the fundingManagers exchange announcement signatures.
	assertAnnouncementSignatures(t, alice, bob)

	// The internal state-machine should now have deleted the channelStates
	// from the database, as the channel is announced.
	assertNoChannelState(t, alice, bob, fundingOutPoint)
}

// TestFundingManagerRestartAfterChanAnn checks that the fundingManager properly
// handles receiving a fundingLocked after the its own fundingLocked and channel
// announcement is sent and gets restarted.
func TestFundingManagerRestartAfterChanAnn(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// We will consume the channel updates as we go, so no buffering is needed.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)

	// Run through the process of opening the channel, up until the funding
	// transaction is broadcasted.
	fundingOutPoint := openChannel(t, alice, bob, 500000, 0, 1, updateChan,
		true)

	// Notify that transaction was mined
	alice.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}

	// The funding transaction was mined, so assert that both funding
	// managers now have the state of this channel 'markedOpen' in their
	// internal state machine.
	assertMarkedOpen(t, alice, bob, fundingOutPoint)

	// After the funding transaction is mined, Alice will send
	// fundingLocked to Bob.
	fundingLockedAlice := assertFundingMsgSent(
		t, alice.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// And similarly Bob will send funding locked to Alice.
	fundingLockedBob := assertFundingMsgSent(
		t, bob.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// Check that the state machine is updated accordingly
	assertFundingLockedSent(t, alice, bob, fundingOutPoint)

	// Make sure both fundingManagers send the expected channel
	// announcements.
	assertChannelAnnouncements(t, alice, bob)

	// Check that the state machine is updated accordingly
	assertAddedToRouterGraph(t, alice, bob, fundingOutPoint)

	// The funding transaction is now confirmed, wait for the
	// OpenStatusUpdate_ChanOpen update
	waitForOpenUpdate(t, updateChan)

	// At this point we restart Alice's fundingManager, before she receives
	// the fundingLocked message. After restart, she will receive it, and
	// we expect her to be able to handle it correctly.
	recreateAliceFundingManager(t, alice)

	// Exchange the fundingLocked messages.
	alice.fundingMgr.processFundingLocked(fundingLockedBob, bobAddr)
	bob.fundingMgr.processFundingLocked(fundingLockedAlice, aliceAddr)

	// Check that they notify the breach arbiter and peer about the new
	// channel.
	assertHandleFundingLocked(t, alice, bob)

	// Notify that six confirmations has been reached on funding transaction.
	alice.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}

	// Make sure both fundingManagers send the expected channel announcements.
	assertAnnouncementSignatures(t, alice, bob)

	// The internal state-machine should now have deleted the channelStates
	// from the database, as the channel is announced.
	assertNoChannelState(t, alice, bob, fundingOutPoint)
}

// TestFundingManagerRestartAfterReceivingFundingLocked checks that the
// fundingManager continues to operate as expected after it has received
// fundingLocked and then gets restarted.
func TestFundingManagerRestartAfterReceivingFundingLocked(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// We will consume the channel updates as we go, so no buffering is needed.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)

	// Run through the process of opening the channel, up until the funding
	// transaction is broadcasted.
	fundingOutPoint := openChannel(t, alice, bob, 500000, 0, 1, updateChan,
		true)

	// Notify that transaction was mined
	alice.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}

	// The funding transaction was mined, so assert that both funding
	// managers now have the state of this channel 'markedOpen' in their
	// internal state machine.
	assertMarkedOpen(t, alice, bob, fundingOutPoint)

	// After the funding transaction is mined, Alice will send
	// fundingLocked to Bob.
	fundingLockedAlice := assertFundingMsgSent(
		t, alice.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// And similarly Bob will send funding locked to Alice.
	fundingLockedBob := assertFundingMsgSent(
		t, bob.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// Check that the state machine is updated accordingly
	assertFundingLockedSent(t, alice, bob, fundingOutPoint)

	// Let Alice immediately get the fundingLocked message.
	alice.fundingMgr.processFundingLocked(fundingLockedBob, bobAddr)

	// Also let Bob get the fundingLocked message.
	bob.fundingMgr.processFundingLocked(fundingLockedAlice, aliceAddr)

	// Check that they notify the breach arbiter and peer about the new
	// channel.
	assertHandleFundingLocked(t, alice, bob)

	// At this point we restart Alice's fundingManager.
	recreateAliceFundingManager(t, alice)

	// Make sure both fundingManagers send the expected channel
	// announcements.
	assertChannelAnnouncements(t, alice, bob)

	// Check that the state machine is updated accordingly
	assertAddedToRouterGraph(t, alice, bob, fundingOutPoint)

	// Notify that six confirmations has been reached on funding transaction.
	alice.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}

	// Make sure both fundingManagers send the expected channel announcements.
	assertAnnouncementSignatures(t, alice, bob)

	// The internal state-machine should now have deleted the channelStates
	// from the database, as the channel is announced.
	assertNoChannelState(t, alice, bob, fundingOutPoint)
}

// TestFundingManagerPrivateChannel tests that if we open a private channel
// (a channel not supposed to be announced to the rest of the network),
// the announcementSignatures nor the nodeAnnouncement messages are sent.
func TestFundingManagerPrivateChannel(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// We will consume the channel updates as we go, so no buffering is needed.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)

	// Run through the process of opening the channel, up until the funding
	// transaction is broadcasted.
	fundingOutPoint := openChannel(t, alice, bob, 500000, 0, 1, updateChan,
		false)

	// Notify that transaction was mined
	alice.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}

	// The funding transaction was mined, so assert that both funding
	// managers now have the state of this channel 'markedOpen' in their
	// internal state machine.
	assertMarkedOpen(t, alice, bob, fundingOutPoint)

	// After the funding transaction is mined, Alice will send
	// fundingLocked to Bob.
	fundingLockedAlice := assertFundingMsgSent(
		t, alice.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// And similarly Bob will send funding locked to Alice.
	fundingLockedBob := assertFundingMsgSent(
		t, bob.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// Check that the state machine is updated accordingly
	assertFundingLockedSent(t, alice, bob, fundingOutPoint)

	// Make sure both fundingManagers send the expected channel
	// announcements.
	assertChannelAnnouncements(t, alice, bob)

	// The funding transaction is now confirmed, wait for the
	// OpenStatusUpdate_ChanOpen update
	waitForOpenUpdate(t, updateChan)

	// Exchange the fundingLocked messages.
	alice.fundingMgr.processFundingLocked(fundingLockedBob, bobAddr)
	bob.fundingMgr.processFundingLocked(fundingLockedAlice, aliceAddr)

	// Check that they notify the breach arbiter and peer about the new
	// channel.
	assertHandleFundingLocked(t, alice, bob)

	// Notify that six confirmations has been reached on funding transaction.
	alice.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}

	// Since this is a private channel, we shouldn't receive the
	// announcement signatures or node announcement messages.
	select {
	case ann := <-alice.announceChan:
		t.Fatalf("unexpectedly got channel announcement message: %v", ann)
	case <-time.After(300 * time.Millisecond):
		// Expected
	}

	select {
	case ann := <-bob.announceChan:
		t.Fatalf("unexpectedly got channel announcement message: %v", ann)
	case <-time.After(300 * time.Millisecond):
		// Expected
	}

	// The internal state-machine should now have deleted the channelStates
	// from the database, as the channel is announced.
	assertNoChannelState(t, alice, bob, fundingOutPoint)
}

// TestFundingManagerPrivateRestart tests that the privacy guarantees granted
// by the private channel persist even on restart. This means that the
// announcement signatures nor the node announcement messages are sent upon
// restart.
func TestFundingManagerPrivateRestart(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// We will consume the channel updates as we go, so no buffering is needed.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)

	// Run through the process of opening the channel, up until the funding
	// transaction is broadcasted.
	fundingOutPoint := openChannel(t, alice, bob, 500000, 0, 1, updateChan,
		false)

	// Notify that transaction was mined
	alice.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.oneConfChannel <- &chainntnfs.TxConfirmation{}

	// The funding transaction was mined, so assert that both funding
	// managers now have the state of this channel 'markedOpen' in their
	// internal state machine.
	assertMarkedOpen(t, alice, bob, fundingOutPoint)

	// After the funding transaction is mined, Alice will send
	// fundingLocked to Bob.
	fundingLockedAlice := assertFundingMsgSent(
		t, alice.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// And similarly Bob will send funding locked to Alice.
	fundingLockedBob := assertFundingMsgSent(
		t, bob.msgChan, "FundingLocked",
	).(*lnwire.FundingLocked)

	// Check that the state machine is updated accordingly
	assertFundingLockedSent(t, alice, bob, fundingOutPoint)

	// Make sure both fundingManagers send the expected channel
	// announcements.
	assertChannelAnnouncements(t, alice, bob)

	// Note: We don't check for the addedToRouterGraph state because in
	// the private channel mode, the state is quickly changed from
	// addedToRouterGraph to deleted from the database since the public
	// announcement phase is skipped.

	// The funding transaction is now confirmed, wait for the
	// OpenStatusUpdate_ChanOpen update
	waitForOpenUpdate(t, updateChan)

	// Exchange the fundingLocked messages.
	alice.fundingMgr.processFundingLocked(fundingLockedBob, bobAddr)
	bob.fundingMgr.processFundingLocked(fundingLockedAlice, aliceAddr)

	// Check that they notify the breach arbiter and peer about the new
	// channel.
	assertHandleFundingLocked(t, alice, bob)

	// Restart Alice's fundingManager so we can prove that the public
	// channel announcements are not sent upon restart and that the private
	// setting persists upon restart.
	recreateAliceFundingManager(t, alice)
	time.Sleep(300 * time.Millisecond)

	// Notify that six confirmations has been reached on funding transaction.
	alice.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}
	bob.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{}

	// Since this is a private channel, we shouldn't receive the public
	// channel announcement messages announcement signatures or
	// node announcement.
	select {
	case ann := <-alice.announceChan:
		t.Fatalf("unexpectedly got channel announcement message: %v", ann)
	case <-time.After(300 * time.Millisecond):
		// Expected
	}

	select {
	case ann := <-bob.announceChan:
		t.Fatalf("unexpectedly got channel announcement message: %v", ann)
	case <-time.After(300 * time.Millisecond):
		// Expected
	}

	// The internal state-machine should now have deleted the channelStates
	// from the database, as the channel is announced.
	assertNoChannelState(t, alice, bob, fundingOutPoint)
}

// TestFundingManagerCustomChannelParameters checks that custom requirements we
// specify during the channel funding flow is preserved correcly on both sides.
func TestFundingManagerCustomChannelParameters(t *testing.T) {
	alice, bob := setupFundingManagers(t)
	defer tearDownFundingManagers(t, alice, bob)

	// This is the custom parameters we'll use.
	const csvDelay = 67
	const minHtlc = 1234

	// We will consume the channel updates as we go, so no buffering is
	// needed.
	updateChan := make(chan *lnrpc.OpenStatusUpdate)

	// Create a funding request with the custom parameters and start the
	// workflow.
	errChan := make(chan error, 1)
	initReq := &openChanReq{
		targetPubkey:    bob.privKey.PubKey(),
		chainHash:       *activeNetParams.GenesisHash,
		localFundingAmt: 5000000,
		pushAmt:         lnwire.NewMSatFromSatoshis(0),
		private:         false,
		minHtlc:         minHtlc,
		remoteCsvDelay:  csvDelay,
		updates:         updateChan,
		err:             errChan,
	}

	alice.fundingMgr.initFundingWorkflow(bobAddr, initReq)

	// Alice should have sent the OpenChannel message to Bob.
	var aliceMsg lnwire.Message
	select {
	case aliceMsg = <-alice.msgChan:
	case err := <-initReq.err:
		t.Fatalf("error init funding workflow: %v", err)
	case <-time.After(time.Second * 5):
		t.Fatalf("alice did not send OpenChannel message")
	}

	openChannelReq, ok := aliceMsg.(*lnwire.OpenChannel)
	if !ok {
		errorMsg, gotError := aliceMsg.(*lnwire.Error)
		if gotError {
			t.Fatalf("expected OpenChannel to be sent "+
				"from bob, instead got error: %v",
				lnwire.ErrorCode(errorMsg.Data[0]))
		}
		t.Fatalf("expected OpenChannel to be sent from "+
			"alice, instead got %T", aliceMsg)
	}

	// Check that the custom CSV delay is sent as part of OpenChannel.
	if openChannelReq.CsvDelay != csvDelay {
		t.Fatalf("expected OpenChannel to have CSV delay %v, got %v",
			csvDelay, openChannelReq.CsvDelay)
	}

	// Check that the custom minHTLC value is sent.
	if openChannelReq.HtlcMinimum != minHtlc {
		t.Fatalf("expected OpenChannel to have minHtlc %v, got %v",
			minHtlc, openChannelReq.HtlcMinimum)
	}

	chanID := openChannelReq.PendingChannelID

	// Let Bob handle the init message.
	bob.fundingMgr.processFundingOpen(openChannelReq, aliceAddr)

	// Bob should answer with an AcceptChannel message.
	acceptChannelResponse := assertFundingMsgSent(
		t, bob.msgChan, "AcceptChannel",
	).(*lnwire.AcceptChannel)

	// Bob should require the default delay of 4.
	if acceptChannelResponse.CsvDelay != 4 {
		t.Fatalf("expected AcceptChannel to have CSV delay %v, got %v",
			4, acceptChannelResponse.CsvDelay)
	}

	// And the default MinHTLC value of 5.
	if acceptChannelResponse.HtlcMinimum != 5 {
		t.Fatalf("expected AcceptChannel to have minHtlc %v, got %v",
			5, acceptChannelResponse.HtlcMinimum)
	}

	// Forward the response to Alice.
	alice.fundingMgr.processFundingAccept(acceptChannelResponse, bobAddr)

	// Alice responds with a FundingCreated message.
	fundingCreated := assertFundingMsgSent(
		t, alice.msgChan, "FundingCreated",
	).(*lnwire.FundingCreated)

	// Give the message to Bob.
	bob.fundingMgr.processFundingCreated(fundingCreated, aliceAddr)

	// Finally, Bob should send the FundingSigned message.
	fundingSigned := assertFundingMsgSent(
		t, bob.msgChan, "FundingSigned",
	).(*lnwire.FundingSigned)

	// Forward the signature to Alice.
	alice.fundingMgr.processFundingSigned(fundingSigned, bobAddr)

	// After Alice processes the singleFundingSignComplete message, she will
	// broadcast the funding transaction to the network. We expect to get a
	// channel update saying the channel is pending.
	var pendingUpdate *lnrpc.OpenStatusUpdate
	select {
	case pendingUpdate = <-updateChan:
	case <-time.After(time.Second * 5):
		t.Fatalf("alice did not send OpenStatusUpdate_ChanPending")
	}

	_, ok = pendingUpdate.Update.(*lnrpc.OpenStatusUpdate_ChanPending)
	if !ok {
		t.Fatal("OpenStatusUpdate was not OpenStatusUpdate_ChanPending")
	}

	// Wait for Alice to published the funding tx to the network.
	select {
	case <-alice.publTxChan:
	case <-time.After(time.Second * 5):
		t.Fatalf("alice did not publish funding tx")
	}

	// Helper method for checking the CSV delay stored for a reservation.
	assertDelay := func(resCtx *reservationWithCtx,
		ourDelay, theirDelay uint16) error {

		ourCsvDelay := resCtx.reservation.OurContribution().CsvDelay
		if ourCsvDelay != ourDelay {
			return fmt.Errorf("expected our CSV delay to be %v, "+
				"was %v", ourDelay, ourCsvDelay)
		}

		theirCsvDelay := resCtx.reservation.TheirContribution().CsvDelay
		if theirCsvDelay != theirDelay {
			return fmt.Errorf("expected their CSV delay to be %v, "+
				"was %v", theirDelay, theirCsvDelay)
		}
		return nil
	}

	// Helper method for checking the MinHtlc value stored for a
	// reservation.
	assertMinHtlc := func(resCtx *reservationWithCtx,
		expOurMinHtlc, expTheirMinHtlc lnwire.MilliSatoshi) error {

		ourMinHtlc := resCtx.reservation.OurContribution().MinHTLC
		if ourMinHtlc != expOurMinHtlc {
			return fmt.Errorf("expected our minHtlc to be %v, "+
				"was %v", expOurMinHtlc, ourMinHtlc)
		}

		theirMinHtlc := resCtx.reservation.TheirContribution().MinHTLC
		if theirMinHtlc != expTheirMinHtlc {
			return fmt.Errorf("expected their minHtlc to be %v, "+
				"was %v", expTheirMinHtlc, theirMinHtlc)
		}
		return nil
	}

	// Check that the custom channel parameters were properly set in the
	// channel reservation.
	resCtx, err := alice.fundingMgr.getReservationCtx(bobPubKey, chanID)
	if err != nil {
		t.Fatalf("unable to find ctx: %v", err)
	}

	// Alice's CSV delay should be 4 since Bob sent the fedault value, and
	// Bob's should be 67 since Alice sent the custom value.
	if err := assertDelay(resCtx, 4, csvDelay); err != nil {
		t.Fatal(err)
	}

	// The minimum HTLC value Alice can offer should be 5, and the minimum
	// Bob can offer should be 1234.
	if err := assertMinHtlc(resCtx, 5, minHtlc); err != nil {
		t.Fatal(err)
	}

	// Also make sure the parameters are properly set on Bob's end.
	resCtx, err = bob.fundingMgr.getReservationCtx(alicePubKey, chanID)
	if err != nil {
		t.Fatalf("unable to find ctx: %v", err)
	}

	if err := assertDelay(resCtx, csvDelay, 4); err != nil {
		t.Fatal(err)
	}

	if err := assertMinHtlc(resCtx, minHtlc, 5); err != nil {
		t.Fatal(err)
	}
}
