package htlcswitch

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/breez/lightninglib/chainntnfs"
	"github.com/breez/lightninglib/channeldb"
	"github.com/breez/lightninglib/contractcourt"
	"github.com/breez/lightninglib/lnrpc"
	"github.com/breez/lightninglib/lnwallet"
	"github.com/breez/lightninglib/lnwire"
	"github.com/breez/lightninglib/ticker"
	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/coreos/bbolt"
	"github.com/davecgh/go-spew/spew"
)

const (
	// DefaultFwdEventInterval is the duration between attempts to flush
	// pending forwarding events to disk.
	DefaultFwdEventInterval = 15 * time.Second

	// DefaultLogInterval is the duration between attempts to log statistics
	// about forwarding events.
	DefaultLogInterval = 10 * time.Second
)

var (
	// ErrChannelLinkNotFound is used when channel link hasn't been found.
	ErrChannelLinkNotFound = errors.New("channel link not found")

	// ErrDuplicateAdd signals that the ADD htlc was already forwarded
	// through the switch and is locked into another commitment txn.
	ErrDuplicateAdd = errors.New("duplicate add HTLC detected")

	// ErrIncompleteForward is used when an htlc was already forwarded
	// through the switch, but did not get locked into another commitment
	// txn.
	ErrIncompleteForward = errors.New("incomplete forward detected")

	// ErrUnknownErrorDecryptor signals that we were unable to locate the
	// error decryptor for this payment. This is likely due to restarting
	// the daemon.
	ErrUnknownErrorDecryptor = errors.New("unknown error decryptor")

	// ErrSwitchExiting signaled when the switch has received a shutdown
	// request.
	ErrSwitchExiting = errors.New("htlcswitch shutting down")

	// ErrNoLinksFound is an error returned when we attempt to retrieve the
	// active links in the switch for a specific destination.
	ErrNoLinksFound = errors.New("no channel links found")

	// zeroPreimage is the empty preimage which is returned when we have
	// some errors.
	zeroPreimage [sha256.Size]byte
)

// pendingPayment represents the payment which made by user and waits for
// updates to be received whether the payment has been rejected or proceed
// successfully.
type pendingPayment struct {
	paymentHash lnwallet.PaymentHash
	amount      lnwire.MilliSatoshi

	preimage chan [sha256.Size]byte
	err      chan error

	// deobfuscator is a serializable entity which is used if we received
	// an error, it deobfuscates the onion failure blob, and extracts the
	// exact error from it.
	deobfuscator ErrorDecrypter
}

// plexPacket encapsulates switch packet and adds error channel to receive
// error from request handler.
type plexPacket struct {
	pkt *htlcPacket
	err chan error
}

// ChannelCloseType is an enum which signals the type of channel closure the
// peer should execute.
type ChannelCloseType uint8

const (
	// CloseRegular indicates a regular cooperative channel closure
	// should be attempted.
	CloseRegular ChannelCloseType = iota

	// CloseBreach indicates that a channel breach has been detected, and
	// the link should immediately be marked as unavailable.
	CloseBreach
)

// ChanClose represents a request which close a particular channel specified by
// its id.
type ChanClose struct {
	// CloseType is a variable which signals the type of channel closure the
	// peer should execute.
	CloseType ChannelCloseType

	// ChanPoint represent the id of the channel which should be closed.
	ChanPoint *wire.OutPoint

	// TargetFeePerKw is the ideal fee that was specified by the caller.
	// This value is only utilized if the closure type is CloseRegular.
	// This will be the starting offered fee when the fee negotiation
	// process for the cooperative closure transaction kicks off.
	TargetFeePerKw lnwallet.SatPerKWeight

	// Updates is used by request creator to receive the notifications about
	// execution of the close channel request.
	Updates chan *lnrpc.CloseStatusUpdate

	// Err is used by request creator to receive request execution error.
	Err chan error
}

// Config defines the configuration for the service. ALL elements within the
// configuration MUST be non-nil for the service to carry out its duties.
type Config struct {
	// SelfKey is the key of the backing Lightning node. This key is used
	// to properly craft failure messages, such that the Layer 3 router can
	// properly route around link./vertex failures.
	SelfKey *btcec.PublicKey

	// FwdingLog is an interface that will be used by the switch to log
	// forwarding events. A forwarding event happens each time a payment
	// circuit is successfully completed. So when we forward an HTLC, and a
	// settle is eventually received.
	FwdingLog ForwardingLog

	// LocalChannelClose kicks-off the workflow to execute a cooperative or
	// forced unilateral closure of the channel initiated by a local
	// subsystem.
	LocalChannelClose func(pubKey []byte, request *ChanClose)

	// DB is the channeldb instance that will be used to back the switch's
	// persistent circuit map.
	DB *channeldb.DB

	// SwitchPackager provides access to the forwarding packages of all
	// active channels. This gives the switch the ability to read arbitrary
	// forwarding packages, and ack settles and fails contained within them.
	SwitchPackager channeldb.FwdOperator

	// ExtractErrorEncrypter is an interface allowing switch to reextract
	// error encrypters stored in the circuit map on restarts, since they
	// are not stored directly within the database.
	ExtractErrorEncrypter ErrorEncrypterExtracter

	// FetchLastChannelUpdate retrieves the latest routing policy for a
	// target channel. This channel will typically be the outgoing channel
	// specified when we receive an incoming HTLC.  This will be used to
	// provide payment senders our latest policy when sending encrypted
	// error messages.
	FetchLastChannelUpdate func(lnwire.ShortChannelID) (*lnwire.ChannelUpdate, error)

	// Notifier is an instance of a chain notifier that we'll use to signal
	// the switch when a new block has arrived.
	Notifier chainntnfs.ChainNotifier

	// FwdEventTicker is a signal that instructs the htlcswitch to flush any
	// pending forwarding events.
	FwdEventTicker ticker.Ticker

	// LogEventTicker is a signal instructing the htlcswitch to log
	// aggregate stats about it's forwarding during the last interval.
	LogEventTicker ticker.Ticker
}

// Switch is the central messaging bus for all incoming/outgoing HTLCs.
// Connected peers with active channels are treated as named interfaces which
// refer to active channels as links. A link is the switch's message
// communication point with the goroutine that manages an active channel. New
// links are registered each time a channel is created, and unregistered once
// the channel is closed. The switch manages the hand-off process for multi-hop
// HTLCs, forwarding HTLCs initiated from within the daemon, and finally
// notifies users local-systems concerning their outstanding payment requests.
type Switch struct {
	started  int32 // To be used atomically.
	shutdown int32 // To be used atomically.

	// bestHeight is the best known height of the main chain. The links will
	// be used this information to govern decisions based on HTLC timeouts.
	// This will be retrieved by the registered links atomically.
	bestHeight uint32

	wg   sync.WaitGroup
	quit chan struct{}

	// cfg is a copy of the configuration struct that the htlc switch
	// service was initialized with.
	cfg *Config

	// pendingPayments stores payments initiated by the user that are not yet
	// settled. The map is used to later look up the payments and notify the
	// user of the result when they are complete. Each payment is given a unique
	// integer ID when it is created.
	pendingPayments map[uint64]*pendingPayment
	pendingMutex    sync.RWMutex

	paymentSequencer Sequencer

	// control provides verification of sending htlc mesages
	control ControlTower

	// circuits is storage for payment circuits which are used to
	// forward the settle/fail htlc updates back to the add htlc initiator.
	circuits CircuitMap

	// mailOrchestrator manages the lifecycle of mailboxes used throughout
	// the switch, and facilitates delayed delivery of packets to links that
	// later come online.
	mailOrchestrator *mailOrchestrator

	// indexMtx is a read/write mutex that protects the set of indexes
	// below.
	indexMtx sync.RWMutex

	// pendingLinkIndex holds links that have not had their final, live
	// short_chan_id assigned. These links can be transitioned into the
	// primary linkIndex by using UpdateShortChanID to load their live id.
	pendingLinkIndex map[lnwire.ChannelID]ChannelLink

	// links is a map of channel id and channel link which manages
	// this channel.
	linkIndex map[lnwire.ChannelID]ChannelLink

	// forwardingIndex is an index which is consulted by the switch when it
	// needs to locate the next hop to forward an incoming/outgoing HTLC
	// update to/from.
	//
	// TODO(roasbeef): eventually add a NetworkHop mapping before the
	// ChannelLink
	forwardingIndex map[lnwire.ShortChannelID]ChannelLink

	// interfaceIndex maps the compressed public key of a peer to all the
	// channels that the switch maintains with that peer.
	interfaceIndex map[[33]byte]map[lnwire.ChannelID]ChannelLink

	// htlcPlex is the channel which all connected links use to coordinate
	// the setup/teardown of Sphinx (onion routing) payment circuits.
	// Active links forward any add/settle messages over this channel each
	// state transition, sending new adds/settles which are fully locked
	// in.
	htlcPlex chan *plexPacket

	// chanCloseRequests is used to transfer the channel close request to
	// the channel close handler.
	chanCloseRequests chan *ChanClose

	// resolutionMsgs is the channel that all external contract resolution
	// messages will be sent over.
	resolutionMsgs chan *resolutionMsg

	// linkControl is a channel used to propagate add/remove/get htlc
	// switch handler commands.
	linkControl chan interface{}

	// pendingFwdingEvents is the set of forwarding events which have been
	// collected during the current interval, but hasn't yet been written
	// to the forwarding log.
	fwdEventMtx         sync.Mutex
	pendingFwdingEvents []channeldb.ForwardingEvent

	// blockEpochStream is an active block epoch event stream backed by an
	// active ChainNotifier instance. This will be used to retrieve the
	// lastest height of the chain.
	blockEpochStream *chainntnfs.BlockEpochEvent
}

// New creates the new instance of htlc switch.
func New(cfg Config, currentHeight uint32) (*Switch, error) {
	circuitMap, err := NewCircuitMap(&CircuitMapConfig{
		DB:                    cfg.DB,
		ExtractErrorEncrypter: cfg.ExtractErrorEncrypter,
	})
	if err != nil {
		return nil, err
	}

	sequencer, err := NewPersistentSequencer(cfg.DB)
	if err != nil {
		return nil, err
	}

	return &Switch{
		bestHeight:        currentHeight,
		cfg:               &cfg,
		circuits:          circuitMap,
		paymentSequencer:  sequencer,
		control:           NewPaymentControl(false, cfg.DB),
		linkIndex:         make(map[lnwire.ChannelID]ChannelLink),
		mailOrchestrator:  newMailOrchestrator(),
		forwardingIndex:   make(map[lnwire.ShortChannelID]ChannelLink),
		interfaceIndex:    make(map[[33]byte]map[lnwire.ChannelID]ChannelLink),
		pendingLinkIndex:  make(map[lnwire.ChannelID]ChannelLink),
		pendingPayments:   make(map[uint64]*pendingPayment),
		htlcPlex:          make(chan *plexPacket),
		chanCloseRequests: make(chan *ChanClose),
		resolutionMsgs:    make(chan *resolutionMsg),
		quit:              make(chan struct{}),
	}, nil
}

// resolutionMsg is a struct that wraps an existing ResolutionMsg with a done
// channel. We'll use this channel to synchronize delivery of the message with
// the caller.
type resolutionMsg struct {
	contractcourt.ResolutionMsg

	doneChan chan struct{}
}

// ProcessContractResolution is called by active contract resolvers once a
// contract they are watching over has been fully resolved. The message carries
// an external signal that *would* have been sent if the outgoing channel
// didn't need to go to the chain in order to fulfill a contract. We'll process
// this message just as if it came from an active outgoing channel.
func (s *Switch) ProcessContractResolution(msg contractcourt.ResolutionMsg) error {

	done := make(chan struct{})

	select {
	case s.resolutionMsgs <- &resolutionMsg{
		ResolutionMsg: msg,
		doneChan:      done,
	}:
	case <-s.quit:
		return ErrSwitchExiting
	}

	select {
	case <-done:
	case <-s.quit:
		return ErrSwitchExiting
	}

	return nil
}

// SendHTLC is used by other subsystems which aren't belong to htlc switch
// package in order to send the htlc update.
func (s *Switch) SendHTLC(firstHop lnwire.ShortChannelID,
	htlc *lnwire.UpdateAddHTLC,
	deobfuscator ErrorDecrypter) ([sha256.Size]byte, error) {

	// Before sending, double check that we don't already have 1) an
	// in-flight payment to this payment hash, or 2) a complete payment for
	// the same hash.
	if err := s.control.ClearForTakeoff(htlc); err != nil {
		return zeroPreimage, err
	}

	// Create payment and add to the map of payment in order later to be
	// able to retrieve it and return response to the user.
	payment := &pendingPayment{
		err:          make(chan error, 1),
		preimage:     make(chan [sha256.Size]byte, 1),
		paymentHash:  htlc.PaymentHash,
		amount:       htlc.Amount,
		deobfuscator: deobfuscator,
	}

	paymentID, err := s.paymentSequencer.NextID()
	if err != nil {
		return zeroPreimage, err
	}

	s.pendingMutex.Lock()
	s.pendingPayments[paymentID] = payment
	s.pendingMutex.Unlock()

	// Generate and send new update packet, if error will be received on
	// this stage it means that packet haven't left boundaries of our
	// system and something wrong happened.
	packet := &htlcPacket{
		incomingChanID: sourceHop,
		incomingHTLCID: paymentID,
		outgoingChanID: firstHop,
		htlc:           htlc,
	}

	if err := s.forward(packet); err != nil {
		s.removePendingPayment(paymentID)
		if err := s.control.Fail(htlc.PaymentHash); err != nil {
			return zeroPreimage, err
		}

		return zeroPreimage, err
	}

	// Returns channels so that other subsystem might wait/skip the
	// waiting of handling of payment.
	var preimage [sha256.Size]byte

	select {
	case e := <-payment.err:
		err = e
	case <-s.quit:
		return zeroPreimage, ErrSwitchExiting
	}

	select {
	case p := <-payment.preimage:
		preimage = p
	case <-s.quit:
		return zeroPreimage, ErrSwitchExiting
	}

	return preimage, err
}

// UpdateForwardingPolicies sends a message to the switch to update the
// forwarding policies for the set of target channels. If the set of targeted
// channels is nil, then the forwarding policies for all active channels with
// be updated.
//
// NOTE: This function is synchronous and will block until either the
// forwarding policies for all links have been updated, or the switch shuts
// down.
func (s *Switch) UpdateForwardingPolicies(newPolicy ForwardingPolicy,
	targetChans ...wire.OutPoint) error {

	log.Debugf("Updating link policies: %v", newLogClosure(func() string {
		return spew.Sdump(newPolicy)
	}))

	var linksToUpdate []ChannelLink

	s.indexMtx.RLock()

	// If no channels have been targeted, then we'll collect all inks to
	// update their policies.
	if len(targetChans) == 0 {
		for _, link := range s.linkIndex {
			linksToUpdate = append(linksToUpdate, link)
		}
	} else {
		// Otherwise, we'll only attempt to update the forwarding
		// policies for the set of targeted links.
		for _, targetLink := range targetChans {
			cid := lnwire.NewChanIDFromOutPoint(&targetLink)

			// If we can't locate a link by its converted channel
			// ID, then we'll return an error back to the caller.
			link, ok := s.linkIndex[cid]
			if !ok {
				s.indexMtx.RUnlock()

				return fmt.Errorf("unable to find "+
					"ChannelPoint(%v) to update link "+
					"policy", targetLink)
			}

			linksToUpdate = append(linksToUpdate, link)
		}
	}

	s.indexMtx.RUnlock()

	// With all the links we need to update collected, we can release the
	// mutex then update each link directly.
	for _, link := range linksToUpdate {
		link.UpdateForwardingPolicy(newPolicy)
	}

	return nil
}

// forward is used in order to find next channel link and apply htlc update.
// Also this function is used by channel links itself in order to forward the
// update after it has been included in the channel.
func (s *Switch) forward(packet *htlcPacket) error {
	switch htlc := packet.htlc.(type) {
	case *lnwire.UpdateAddHTLC:
		circuit := newPaymentCircuit(&htlc.PaymentHash, packet)
		actions, err := s.circuits.CommitCircuits(circuit)
		if err != nil {
			log.Errorf("unable to commit circuit in switch: %v", err)
			return err
		}

		// Drop duplicate packet if it has already been seen.
		switch {
		case len(actions.Drops) == 1:
			return ErrDuplicateAdd

		case len(actions.Fails) == 1:
			if packet.incomingChanID == sourceHop {
				return err
			}

			var failure lnwire.FailureMessage
			update, err := s.cfg.FetchLastChannelUpdate(
				packet.incomingChanID,
			)
			if err != nil {
				failure = &lnwire.FailTemporaryNodeFailure{}
			} else {
				failure = lnwire.NewTemporaryChannelFailure(update)
			}
			addErr := ErrIncompleteForward

			return s.failAddPacket(packet, failure, addErr)
		}

		packet.circuit = circuit
	}

	return s.route(packet)
}

// ForwardPackets adds a list of packets to the switch for processing. Fails
// and settles are added on a first past, simultaneously constructing circuits
// for any adds. After persisting the circuits, another pass of the adds is
// given to forward them through the router. The sending link's quit channel is
// used to prevent deadlocks when the switch stops a link in the midst of
// forwarding.
//
// NOTE: This method guarantees that the returned err chan will eventually be
// closed. The receiver should read on the channel until receiving such a
// signal.
func (s *Switch) ForwardPackets(linkQuit chan struct{},
	packets ...*htlcPacket) chan error {

	var (
		// fwdChan is a buffered channel used to receive err msgs from
		// the htlcPlex when forwarding this batch.
		fwdChan = make(chan error, len(packets))

		// errChan is a buffered channel returned to the caller, that is
		// proxied by the fwdChan. This method guarantees that errChan
		// will be closed eventually to alert the receiver that it can
		// stop reading from the channel.
		errChan = make(chan error, len(packets))

		// numSent keeps a running count of how many packets are
		// forwarded to the switch, which determines how many responses
		// we will wait for on the fwdChan..
		numSent int
	)

	// No packets, nothing to do.
	if len(packets) == 0 {
		close(errChan)
		return errChan
	}

	// Setup a barrier to prevent the background tasks from processing
	// responses until this function returns to the user.
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()

	// Before spawning the following goroutine to proxy our error responses,
	// check to see if we have already been issued a shutdown request. If
	// so, we exit early to avoid incrementing the switch's waitgroup while
	// it is already in the process of shutting down.
	select {
	case <-linkQuit:
		close(errChan)
		return errChan
	case <-s.quit:
		close(errChan)
		return errChan
	default:
		// Spawn a goroutine the proxy the errs back to the returned err
		// chan.  This is done to ensure the err chan returned to the
		// caller closed properly, alerting the receiver of completion
		// or shutdown.
		s.wg.Add(1)
		go s.proxyFwdErrs(&numSent, &wg, fwdChan, errChan)
	}

	// Make a first pass over the packets, forwarding any settles or fails.
	// As adds are found, we create a circuit and append it to our set of
	// circuits to be written to disk.
	var circuits []*PaymentCircuit
	var addBatch []*htlcPacket
	for _, packet := range packets {
		switch htlc := packet.htlc.(type) {
		case *lnwire.UpdateAddHTLC:
			circuit := newPaymentCircuit(&htlc.PaymentHash, packet)
			packet.circuit = circuit
			circuits = append(circuits, circuit)
			addBatch = append(addBatch, packet)
		default:
			err := s.routeAsync(packet, fwdChan, linkQuit)
			if err != nil {
				return errChan
			}
			numSent++
		}
	}

	// If this batch did not contain any circuits to commit, we can return
	// early.
	if len(circuits) == 0 {
		return errChan
	}

	// Write any circuits that we found to disk.
	actions, err := s.circuits.CommitCircuits(circuits...)
	if err != nil {
		log.Errorf("unable to commit circuits in switch: %v", err)
	}

	// Split the htlc packets by comparing an in-order seek to the head of
	// the added, dropped, or failed circuits.
	//
	// NOTE: This assumes each list is guaranteed to be a subsequence of the
	// circuits, and that the union of the sets results in the original set
	// of circuits.
	var addedPackets, failedPackets []*htlcPacket
	for _, packet := range addBatch {
		switch {
		case len(actions.Adds) > 0 && packet.circuit == actions.Adds[0]:
			addedPackets = append(addedPackets, packet)
			actions.Adds = actions.Adds[1:]

		case len(actions.Drops) > 0 && packet.circuit == actions.Drops[0]:
			actions.Drops = actions.Drops[1:]

		case len(actions.Fails) > 0 && packet.circuit == actions.Fails[0]:
			failedPackets = append(failedPackets, packet)
			actions.Fails = actions.Fails[1:]
		}
	}

	// Now, forward any packets for circuits that were successfully added to
	// the switch's circuit map.
	for _, packet := range addedPackets {
		err := s.routeAsync(packet, fwdChan, linkQuit)
		if err != nil {
			return errChan
		}
		numSent++
	}

	// Lastly, for any packets that failed, this implies that they were
	// left in a half added state, which can happen when recovering from
	// failures.
	if len(failedPackets) > 0 {
		var failure lnwire.FailureMessage
		update, err := s.cfg.FetchLastChannelUpdate(
			failedPackets[0].incomingChanID,
		)
		if err != nil {
			failure = &lnwire.FailTemporaryNodeFailure{}
		} else {
			failure = lnwire.NewTemporaryChannelFailure(update)
		}

		for _, packet := range failedPackets {
			addErr := errors.New("failing packet after " +
				"detecting incomplete forward")

			// We don't handle the error here since this method
			// always returns an error.
			s.failAddPacket(packet, failure, addErr)
		}
	}

	return errChan
}

// proxyFwdErrs transmits any errors received on `fwdChan` back to `errChan`,
// and guarantees that the `errChan` will be closed after 1) all errors have
// been sent, or 2) the switch has received a shutdown. The `errChan` should be
// buffered with at least the value of `num` after the barrier has been
// released.
//
// NOTE: The receiver of `errChan` should read until the channel closed, since
// this proxying guarantees that the close will happen.
func (s *Switch) proxyFwdErrs(num *int, wg *sync.WaitGroup,
	fwdChan, errChan chan error) {
	defer s.wg.Done()
	defer close(errChan)

	// Wait here until the outer function has finished persisting
	// and routing the packets. This guarantees we don't read from num until
	// the value is accurate.
	wg.Wait()

	numSent := *num
	for i := 0; i < numSent; i++ {
		select {
		case err := <-fwdChan:
			errChan <- err
		case <-s.quit:
			log.Errorf("unable to forward htlc packet " +
				"htlc switch was stopped")
			return
		}
	}
}

// route sends a single htlcPacket through the switch and synchronously awaits a
// response.
func (s *Switch) route(packet *htlcPacket) error {
	command := &plexPacket{
		pkt: packet,
		err: make(chan error, 1),
	}

	select {
	case s.htlcPlex <- command:
	case <-s.quit:
		return ErrSwitchExiting
	}

	select {
	case err := <-command.err:
		return err
	case <-s.quit:
		return ErrSwitchExiting
	}
}

// routeAsync sends a packet through the htlc switch, using the provided err
// chan to propagate errors back to the caller. The link's quit channel is
// provided so that the send can be canceled if either the link or the switch
// receive a shutdown requuest. This method does not wait for a response from
// the htlcForwarder before returning.
func (s *Switch) routeAsync(packet *htlcPacket, errChan chan error,
	linkQuit chan struct{}) error {

	command := &plexPacket{
		pkt: packet,
		err: errChan,
	}

	select {
	case s.htlcPlex <- command:
		return nil
	case <-linkQuit:
		return ErrLinkShuttingDown
	case <-s.quit:
		return errors.New("Htlc Switch was stopped")
	}
}

// handleLocalDispatch is used at the start/end of the htlc update life cycle.
// At the start (1) it is used to send the htlc to the channel link without
// creation of circuit. At the end (2) it is used to notify the user about the
// result of his payment is it was successful or not.
//
//   Alice         Bob          Carol
//     o --add----> o ---add----> o
//    (1)
//
//    (2)
//     o <-settle-- o <--settle-- o
//   Alice         Bob         Carol
//
func (s *Switch) handleLocalDispatch(pkt *htlcPacket) error {
	// User have created the htlc update therefore we should find the
	// appropriate channel link and send the payment over this link.
	if htlc, ok := pkt.htlc.(*lnwire.UpdateAddHTLC); ok {
		// Try to find links by node destination.
		s.indexMtx.RLock()
		link, err := s.getLinkByShortID(pkt.outgoingChanID)
		s.indexMtx.RUnlock()
		if err != nil {
			log.Errorf("Link %v not found", pkt.outgoingChanID)
			return &ForwardingError{
				ErrorSource:    s.cfg.SelfKey,
				FailureMessage: &lnwire.FailUnknownNextPeer{},
			}
		}

		if !link.EligibleToForward() {
			err := fmt.Errorf("Link %v is not available to forward",
				pkt.outgoingChanID)
			log.Error(err)

			// The update does not need to be populated as the error
			// will be returned back to the router.
			htlcErr := lnwire.NewTemporaryChannelFailure(nil)
			return &ForwardingError{
				ErrorSource:    s.cfg.SelfKey,
				ExtraMsg:       err.Error(),
				FailureMessage: htlcErr,
			}
		}

		if link.Bandwidth() < htlc.Amount {
			err := fmt.Errorf("Link %v has insufficient capacity: "+
				"need %v, has %v", pkt.outgoingChanID,
				htlc.Amount, link.Bandwidth())
			log.Error(err)

			// The update does not need to be populated as the error
			// will be returned back to the router.
			htlcErr := lnwire.NewTemporaryChannelFailure(nil)
			return &ForwardingError{
				ErrorSource:    s.cfg.SelfKey,
				ExtraMsg:       err.Error(),
				FailureMessage: htlcErr,
			}
		}

		return link.HandleSwitchPacket(pkt)
	}

	s.wg.Add(1)
	go s.handleLocalResponse(pkt)

	return nil
}

// handleLocalResponse processes a Settle or Fail responding to a
// locally-initiated payment. This is handled asynchronously to avoid blocking
// the main event loop within the switch, as these operations can require
// multiple db transactions. The guarantees of the circuit map are stringent
// enough such that we are able to tolerate reordering of these operations
// without side effects. The primary operations handled are:
//  1. Ack settle/fail references, to avoid resending this response internally
//  2. Teardown the closing circuit in the circuit map
//  3. Transition the payment status to grounded or completed.
//  4. Respond to an in-mem pending payment, if it is found.
//
// NOTE: This method MUST be spawned as a goroutine.
func (s *Switch) handleLocalResponse(pkt *htlcPacket) {
	defer s.wg.Done()

	// First, we'll clean up any fwdpkg references, circuit entries, and
	// mark in our db that the payment for this payment hash has either
	// succeeded or failed.
	//
	// If this response is contained in a forwarding package, we'll start by
	// acking the settle/fail so that we don't continue to retransmit the
	// HTLC internally.
	if pkt.destRef != nil {
		if err := s.ackSettleFail(*pkt.destRef); err != nil {
			log.Warnf("Unable to ack settle/fail reference: %s: %v",
				*pkt.destRef, err)
			return
		}
	}

	// Next, we'll remove the circuit since we are about to complete an
	// fulfill/fail of this HTLC. Since we've already removed the
	// settle/fail fwdpkg reference, the response from the peer cannot be
	// replayed internally if this step fails. If this happens, this logic
	// will be executed when a provided resolution message comes through.
	// This can only happen if the circuit is still open, which is why this
	// ordering is chosen.
	if err := s.teardownCircuit(pkt); err != nil {
		log.Warnf("Unable to teardown circuit %s: %v",
			pkt.inKey(), err)
		return
	}

	// Locate the pending payment to notify the application that this
	// payment has failed. If one is not found, it likely means the daemon
	// has been restarted since sending the payment.
	payment := s.findPayment(pkt.incomingHTLCID)

	var (
		preimage   [32]byte
		paymentErr error
	)

	switch htlc := pkt.htlc.(type) {

	// We've received a settle update which means we can finalize the user
	// payment and return successful response.
	case *lnwire.UpdateFulfillHTLC:
		// Persistently mark that a payment to this payment hash
		// succeeded. This will prevent us from ever making another
		// payment to this hash.
		err := s.control.Success(pkt.circuit.PaymentHash)
		if err != nil && err != ErrPaymentAlreadyCompleted {
			log.Warnf("Unable to mark completed payment %x: %v",
				pkt.circuit.PaymentHash, err)
			return
		}

		preimage = htlc.PaymentPreimage

	// We've received a fail update which means we can finalize the user
	// payment and return fail response.
	case *lnwire.UpdateFailHTLC:
		// Persistently mark that a payment to this payment hash failed.
		// This will permit us to make another attempt at a successful
		// payment.
		err := s.control.Fail(pkt.circuit.PaymentHash)
		if err != nil && err != ErrPaymentAlreadyCompleted {
			log.Warnf("Unable to ground payment %x: %v",
				pkt.circuit.PaymentHash, err)
			return
		}

		paymentErr = s.parseFailedPayment(payment, pkt, htlc)

	default:
		log.Warnf("Received unknown response type: %T", pkt.htlc)
		return
	}

	// Deliver the payment error and preimage to the application, if it is
	// waiting for a response.
	if payment != nil {
		payment.err <- paymentErr
		payment.preimage <- preimage
		s.removePendingPayment(pkt.incomingHTLCID)
	}
}

// parseFailedPayment determines the appropriate failure message to return to
// a user initiated payment. The three cases handled are:
// 1) A local failure, which should already plaintext.
// 2) A resolution from the chain arbitrator,
// 3) A failure from the remote party, which will need to be decrypted using the
//      payment deobfuscator.
func (s *Switch) parseFailedPayment(payment *pendingPayment, pkt *htlcPacket,
	htlc *lnwire.UpdateFailHTLC) *ForwardingError {

	var failure *ForwardingError

	switch {

	// The payment never cleared the link, so we don't need to
	// decrypt the error, simply decode it them report back to the
	// user.
	case pkt.localFailure:
		var userErr string
		r := bytes.NewReader(htlc.Reason)
		failureMsg, err := lnwire.DecodeFailure(r, 0)
		if err != nil {
			userErr = fmt.Sprintf("unable to decode onion failure, "+
				"htlc with hash(%x): %v",
				pkt.circuit.PaymentHash[:], err)
			log.Error(userErr)

			// As this didn't even clear the link, we don't need to
			// apply an update here since it goes directly to the
			// router.
			failureMsg = lnwire.NewTemporaryChannelFailure(nil)
		}
		failure = &ForwardingError{
			ErrorSource:    s.cfg.SelfKey,
			ExtraMsg:       userErr,
			FailureMessage: failureMsg,
		}

	// A payment had to be timed out on chain before it got past
	// the first hop. In this case, we'll report a permanent
	// channel failure as this means us, or the remote party had to
	// go on chain.
	case pkt.isResolution && htlc.Reason == nil:
		userErr := fmt.Sprintf("payment was resolved " +
			"on-chain, then cancelled back")
		failure = &ForwardingError{
			ErrorSource:    s.cfg.SelfKey,
			ExtraMsg:       userErr,
			FailureMessage: lnwire.FailPermanentChannelFailure{},
		}

	// If the provided payment is nil, we have discarded the error decryptor
	// due to a restart. We'll return a fixed error and signal a temporary
	// channel failure to the router.
	case payment == nil:
		userErr := fmt.Sprintf("error decryptor for payment " +
			"could not be located, likely due to restart")
		failure = &ForwardingError{
			ErrorSource:    s.cfg.SelfKey,
			ExtraMsg:       userErr,
			FailureMessage: lnwire.NewTemporaryChannelFailure(nil),
		}

	// A regular multi-hop payment error that we'll need to
	// decrypt.
	default:
		var err error
		// We'll attempt to fully decrypt the onion encrypted
		// error. If we're unable to then we'll bail early.
		failure, err = payment.deobfuscator.DecryptError(htlc.Reason)
		if err != nil {
			userErr := fmt.Sprintf("unable to de-obfuscate onion "+
				"failure, htlc with hash(%x): %v",
				pkt.circuit.PaymentHash[:], err)
			log.Error(userErr)
			failure = &ForwardingError{
				ErrorSource:    s.cfg.SelfKey,
				ExtraMsg:       userErr,
				FailureMessage: lnwire.NewTemporaryChannelFailure(nil),
			}
		}
	}

	return failure
}

// handlePacketForward is used in cases when we need forward the htlc update
// from one channel link to another and be able to propagate the settle/fail
// updates back. This behaviour is achieved by creation of payment circuits.
func (s *Switch) handlePacketForward(packet *htlcPacket) error {
	switch htlc := packet.htlc.(type) {

	// Channel link forwarded us a new htlc, therefore we initiate the
	// payment circuit within our internal state so we can properly forward
	// the ultimate settle message back latter.
	case *lnwire.UpdateAddHTLC:
		if packet.incomingChanID == sourceHop {
			// A blank incomingChanID indicates that this is
			// a pending user-initiated payment.
			return s.handleLocalDispatch(packet)
		}

		s.indexMtx.RLock()
		targetLink, err := s.getLinkByShortID(packet.outgoingChanID)
		if err != nil {
			s.indexMtx.RUnlock()

			// If packet was forwarded from another channel link
			// than we should notify this link that some error
			// occurred.
			failure := &lnwire.FailUnknownNextPeer{}
			addErr := fmt.Errorf("unable to find link with "+
				"destination %v", packet.outgoingChanID)

			return s.failAddPacket(packet, failure, addErr)
		}
		interfaceLinks, _ := s.getLinks(targetLink.Peer().PubKey())
		s.indexMtx.RUnlock()

		// We'll keep track of any HTLC failures during the link
		// selection process. This way we can return the error for
		// precise link that the sender selected, while optimistically
		// trying all links to utilize our available bandwidth.
		linkErrs := make(map[lnwire.ShortChannelID]lnwire.FailureMessage)

		// Try to find destination channel link with appropriate
		// bandwidth.
		var destination ChannelLink
		for _, link := range interfaceLinks {
			// We'll skip any links that aren't yet eligible for
			// forwarding.
			switch {
			case !link.EligibleToForward():
				continue

			// If the link doesn't yet have a source chan ID, then
			// we'll skip it as well.
			case link.ShortChanID() == sourceHop:
				continue
			}

			// Before we check the link's bandwidth, we'll ensure
			// that the HTLC satisfies the current forwarding
			// policy of this target link.
			currentHeight := atomic.LoadUint32(&s.bestHeight)
			err := link.HtlcSatifiesPolicy(
				htlc.PaymentHash, packet.incomingAmount,
				packet.amount, packet.incomingTimeout,
				packet.outgoingTimeout, currentHeight,
			)
			if err != nil {
				linkErrs[link.ShortChanID()] = err
				continue
			}

			if link.Bandwidth() >= htlc.Amount {
				destination = link

				break
			}
		}

		switch {
		// If the channel link we're attempting to forward the update
		// over has insufficient capacity, and didn't violate any
		// forwarding policies, then we'll cancel the htlc as the
		// payment cannot succeed.
		case destination == nil && len(linkErrs) == 0:
			// If packet was forwarded from another channel link
			// than we should notify this link that some error
			// occurred.
			var failure lnwire.FailureMessage
			update, err := s.cfg.FetchLastChannelUpdate(
				packet.outgoingChanID,
			)
			if err != nil {
				failure = &lnwire.FailTemporaryNodeFailure{}
			} else {
				failure = lnwire.NewTemporaryChannelFailure(update)
			}

			addErr := fmt.Errorf("unable to find appropriate "+
				"channel link insufficient capacity, need "+
				"%v", htlc.Amount)

			return s.failAddPacket(packet, failure, addErr)

		// If we had a forwarding failure due to the HTLC not
		// satisfying the current policy, then we'll send back an
		// error, but ensure we send back the error sourced at the
		// *target* link.
		case destination == nil && len(linkErrs) != 0:
			// At this point, some or all of the links rejected the
			// HTLC so we couldn't forward it. So we'll try to look
			// up the error that came from the source.
			linkErr, ok := linkErrs[packet.outgoingChanID]
			if !ok {
				// If we can't find the error of the source,
				// then we'll return an unknown next peer,
				// though this should never happen.
				linkErr = &lnwire.FailUnknownNextPeer{}
				log.Warnf("unable to find err source for "+
					"outgoing_link=%v, errors=%v",
					packet.outgoingChanID, newLogClosure(func() string {
						return spew.Sdump(linkErrs)
					}))
			}

			addErr := fmt.Errorf("incoming HTLC(%x) violated "+
				"target outgoing link (id=%v) policy: %v",
				htlc.PaymentHash[:], packet.outgoingChanID,
				linkErr)

			return s.failAddPacket(packet, linkErr, addErr)
		}

		// Send the packet to the destination channel link which
		// manages the channel.
		packet.outgoingChanID = destination.ShortChanID()
		return destination.HandleSwitchPacket(packet)

	case *lnwire.UpdateFailHTLC, *lnwire.UpdateFulfillHTLC:
		// If the source of this packet has not been set, use the
		// circuit map to lookup the origin.
		circuit, err := s.closeCircuit(packet)
		if err != nil {
			return err
		}

		fail, isFail := htlc.(*lnwire.UpdateFailHTLC)
		if isFail && !packet.hasSource {
			switch {
			case circuit.ErrorEncrypter == nil:
				// No message to encrypt, locally sourced
				// payment.

			case packet.isResolution:
				// If this is a resolution message, then we'll need to encrypt
				// it as it's actually internally sourced.
				var err error
				// TODO(roasbeef): don't need to pass actually?
				failure := &lnwire.FailPermanentChannelFailure{}
				fail.Reason, err = circuit.ErrorEncrypter.EncryptFirstHop(
					failure,
				)
				if err != nil {
					err = fmt.Errorf("unable to obfuscate "+
						"error: %v", err)
					log.Error(err)
				}

			default:
				// Otherwise, it's a forwarded error, so we'll perform a
				// wrapper encryption as normal.
				fail.Reason = circuit.ErrorEncrypter.IntermediateEncrypt(
					fail.Reason,
				)
			}
		} else if !isFail && circuit.Outgoing != nil {
			// If this is an HTLC settle, and it wasn't from a
			// locally initiated HTLC, then we'll log a forwarding
			// event so we can flush it to disk later.
			//
			// TODO(roasbeef): only do this once link actually
			// fully settles?
			localHTLC := packet.incomingChanID == sourceHop
			if !localHTLC {
				s.fwdEventMtx.Lock()
				s.pendingFwdingEvents = append(
					s.pendingFwdingEvents,
					channeldb.ForwardingEvent{
						Timestamp:      time.Now(),
						IncomingChanID: circuit.Incoming.ChanID,
						OutgoingChanID: circuit.Outgoing.ChanID,
						AmtIn:          circuit.IncomingAmount,
						AmtOut:         circuit.OutgoingAmount,
					},
				)
				s.fwdEventMtx.Unlock()
			}
		}

		// A blank IncomingChanID in a circuit indicates that it is a pending
		// user-initiated payment.
		if packet.incomingChanID == sourceHop {
			return s.handleLocalDispatch(packet)
		}

		// Check to see that the source link is online before removing
		// the circuit.
		return s.mailOrchestrator.Deliver(packet.incomingChanID, packet)

	default:
		return errors.New("wrong update type")
	}
}

// failAddPacket encrypts a fail packet back to an add packet's source.
// The ciphertext will be derived from the failure message proivded by context.
// This method returns the failErr if all other steps complete successfully.
func (s *Switch) failAddPacket(packet *htlcPacket,
	failure lnwire.FailureMessage, failErr error) error {

	// Encrypt the failure so that the sender will be able to read the error
	// message. Since we failed this packet, we use EncryptFirstHop to
	// obfuscate the failure for their eyes only.
	reason, err := packet.obfuscator.EncryptFirstHop(failure)
	if err != nil {
		err := fmt.Errorf("unable to obfuscate "+
			"error: %v", err)
		log.Error(err)
		return err
	}

	log.Error(failErr)

	failPkt := &htlcPacket{
		sourceRef:      packet.sourceRef,
		incomingChanID: packet.incomingChanID,
		incomingHTLCID: packet.incomingHTLCID,
		circuit:        packet.circuit,
		htlc: &lnwire.UpdateFailHTLC{
			Reason: reason,
		},
	}

	// Route a fail packet back to the source link.
	err = s.mailOrchestrator.Deliver(failPkt.incomingChanID, failPkt)
	if err != nil {
		err = fmt.Errorf("source chanid=%v unable to "+
			"handle switch packet: %v",
			packet.incomingChanID, err)
		log.Error(err)
		return err
	}

	return failErr
}

// closeCircuit accepts a settle or fail htlc and the associated htlc packet and
// attempts to determine the source that forwarded this htlc. This method will
// set the incoming chan and htlc ID of the given packet if the source was
// found, and will properly [re]encrypt any failure messages.
func (s *Switch) closeCircuit(pkt *htlcPacket) (*PaymentCircuit, error) {
	// If the packet has its source, that means it was failed locally by
	// the outgoing link. We fail it here to make sure only one response
	// makes it through the switch.
	if pkt.hasSource {
		circuit, err := s.circuits.FailCircuit(pkt.inKey())
		switch err {

		// Circuit successfully closed.
		case nil:
			return circuit, nil

		// Circuit was previously closed, but has not been deleted.
		// We'll just drop this response until the circuit has been
		// fully removed.
		case ErrCircuitClosing:
			return nil, err

		// Failed to close circuit because it does not exist. This is
		// likely because the circuit was already successfully closed.
		// Since this packet failed locally, there is no forwarding
		// package entry to acknowledge.
		case ErrUnknownCircuit:
			return nil, err

		// Unexpected error.
		default:
			return nil, err
		}
	}

	// Otherwise, this is packet was received from the remote party.  Use
	// circuit map to find the incoming link to receive the settle/fail.
	circuit, err := s.circuits.CloseCircuit(pkt.outKey())
	switch err {

	// Open circuit successfully closed.
	case nil:
		pkt.incomingChanID = circuit.Incoming.ChanID
		pkt.incomingHTLCID = circuit.Incoming.HtlcID
		pkt.circuit = circuit
		pkt.sourceRef = &circuit.AddRef

		pktType := "SETTLE"
		if _, ok := pkt.htlc.(*lnwire.UpdateFailHTLC); ok {
			pktType = "FAIL"
		}

		log.Debugf("Closed completed %s circuit for %x: "+
			"(%s, %d) <-> (%s, %d)", pktType, pkt.circuit.PaymentHash,
			pkt.incomingChanID, pkt.incomingHTLCID,
			pkt.outgoingChanID, pkt.outgoingHTLCID)

		return circuit, nil

	// Circuit was previously closed, but has not been deleted. We'll just
	// drop this response until the circuit has been removed.
	case ErrCircuitClosing:
		return nil, err

	// Failed to close circuit because it does not exist. This is likely
	// because the circuit was already successfully closed.
	case ErrUnknownCircuit:
		err := fmt.Errorf("Unable to find target channel "+
			"for HTLC settle/fail: channel ID = %s, "+
			"HTLC ID = %d", pkt.outgoingChanID,
			pkt.outgoingHTLCID)
		log.Error(err)

		// TODO(conner): ack settle/fail
		if pkt.destRef != nil {
			if err := s.ackSettleFail(*pkt.destRef); err != nil {
				return nil, err
			}
		}

		return nil, err

	// Unexpected error.
	default:
		return nil, err
	}
}

// ackSettleFail is used by the switch to ACK any settle/fail entries in the
// forwarding package of the outgoing link for a payment circuit. We do this if
// we're the originator of the payment, so the link stops attempting to
// re-broadcast.
func (s *Switch) ackSettleFail(settleFailRef channeldb.SettleFailRef) error {
	return s.cfg.DB.Batch(func(tx *bbolt.Tx) error {
		return s.cfg.SwitchPackager.AckSettleFails(tx, settleFailRef)
	})
}

// teardownCircuit removes a pending or open circuit from the switch's circuit
// map and prints useful logging statements regarding the outcome.
func (s *Switch) teardownCircuit(pkt *htlcPacket) error {
	var pktType string
	switch htlc := pkt.htlc.(type) {
	case *lnwire.UpdateFulfillHTLC:
		pktType = "SETTLE"
	case *lnwire.UpdateFailHTLC:
		pktType = "FAIL"
	default:
		err := fmt.Errorf("cannot tear down packet of type: %T", htlc)
		log.Errorf(err.Error())
		return err
	}

	switch {
	case pkt.circuit.HasKeystone():
		log.Debugf("Tearing down open circuit with %s pkt, removing circuit=%v "+
			"with keystone=%v", pktType, pkt.inKey(), pkt.outKey())

		err := s.circuits.DeleteCircuits(pkt.inKey())
		if err != nil {
			log.Warnf("Failed to tear down open circuit (%s, %d) <-> (%s, %d) "+
				"with payment_hash-%v using %s pkt",
				pkt.incomingChanID, pkt.incomingHTLCID,
				pkt.outgoingChanID, pkt.outgoingHTLCID,
				pkt.circuit.PaymentHash, pktType)
			return err
		}

		log.Debugf("Closed completed %s circuit for %x: "+
			"(%s, %d) <-> (%s, %d)", pktType, pkt.circuit.PaymentHash,
			pkt.incomingChanID, pkt.incomingHTLCID,
			pkt.outgoingChanID, pkt.outgoingHTLCID)

	default:
		log.Debugf("Tearing down incomplete circuit with %s for inkey=%v",
			pktType, pkt.inKey())

		err := s.circuits.DeleteCircuits(pkt.inKey())
		if err != nil {
			log.Warnf("Failed to tear down pending %s circuit for %x: "+
				"(%s, %d)", pktType, pkt.circuit.PaymentHash,
				pkt.incomingChanID, pkt.incomingHTLCID)
			return err
		}

		log.Debugf("Removed pending onion circuit for %x: "+
			"(%s, %d)", pkt.circuit.PaymentHash,
			pkt.incomingChanID, pkt.incomingHTLCID)
	}

	return nil
}

// CloseLink creates and sends the close channel command to the target link
// directing the specified closure type. If the closure type if CloseRegular,
// then the last parameter should be the ideal fee-per-kw that will be used as
// a starting point for close negotiation.
func (s *Switch) CloseLink(chanPoint *wire.OutPoint, closeType ChannelCloseType,
	targetFeePerKw lnwallet.SatPerKWeight) (chan *lnrpc.CloseStatusUpdate,
	chan error) {

	// TODO(roasbeef) abstract out the close updates.
	updateChan := make(chan *lnrpc.CloseStatusUpdate, 2)
	errChan := make(chan error, 1)

	command := &ChanClose{
		CloseType:      closeType,
		ChanPoint:      chanPoint,
		Updates:        updateChan,
		TargetFeePerKw: targetFeePerKw,
		Err:            errChan,
	}

	select {
	case s.chanCloseRequests <- command:
		return updateChan, errChan

	case <-s.quit:
		errChan <- ErrSwitchExiting
		close(updateChan)
		return updateChan, errChan
	}
}

// htlcForwarder is responsible for optimally forwarding (and possibly
// fragmenting) incoming/outgoing HTLCs amongst all active interfaces and their
// links. The duties of the forwarder are similar to that of a network switch,
// in that it facilitates multi-hop payments by acting as a central messaging
// bus. The switch communicates will active links to create, manage, and tear
// down active onion routed payments. Each active channel is modeled as
// networked device with metadata such as the available payment bandwidth, and
// total link capacity.
//
// NOTE: This MUST be run as a goroutine.
func (s *Switch) htlcForwarder() {
	defer s.wg.Done()

	defer func() {
		s.blockEpochStream.Cancel()

		// Remove all links once we've been signalled for shutdown.
		var linksToStop []ChannelLink
		s.indexMtx.Lock()
		for _, link := range s.linkIndex {
			activeLink := s.removeLink(link.ChanID())
			if activeLink == nil {
				log.Errorf("unable to remove ChannelLink(%v) "+
					"on stop", link.ChanID())
				continue
			}
			linksToStop = append(linksToStop, activeLink)
		}
		for _, link := range s.pendingLinkIndex {
			pendingLink := s.removeLink(link.ChanID())
			if pendingLink == nil {
				log.Errorf("unable to remove ChannelLink(%v) "+
					"on stop", link.ChanID())
				continue
			}
			linksToStop = append(linksToStop, pendingLink)
		}
		s.indexMtx.Unlock()

		// Now that all pending and live links have been removed from
		// the forwarding indexes, stop each one before shutting down.
		// We'll shut them down in parallel to make exiting as fast as
		// possible.
		var wg sync.WaitGroup
		for _, link := range linksToStop {
			wg.Add(1)
			go func(l ChannelLink) {
				defer wg.Done()
				l.Stop()
			}(link)
		}
		wg.Wait()

		// Before we exit fully, we'll attempt to flush out any
		// forwarding events that may still be lingering since the last
		// batch flush.
		if err := s.FlushForwardingEvents(); err != nil {
			log.Errorf("unable to flush forwarding events: %v", err)
		}
	}()

	// TODO(roasbeef): cleared vs settled distinction
	var (
		totalNumUpdates uint64
		totalSatSent    btcutil.Amount
		totalSatRecv    btcutil.Amount
	)
	s.cfg.LogEventTicker.Resume()
	defer s.cfg.LogEventTicker.Stop()

	// Every 15 seconds, we'll flush out the forwarding events that
	// occurred during that period.
	s.cfg.FwdEventTicker.Resume()
	defer s.cfg.FwdEventTicker.Stop()

out:
	for {
		select {
		case blockEpoch, ok := <-s.blockEpochStream.Epochs:
			if !ok {
				break out
			}

			atomic.StoreUint32(&s.bestHeight, uint32(blockEpoch.Height))

		// A local close request has arrived, we'll forward this to the
		// relevant link (if it exists) so the channel can be
		// cooperatively closed (if possible).
		case req := <-s.chanCloseRequests:
			chanID := lnwire.NewChanIDFromOutPoint(req.ChanPoint)

			s.indexMtx.RLock()
			link, ok := s.linkIndex[chanID]
			if !ok {
				s.indexMtx.RUnlock()

				req.Err <- fmt.Errorf("no peer for channel with "+
					"chan_id=%x", chanID[:])
				continue
			}
			s.indexMtx.RUnlock()

			peerPub := link.Peer().PubKey()
			log.Debugf("Requesting local channel close: peer=%v, "+
				"chan_id=%x", link.Peer(), chanID[:])

			go s.cfg.LocalChannelClose(peerPub[:], req)

		case resolutionMsg := <-s.resolutionMsgs:
			pkt := &htlcPacket{
				outgoingChanID: resolutionMsg.SourceChan,
				outgoingHTLCID: resolutionMsg.HtlcIndex,
				isResolution:   true,
			}

			// Resolution messages will either be cancelling
			// backwards an existing HTLC, or settling a previously
			// outgoing HTLC. Based on this, we'll map the message
			// to the proper htlcPacket.
			if resolutionMsg.Failure != nil {
				pkt.htlc = &lnwire.UpdateFailHTLC{}
			} else {
				pkt.htlc = &lnwire.UpdateFulfillHTLC{
					PaymentPreimage: *resolutionMsg.PreImage,
				}
			}

			log.Infof("Received outside contract resolution, "+
				"mapping to: %v", spew.Sdump(pkt))

			// We don't check the error, as the only failure we can
			// encounter is due to the circuit already being
			// closed. This is fine, as processing this message is
			// meant to be idempotent.
			err := s.handlePacketForward(pkt)
			if err != nil {
				log.Errorf("Unable to forward resolution msg: %v", err)
			}

			// With the message processed, we'll now close out
			close(resolutionMsg.doneChan)

		// A new packet has arrived for forwarding, we'll interpret the
		// packet concretely, then either forward it along, or
		// interpret a return packet to a locally initialized one.
		case cmd := <-s.htlcPlex:
			cmd.err <- s.handlePacketForward(cmd.pkt)

		// When this time ticks, then it indicates that we should
		// collect all the forwarding events since the last internal,
		// and write them out to our log.
		case <-s.cfg.FwdEventTicker.Ticks():
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()

				if err := s.FlushForwardingEvents(); err != nil {
					log.Errorf("unable to flush "+
						"forwarding events: %v", err)
				}
			}()

		// The log ticker has fired, so we'll calculate some forwarding
		// stats for the last 10 seconds to display within the logs to
		// users.
		case <-s.cfg.LogEventTicker.Ticks():
			// First, we'll collate the current running tally of
			// our forwarding stats.
			prevSatSent := totalSatSent
			prevSatRecv := totalSatRecv
			prevNumUpdates := totalNumUpdates

			var (
				newNumUpdates uint64
				newSatSent    btcutil.Amount
				newSatRecv    btcutil.Amount
			)

			// Next, we'll run through all the registered links and
			// compute their up-to-date forwarding stats.
			s.indexMtx.RLock()
			for _, link := range s.linkIndex {
				// TODO(roasbeef): when links first registered
				// stats printed.
				updates, sent, recv := link.Stats()
				newNumUpdates += updates
				newSatSent += sent.ToSatoshis()
				newSatRecv += recv.ToSatoshis()
			}
			s.indexMtx.RUnlock()

			var (
				diffNumUpdates uint64
				diffSatSent    btcutil.Amount
				diffSatRecv    btcutil.Amount
			)

			// If this is the first time we're computing these
			// stats, then the diff is just the new value. We do
			// this in order to avoid integer underflow issues.
			if prevNumUpdates == 0 {
				diffNumUpdates = newNumUpdates
				diffSatSent = newSatSent
				diffSatRecv = newSatRecv
			} else {
				diffNumUpdates = newNumUpdates - prevNumUpdates
				diffSatSent = newSatSent - prevSatSent
				diffSatRecv = newSatRecv - prevSatRecv
			}

			// If the diff of num updates is zero, then we haven't
			// forwarded anything in the last 10 seconds, so we can
			// skip this update.
			if diffNumUpdates == 0 {
				continue
			}

			// If the diff of num updates is negative, then some
			// links may have been unregistered from the switch, so
			// we'll update our stats to only include our registered
			// links.
			if int64(diffNumUpdates) < 0 {
				totalNumUpdates = newNumUpdates
				totalSatSent = newSatSent
				totalSatRecv = newSatRecv
				continue
			}

			// Otherwise, we'll log this diff, then accumulate the
			// new stats into the running total.
			log.Debugf("Sent %d satoshis and received %d satoshis "+
				"in the last 10 seconds (%f tx/sec)",
				diffSatSent, diffSatRecv,
				float64(diffNumUpdates)/10)

			totalNumUpdates += diffNumUpdates
			totalSatSent += diffSatSent
			totalSatRecv += diffSatRecv

		case <-s.quit:
			return
		}
	}
}

// Start starts all helper goroutines required for the operation of the switch.
func (s *Switch) Start() error {
	if !atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		log.Warn("Htlc Switch already started")
		return errors.New("htlc switch already started")
	}

	log.Infof("Starting HTLC Switch")

	blockEpochStream, err := s.cfg.Notifier.RegisterBlockEpochNtfn(nil)
	if err != nil {
		return err
	}
	s.blockEpochStream = blockEpochStream

	s.wg.Add(1)
	go s.htlcForwarder()

	if err := s.reforwardResponses(); err != nil {
		s.Stop()
		log.Errorf("unable to reforward responses: %v", err)
		return err
	}

	return nil
}

// reforwardResponses for every known, non-pending channel, loads all associated
// forwarding packages and reforwards any Settle or Fail HTLCs found. This is
// used to resurrect the switch's mailboxes after a restart.
func (s *Switch) reforwardResponses() error {
	openChannels, err := s.cfg.DB.FetchAllOpenChannels()
	if err != nil {
		return err
	}

	for _, openChannel := range openChannels {
		shortChanID := openChannel.ShortChanID()

		// Locally-initiated payments never need reforwarding.
		if shortChanID == sourceHop {
			continue
		}

		// If the channel is pending, it should have no forwarding
		// packages, and nothing to reforward.
		if openChannel.IsPending {
			continue
		}

		// Channels in open or waiting-close may still have responses in
		// their forwarding packages. We will continue to reattempt
		// forwarding on startup until the channel is fully-closed.
		//
		// Load this channel's forwarding packages, and deliver them to
		// the switch.
		fwdPkgs, err := s.loadChannelFwdPkgs(shortChanID)
		if err != nil {
			log.Errorf("unable to load forwarding "+
				"packages for %v: %v", shortChanID, err)
			return err
		}

		s.reforwardSettleFails(fwdPkgs)
	}

	return nil
}

// loadChannelFwdPkgs loads all forwarding packages owned by the `source` short
// channel identifier.
func (s *Switch) loadChannelFwdPkgs(source lnwire.ShortChannelID) ([]*channeldb.FwdPkg, error) {

	var fwdPkgs []*channeldb.FwdPkg
	if err := s.cfg.DB.Update(func(tx *bbolt.Tx) error {
		var err error
		fwdPkgs, err = s.cfg.SwitchPackager.LoadChannelFwdPkgs(
			tx, source,
		)
		return err
	}); err != nil {
		return nil, err
	}

	return fwdPkgs, nil
}

// reforwardSettleFails parses the Settle and Fail HTLCs from the list of
// forwarding packages, and reforwards those that have not been acknowledged.
// This is intended to occur on startup, in order to recover the switch's
// mailboxes, and to ensure that responses can be propagated in case the
// outgoing link never comes back online.
//
// NOTE: This should mimic the behavior processRemoteSettleFails.
func (s *Switch) reforwardSettleFails(fwdPkgs []*channeldb.FwdPkg) {
	for _, fwdPkg := range fwdPkgs {
		settleFails, err := lnwallet.PayDescsFromRemoteLogUpdates(
			fwdPkg.Source, fwdPkg.Height, fwdPkg.SettleFails,
		)
		if err != nil {
			log.Errorf("Unable to process remote log updates: %v",
				err)
			continue
		}

		switchPackets := make([]*htlcPacket, 0, len(settleFails))
		for i, pd := range settleFails {

			// Skip any settles or fails that have already been
			// acknowledged by the incoming link that originated the
			// forwarded Add.
			if fwdPkg.SettleFailFilter.Contains(uint16(i)) {
				continue
			}

			switch pd.EntryType {

			// A settle for an HTLC we previously forwarded HTLC has
			// been received. So we'll forward the HTLC to the
			// switch which will handle propagating the settle to
			// the prior hop.
			case lnwallet.Settle:
				settlePacket := &htlcPacket{
					outgoingChanID: fwdPkg.Source,
					outgoingHTLCID: pd.ParentIndex,
					destRef:        pd.DestRef,
					htlc: &lnwire.UpdateFulfillHTLC{
						PaymentPreimage: pd.RPreimage,
					},
				}

				// Add the packet to the batch to be forwarded, and
				// notify the overflow queue that a spare spot has been
				// freed up within the commitment state.
				switchPackets = append(switchPackets, settlePacket)

			// A failureCode message for a previously forwarded HTLC has been
			// received. As a result a new slot will be freed up in our
			// commitment state, so we'll forward this to the switch so the
			// backwards undo can continue.
			case lnwallet.Fail:
				// Fetch the reason the HTLC was cancelled so we can
				// continue to propagate it.
				failPacket := &htlcPacket{
					outgoingChanID: fwdPkg.Source,
					outgoingHTLCID: pd.ParentIndex,
					destRef:        pd.DestRef,
					htlc: &lnwire.UpdateFailHTLC{
						Reason: lnwire.OpaqueReason(pd.FailReason),
					},
				}

				// Add the packet to the batch to be forwarded, and
				// notify the overflow queue that a spare spot has been
				// freed up within the commitment state.
				switchPackets = append(switchPackets, failPacket)
			}
		}

		// Since this send isn't tied to a specific link, we pass a nil
		// link quit channel, meaning the send will fail only if the
		// switch receives a shutdown request.
		errChan := s.ForwardPackets(nil, switchPackets...)
		go handleBatchFwdErrs(errChan)
	}
}

// handleBatchFwdErrs waits on the given errChan until it is closed, logging the
// errors returned from any unsuccessful forwarding attempts.
func handleBatchFwdErrs(errChan chan error) {
	for {
		err, ok := <-errChan
		if !ok {
			// Err chan has been drained or switch is shutting down.
			// Either way, return.
			return
		}

		if err == nil {
			continue
		}

		log.Errorf("unhandled error while reforwarding htlc "+
			"settle/fail over htlcswitch: %v", err)
	}
}

// Stop gracefully stops all active helper goroutines, then waits until they've
// exited.
func (s *Switch) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.shutdown, 0, 1) {
		log.Warn("Htlc Switch already stopped")
		return errors.New("htlc switch already shutdown")
	}

	log.Infof("HTLC Switch shutting down")

	close(s.quit)

	s.wg.Wait()

	// Wait until all active goroutines have finished exiting before
	// stopping the mailboxes, otherwise the mailbox map could still be
	// accessed and modified.
	s.mailOrchestrator.Stop()

	return nil
}

// AddLink is used to initiate the handling of the add link command. The
// request will be propagated and handled in the main goroutine.
func (s *Switch) AddLink(link ChannelLink) error {
	s.indexMtx.Lock()
	defer s.indexMtx.Unlock()

	chanID := link.ChanID()

	// First, ensure that this link is not already active in the switch.
	_, err := s.getLink(chanID)
	if err == nil {
		return fmt.Errorf("unable to add ChannelLink(%v), already "+
			"active", chanID)
	}

	// Get and attach the mailbox for this link, which buffers packets in
	// case there packets that we tried to deliver while this link was
	// offline.
	mailbox := s.mailOrchestrator.GetOrCreateMailBox(chanID)
	link.AttachMailBox(mailbox)

	if err := link.Start(); err != nil {
		s.removeLink(chanID)
		return err
	}

	shortChanID := link.ShortChanID()
	if shortChanID == sourceHop {
		log.Infof("Adding pending link chan_id=%v, short_chan_id=%v",
			chanID, shortChanID)

		s.pendingLinkIndex[chanID] = link
	} else {
		log.Infof("Adding live link chan_id=%v, short_chan_id=%v",
			chanID, shortChanID)

		s.addLiveLink(link)
		s.mailOrchestrator.BindLiveShortChanID(
			mailbox, chanID, shortChanID,
		)
	}

	return nil
}

// addLiveLink adds a link to all associated forwarding index, this makes it a
// candidate for forwarding HTLCs.
func (s *Switch) addLiveLink(link ChannelLink) {
	// We'll add the link to the linkIndex which lets us quickly
	// look up a channel when we need to close or register it, and
	// the forwarding index which'll be used when forwarding HTLC's
	// in the multi-hop setting.
	s.linkIndex[link.ChanID()] = link
	s.forwardingIndex[link.ShortChanID()] = link

	// Next we'll add the link to the interface index so we can
	// quickly look up all the channels for a particular node.
	peerPub := link.Peer().PubKey()
	if _, ok := s.interfaceIndex[peerPub]; !ok {
		s.interfaceIndex[peerPub] = make(map[lnwire.ChannelID]ChannelLink)
	}
	s.interfaceIndex[peerPub][link.ChanID()] = link
}

// GetLink is used to initiate the handling of the get link command. The
// request will be propagated/handled to/in the main goroutine.
func (s *Switch) GetLink(chanID lnwire.ChannelID) (ChannelLink, error) {
	s.indexMtx.RLock()
	defer s.indexMtx.RUnlock()

	return s.getLink(chanID)
}

// getLink returns the link stored in either the pending index or the live
// lindex.
func (s *Switch) getLink(chanID lnwire.ChannelID) (ChannelLink, error) {
	link, ok := s.linkIndex[chanID]
	if !ok {
		link, ok = s.pendingLinkIndex[chanID]
		if !ok {
			return nil, ErrChannelLinkNotFound
		}
	}

	return link, nil
}

// getLinkByShortID attempts to return the link which possesses the target
// short channel ID.
//
// NOTE: This MUST be called with the indexMtx held.
func (s *Switch) getLinkByShortID(chanID lnwire.ShortChannelID) (ChannelLink, error) {
	link, ok := s.forwardingIndex[chanID]
	if !ok {
		return nil, ErrChannelLinkNotFound
	}

	return link, nil
}

// HasActiveLink returns true if the given channel ID has a link in the link
// index AND the link is eligible to forward.
func (s *Switch) HasActiveLink(chanID lnwire.ChannelID) bool {
	s.indexMtx.RLock()
	defer s.indexMtx.RUnlock()

	if link, ok := s.linkIndex[chanID]; ok {
		return link.EligibleToForward()
	}

	return false
}

// RemoveLink purges the switch of any link associated with chanID. If a pending
// or active link is not found, this method does nothing. Otherwise, the method
// returns after the link has been completely shutdown.
func (s *Switch) RemoveLink(chanID lnwire.ChannelID) {
	s.indexMtx.Lock()
	link := s.removeLink(chanID)
	s.indexMtx.Unlock()

	if link != nil {
		link.Stop()
	}
}

// removeLink is used to remove and stop the channel link.
//
// NOTE: This MUST be called with the indexMtx held.
func (s *Switch) removeLink(chanID lnwire.ChannelID) ChannelLink {
	log.Infof("Removing channel link with ChannelID(%v)", chanID)

	link, err := s.getLink(chanID)
	if err != nil {
		return nil
	}

	// Remove the channel from live link indexes.
	delete(s.pendingLinkIndex, link.ChanID())
	delete(s.linkIndex, link.ChanID())
	delete(s.forwardingIndex, link.ShortChanID())

	// If the link has been added to the peer index, then we'll move to
	// delete the entry within the index.
	peerPub := link.Peer().PubKey()
	if peerIndex, ok := s.interfaceIndex[peerPub]; ok {
		delete(peerIndex, link.ChanID())

		// If after deletion, there are no longer any links, then we'll
		// remove the interface map all together.
		if len(peerIndex) == 0 {
			delete(s.interfaceIndex, peerPub)
		}
	}

	return link
}

// UpdateShortChanID updates the short chan ID for an existing channel. This is
// required in the case of a re-org and re-confirmation or a channel, or in the
// case that a link was added to the switch before its short chan ID was known.
func (s *Switch) UpdateShortChanID(chanID lnwire.ChannelID) error {
	s.indexMtx.Lock()
	defer s.indexMtx.Unlock()

	// Locate the target link in the pending link index. If no such link
	// exists, then we will ignore the request.
	link, ok := s.pendingLinkIndex[chanID]
	if !ok {
		return fmt.Errorf("link %v not found", chanID)
	}

	oldShortChanID := link.ShortChanID()

	// Try to update the link's short channel ID, returning early if this
	// update failed.
	shortChanID, err := link.UpdateShortChanID()
	if err != nil {
		return err
	}

	// Reject any blank short channel ids.
	if shortChanID == sourceHop {
		return fmt.Errorf("refusing trivial short_chan_id for chan_id=%v"+
			"live link", chanID)
	}

	log.Infof("Updated short_chan_id for ChannelLink(%v): old=%v, new=%v",
		chanID, oldShortChanID, shortChanID)

	// Since the link was in the pending state before, we will remove it
	// from the pending link index and add it to the live link index so that
	// it can be available in forwarding.
	delete(s.pendingLinkIndex, chanID)
	s.addLiveLink(link)

	// Finally, alert the mail orchestrator to the change of short channel
	// ID, and deliver any unclaimed packets to the link.
	mailbox := s.mailOrchestrator.GetOrCreateMailBox(chanID)
	s.mailOrchestrator.BindLiveShortChanID(
		mailbox, chanID, shortChanID,
	)

	return nil
}

// GetLinksByInterface fetches all the links connected to a particular node
// identified by the serialized compressed form of its public key.
func (s *Switch) GetLinksByInterface(hop [33]byte) ([]ChannelLink, error) {
	s.indexMtx.RLock()
	defer s.indexMtx.RUnlock()

	return s.getLinks(hop)
}

// getLinks is function which returns the channel links of the peer by hop
// destination id.
//
// NOTE: This MUST be called with the indexMtx held.
func (s *Switch) getLinks(destination [33]byte) ([]ChannelLink, error) {
	links, ok := s.interfaceIndex[destination]
	if !ok {
		return nil, ErrNoLinksFound
	}

	channelLinks := make([]ChannelLink, 0, len(links))
	for _, link := range links {
		channelLinks = append(channelLinks, link)
	}

	return channelLinks, nil
}

// removePendingPayment is the helper function which removes the pending user
// payment.
func (s *Switch) removePendingPayment(paymentID uint64) {
	s.pendingMutex.Lock()
	defer s.pendingMutex.Unlock()

	delete(s.pendingPayments, paymentID)
}

// findPayment is the helper function which find the payment.
func (s *Switch) findPayment(paymentID uint64) *pendingPayment {
	s.pendingMutex.RLock()
	defer s.pendingMutex.RUnlock()

	payment, ok := s.pendingPayments[paymentID]
	if !ok {
		log.Errorf("Cannot find pending payment with ID %d",
			paymentID)
		return nil
	}

	return payment
}

// CircuitModifier returns a reference to subset of the interfaces provided by
// the circuit map, to allow links to open and close circuits.
func (s *Switch) CircuitModifier() CircuitModifier {
	return s.circuits
}

// numPendingPayments is helper function which returns the overall number of
// pending user payments.
func (s *Switch) numPendingPayments() int {
	s.pendingMutex.RLock()
	defer s.pendingMutex.RUnlock()

	return len(s.pendingPayments)
}

// commitCircuits persistently adds a circuit to the switch's circuit map.
func (s *Switch) commitCircuits(circuits ...*PaymentCircuit) (
	*CircuitFwdActions, error) {

	return s.circuits.CommitCircuits(circuits...)
}

// openCircuits preemptively writes the keystones for Adds that are about to be
// added to a commitment txn.
func (s *Switch) openCircuits(keystones ...Keystone) error {
	return s.circuits.OpenCircuits(keystones...)
}

// deleteCircuits persistently removes the circuit, and keystone if present,
// from the circuit map.
func (s *Switch) deleteCircuits(inKeys ...CircuitKey) error {
	return s.circuits.DeleteCircuits(inKeys...)
}

// lookupCircuit queries the in memory representation of the circuit map to
// retrieve a particular circuit.
func (s *Switch) lookupCircuit(inKey CircuitKey) *PaymentCircuit {
	return s.circuits.LookupCircuit(inKey)
}

// lookupOpenCircuit queries the in-memory representation of the circuit map for a
// circuit whose outgoing circuit key matches outKey.
func (s *Switch) lookupOpenCircuit(outKey CircuitKey) *PaymentCircuit {
	return s.circuits.LookupOpenCircuit(outKey)
}

// FlushForwardingEvents flushes out the set of pending forwarding events to
// the persistent log. This will be used by the switch to periodically flush
// out the set of forwarding events to disk. External callers can also use this
// method to ensure all data is flushed to dis before querying the log.
func (s *Switch) FlushForwardingEvents() error {
	// First, we'll obtain a copy of the current set of pending forwarding
	// events.
	s.fwdEventMtx.Lock()

	// If we won't have any forwarding events, then we can exit early.
	if len(s.pendingFwdingEvents) == 0 {
		s.fwdEventMtx.Unlock()
		return nil
	}

	events := make([]channeldb.ForwardingEvent, len(s.pendingFwdingEvents))
	copy(events[:], s.pendingFwdingEvents[:])

	// With the copy obtained, we can now clear out the header pointer of
	// the current slice. This way, we can re-use the underlying storage
	// allocated for the slice.
	s.pendingFwdingEvents = s.pendingFwdingEvents[:0]
	s.fwdEventMtx.Unlock()

	// Finally, we'll write out the copied events to the persistent
	// forwarding log.
	return s.cfg.FwdingLog.AddForwardingEvents(events)
}

// BestHeight returns the best height known to the switch.
func (s *Switch) BestHeight() uint32 {
	return atomic.LoadUint32(&s.bestHeight)
}
