package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/davecgh/go-spew/spew"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/roasbeef/btcd/blockchain"
	"github.com/roasbeef/btcd/txscript"
	"github.com/roasbeef/btcd/wire"
	"github.com/roasbeef/btcutil"
)

//                          SUMMARY OF OUTPUT STATES
//
//  - CRIB
//    - SerializedType: babyOutput
//    - OriginalOutputType: HTLC
//    - Awaiting: First-stage HTLC CLTV expiry
//    - HeightIndexEntry: Absolute block height of CLTV expiry.
//    - NextState: KNDR
//  - PSCL
//    - SerializedType: kidOutput
//    - OriginalOutputType: Commitment
//    - Awaiting: Confirmation of commitment txn
//    - HeightIndexEntry: None.
//    - NextState: KNDR
//  - KNDR
//    - SerializedType: kidOutput
//    - OriginalOutputType: Commitment or HTLC
//    - Awaiting: Commitment CSV expiry or second-stage HTLC CSV expiry.
//    - HeightIndexEntry: Input confirmation height + relative CSV delay
//    - NextState: GRAD
//  - GRAD:
//    - SerializedType: kidOutput
//    - OriginalOutputType: Commitment or HTLC
//    - Awaiting: All other outputs in channel to become GRAD.
//    - NextState: Mark channel fully closed in channeldb and remove.
//
//                        DESCRIPTION OF OUTPUT STATES
//
// TODO(roasbeef): update comment with both new output types
//
//  - CRIB (babyOutput) outputs are two-stage htlc outputs that are initially
//    locked using a CLTV delay, followed by a CSV delay. The first stage of a
//    crib output requires broadcasting a presigned htlc timeout txn generated
//    by the wallet after an absolute expiry height. Since the timeout txns are
//    predetermined, they cannot be batched after-the-fact, meaning that all
//    CRIB outputs are broadcast and confirmed independently. After the first
//    stage is complete, a CRIB output is moved to the KNDR state, which will
//    finishing sweeping the second-layer CSV delay.
//
//  - PSCL (kidOutput) outputs are commitment outputs locked under a CSV delay.
//    These outputs are stored temporarily in this state until the commitment
//    transaction confirms, as this solidifies an absolute height that the
//    relative time lock will expire. Once this maturity height is determined,
//    the PSCL output is moved into KNDR.
//
//  - KNDR (kidOutput) outputs are CSV delayed outputs for which the maturity
//    height has been fully determined. This results from having received
//    confirmation of the UTXO we are trying to spend, contained in either the
//    commitment txn or htlc timeout txn. Once the maturity height is reached,
//    the utxo nursery will sweep all KNDR outputs scheduled for that height
//    using a single txn.
//
//    NOTE: Due to the fact that KNDR outputs can be dynamically aggregated and
//    swept, we make precautions to finalize the KNDR outputs at a particular
//    height on our first attempt to sweep it. Finalizing involves signing the
//    sweep transaction and persisting it in the nursery store, and recording
//    the last finalized height. Any attempts to replay an already finalized
//    height will result in broadcasting the already finalized txn, ensuring the
//    nursery does not broadcast different txids for the same batch of KNDR
//    outputs. The reason txids may change is due to the probabilistic nature of
//    generating the pkscript in the sweep txn's output, even if the set of
//    inputs remains static across attempts.
//
//  - GRAD (kidOutput) outputs are KNDR outputs that have successfully been
//    swept into the user's wallet. A channel is considered mature once all of
//    its outputs, including two-stage htlcs, have entered the GRAD state,
//    indicating that it safe to mark the channel as fully closed.
//
//
//                     OUTPUT STATE TRANSITIONS IN UTXO NURSERY
//
//      ┌────────────────┐            ┌──────────────┐
//      │ Commit Outputs │            │ HTLC Outputs │
//      └────────────────┘            └──────────────┘
//               │                            │
//               │                            │
//               │                            │               UTXO NURSERY
//   ┌───────────┼────────────────┬───────────┼───────────────────────────────┐
//   │           │                            │                               │
//   │           │                │           │                               │
//   │           │                            │           CLTV-Delayed        │
//   │           │                │           V            babyOutputs        │
//   │           │                        ┌──────┐                            │
//   │           │                │       │ CRIB │                            │
//   │           │                        └──────┘                            │
//   │           │                │           │                               │
//   │           │                            │                               │
//   │           │                │           |                               │
//   │           │                            V    Wait CLTV                  │
//   │           │                │          [ ]       +                      │
//   │           │                            |   Publish Txn                 │
//   │           │                │           │                               │
//   │           │                            │                               │
//   │           │                │           V ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┐        │
//   │           │                           ( )  waitForTimeoutConf          │
//   │           │                │           | └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┘        │
//   │           │                            │                               │
//   │           │                │           │                               │
//   │           │                            │                               │
//   │           V                │           │                               │
//   │       ┌──────┐                         │                               │
//   │       │ PSCL │             └  ──  ──  ─┼  ──  ──  ──  ──  ──  ──  ──  ─┤
//   │       └──────┘                         │                               │
//   │           │                            │                               │
//   │           │                            │                               │
//   │           V ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┐     │            CSV-Delayed        │
//   │          ( )  waitForCommitConf        │             kidOutputs        │
//   │           | └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┘     │                               │
//   │           │                            │                               │
//   │           │                            │                               │
//   │           │                            V                               │
//   │           │                        ┌──────┐                            │
//   │           └─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─▶│ KNDR │                            │
//   │                                    └──────┘                            │
//   │                                        │                               │
//   │                                        │                               │
//   │                                        |                               │
//   │                                        V     Wait CSV                  │
//   │                                       [ ]       +                      │
//   │                                        |   Publish Txn                 │
//   │                                        │                               │
//   │                                        │                               │
//   │                                        V ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐         │
//   │                                       ( )  waitForSweepConf            │
//   │                                        | └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘         │
//   │                                        │                               │
//   │                                        │                               │
//   │                                        V                               │
//   │                                     ┌──────┐                           │
//   │                                     │ GRAD │                           │
//   │                                     └──────┘                           │
//   │                                        │                               │
//   │                                        │                               │
//   │                                        │                               │
//   └────────────────────────────────────────┼───────────────────────────────┘
//                                            │
//                                            │
//                                            │
//                                            │
//                                            V
//                                   ┌────────────────┐
//                                   │ Wallet Outputs │
//                                   └────────────────┘

var byteOrder = binary.BigEndian

var (
	// ErrContractNotFound is returned when the nursery is unable to
	// retrieve information about a queried contract.
	ErrContractNotFound = fmt.Errorf("unable to locate contract")
)

// NurseryConfig abstracts the required subsystems used by the utxo nursery. An
// instance of NurseryConfig is passed to newUtxoNursery during instantiation.
type NurseryConfig struct {
	// ChainIO is used by the utxo nursery to determine the current block
	// height, which drives the incubation of the nursery's outputs.
	ChainIO lnwallet.BlockChainIO

	// ConfDepth is the number of blocks the nursery store waits before
	// determining outputs in the chain as confirmed.
	ConfDepth uint32

	// DB provides access to a user's channels, such that they can be marked
	// fully closed after incubation has concluded.
	DB *channeldb.DB

	// Estimator is used when crafting sweep transactions to estimate the
	// necessary fee relative to the expected size of the sweep transaction.
	Estimator lnwallet.FeeEstimator

	// GenSweepScript generates a P2WKH script belonging to the wallet where
	// funds can be swept.
	GenSweepScript func() ([]byte, error)

	// Notifier provides the utxo nursery the ability to subscribe to
	// transaction confirmation events, which advance outputs through their
	// persistence state transitions.
	Notifier chainntnfs.ChainNotifier

	// PublishTransaction facilitates the process of broadcasting a signed
	// transaction to the appropriate network.
	PublishTransaction func(*wire.MsgTx) error

	// Signer is used by the utxo nursery to generate valid witnesses at the
	// time the incubated outputs need to be spent.
	Signer lnwallet.Signer

	// Store provides access to and modification of the persistent state
	// maintained about the utxo nursery's incubating outputs.
	Store NurseryStore
}

// utxoNursery is a system dedicated to incubating time-locked outputs created
// by the broadcast of a commitment transaction either by us, or the remote
// peer. The nursery accepts outputs and "incubates" them until they've reached
// maturity, then sweep the outputs into the source wallet. An output is
// considered mature after the relative time-lock within the pkScript has
// passed. As outputs reach their maturity age, they're swept in batches into
// the source wallet, returning the outputs so they can be used within future
// channels, or regular Bitcoin transactions.
type utxoNursery struct {
	started uint32 // To be used atomically.
	stopped uint32 // To be used atomically.

	cfg *NurseryConfig

	mu         sync.Mutex
	bestHeight uint32

	quit chan struct{}
	wg   sync.WaitGroup
}

// newUtxoNursery creates a new instance of the utxoNursery from a
// ChainNotifier and LightningWallet instance.
func newUtxoNursery(cfg *NurseryConfig) *utxoNursery {
	return &utxoNursery{
		cfg:  cfg,
		quit: make(chan struct{}),
	}
}

// Start launches all goroutines the utxoNursery needs to properly carry out
// its duties.
func (u *utxoNursery) Start() error {
	if !atomic.CompareAndSwapUint32(&u.started, 0, 1) {
		return nil
	}

	utxnLog.Tracef("Starting UTXO nursery")

	// 1. Start watching for new blocks, as this will drive the nursery
	// store's state machine.

	// Register with the notifier to receive notifications for each newly
	// connected block. We register immediately on startup to ensure that
	// no blocks are missed while we are handling blocks that were missed
	// during the time the UTXO nursery was unavailable.
	newBlockChan, err := u.cfg.Notifier.RegisterBlockEpochNtfn()
	if err != nil {
		return err
	}

	// 2. Flush all fully-graduated channels from the pipeline.

	// Load any pending close channels, which represents the super set of
	// all channels that may still be incubating.
	pendingCloseChans, err := u.cfg.DB.FetchClosedChannels(true)
	if err != nil {
		newBlockChan.Cancel()
		return err
	}

	// Ensure that all mature channels have been marked as fully closed in
	// the channeldb.
	for _, pendingClose := range pendingCloseChans {
		err := u.closeAndRemoveIfMature(&pendingClose.ChanPoint)
		if err != nil {
			newBlockChan.Cancel()
			return err
		}
	}

	// TODO(conner): check if any fully closed channels can be removed from
	// utxn.

	// Query the nursery store for the lowest block height we could be
	// incubating, which is taken to be the last height for which the
	// database was purged.
	lastGraduatedHeight, err := u.cfg.Store.LastGraduatedHeight()
	if err != nil {
		newBlockChan.Cancel()
		return err
	}

	// 2. Restart spend ntfns for any preschool outputs, which are waiting
	// for the force closed commitment txn to confirm, or any second-layer
	// HTLC success transactions.
	//
	// NOTE: The next two steps *may* spawn go routines, thus from this
	// point forward, we must close the nursery's quit channel if we detect
	// any failures during startup to ensure they terminate.
	if err := u.reloadPreschool(); err != nil {
		newBlockChan.Cancel()
		close(u.quit)
		return err
	}

	// 3. Replay all crib and kindergarten outputs from last pruned to
	// current best height.
	if err := u.reloadClasses(lastGraduatedHeight); err != nil {
		newBlockChan.Cancel()
		close(u.quit)
		return err
	}

	u.wg.Add(1)
	go u.incubator(newBlockChan)

	return nil
}

// Stop gracefully shuts down any lingering goroutines launched during normal
// operation of the utxoNursery.
func (u *utxoNursery) Stop() error {
	if !atomic.CompareAndSwapUint32(&u.stopped, 0, 1) {
		return nil
	}

	utxnLog.Infof("UTXO nursery shutting down")

	close(u.quit)
	u.wg.Wait()

	return nil
}

// IncubateOutputs sends a request to the utxoNursery to incubate a set of
// outputs from an existing commitment transaction. Outputs need to incubate if
// they're CLTV absolute time locked, or if they're CSV relative time locked.
// Once all outputs reach maturity, they'll be swept back into the wallet.
func (u *utxoNursery) IncubateOutputs(chanPoint wire.OutPoint,
	commitResolution *lnwallet.CommitOutputResolution,
	outgoingHtlcs []lnwallet.OutgoingHtlcResolution,
	incomingHtlcs []lnwallet.IncomingHtlcResolution) error {

	numHtlcs := len(incomingHtlcs) + len(outgoingHtlcs)
	var (
		hasCommit bool

		// Kid outputs can be swept after an initial confirmation
		// followed by a maturity period.Baby outputs are two stage and
		// will need to wait for an absolute time out to reach a
		// confirmation, then require a relative confirmation delay.
		kidOutputs  = make([]kidOutput, 0, 1+len(incomingHtlcs))
		babyOutputs = make([]babyOutput, 0, len(outgoingHtlcs))
	)

	// 1. Build all the spendable outputs that we will try to incubate.

	// It could be that our to-self output was below the dust limit. In
	// that case the commit resolution would be nil and we would not have
	// that output to incubate.
	if commitResolution != nil {
		hasCommit = true
		selfOutput := makeKidOutput(
			&commitResolution.SelfOutPoint,
			&chanPoint,
			commitResolution.MaturityDelay,
			lnwallet.CommitmentTimeLock,
			&commitResolution.SelfOutputSignDesc,
			0,
		)

		// We'll skip any zero valued outputs as this indicates we
		// don't have a settled balance within the commitment
		// transaction.
		if selfOutput.Amount() > 0 {
			kidOutputs = append(kidOutputs, selfOutput)
		}
	}

	// TODO(roasbeef): query and see if we already have, if so don't add?

	// For each incoming HTLC, we'll register a kid output marked as a
	// second-layer HTLC output. We effectively skip the baby stage (as the
	// timelock is zero), and enter the kid stage.
	for _, htlcRes := range incomingHtlcs {
		htlcOutput := makeKidOutput(
			&htlcRes.ClaimOutpoint, &chanPoint, htlcRes.CsvDelay,
			lnwallet.HtlcAcceptedSuccessSecondLevel,
			&htlcRes.SweepSignDesc, 0,
		)

		if htlcOutput.Amount() > 0 {
			kidOutputs = append(kidOutputs, htlcOutput)
		}
	}

	// For each outgoing HTLC, we'll create a baby output. If this is our
	// commitment transaction, then we'll broadcast a second-layer
	// transaction to transition to a kid output. Otherwise, we'll directly
	// spend once the CLTV delay us up.
	for _, htlcRes := range outgoingHtlcs {
		// If this HTLC is on our commitment transaction, then it'll be
		// a baby output as we need to go to the second level to sweep
		// it.
		if htlcRes.SignedTimeoutTx != nil {
			htlcOutput := makeBabyOutput(&chanPoint, &htlcRes)

			if htlcOutput.Amount() > 0 {
				babyOutputs = append(babyOutputs, htlcOutput)
			}
			continue
		}

		// Otherwise, this is actually a kid output as we can sweep it
		// once the commitment transaction confirms, and the absolute
		// CLTV lock has expired. We set the CSV delay to zero to
		// indicate this is actually a CLTV output.
		htlcOutput := makeKidOutput(
			&htlcRes.ClaimOutpoint, &chanPoint, 0,
			lnwallet.HtlcOfferedRemoteTimeout,
			&htlcRes.SweepSignDesc, htlcRes.Expiry,
		)
		kidOutputs = append(kidOutputs, htlcOutput)
	}

	// TODO(roasbeef): if want to handle outgoing on remote commit
	//  * need ability to cancel in the case that we learn of pre-image or
	//    remote party pulls

	utxnLog.Infof("Incubating Channel(%s) has-commit=%v, num-htlcs=%d",
		chanPoint, hasCommit, numHtlcs)

	u.mu.Lock()
	defer u.mu.Unlock()

	// 2. Persist the outputs we intended to sweep in the nursery store
	if err := u.cfg.Store.Incubate(kidOutputs, babyOutputs); err != nil {
		utxnLog.Errorf("unable to begin incubation of Channel(%s): %v",
			chanPoint, err)
		return err
	}

	// As an intermediate step, we'll now check to see if any of the baby
	// outputs has actually _already_ expired. This may be the case if
	// blocks were mined while we processed this message.
	_, bestHeight, err := u.cfg.ChainIO.GetBestBlock()
	if err != nil {
		return err
	}

	// We'll examine all the baby outputs just inserted into the database,
	// if the output has already expired, then we'll *immediately* sweep
	// it. This may happen if the caller raced a block to call this method.
	for _, babyOutput := range babyOutputs {
		if uint32(bestHeight) >= babyOutput.expiry {
			err = u.sweepCribOutput(uint32(bestHeight), &babyOutput)
			if err != nil {
				return err
			}
		}
	}

	// 3. If we are incubating any preschool outputs, register for a
	// confirmation notification that will transition it to the
	// kindergarten bucket.
	if len(kidOutputs) != 0 {
		for _, kidOutput := range kidOutputs {
			err := u.registerPreschoolConf(&kidOutput, u.bestHeight)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// NurseryReport attempts to return a nursery report stored for the target
// outpoint. A nursery report details the maturity/sweeping progress for a
// contract that was previously force closed. If a report entry for the target
// chanPoint is unable to be constructed, then an error will be returned.
func (u *utxoNursery) NurseryReport(
	chanPoint *wire.OutPoint) (*contractMaturityReport, error) {

	u.mu.Lock()
	defer u.mu.Unlock()

	utxnLog.Infof("NurseryReport: building nursery report for channel %v",
		chanPoint)

	report := &contractMaturityReport{
		chanPoint: *chanPoint,
	}

	if err := u.cfg.Store.ForChanOutputs(chanPoint, func(k, v []byte) error {
		switch {
		case bytes.HasPrefix(k, cribPrefix):
			// Cribs outputs are the only kind currently stored as
			// baby outputs.
			var baby babyOutput
			err := baby.Decode(bytes.NewReader(v))
			if err != nil {
				return err
			}

			// Each crib output represents a stage one htlc, and
			// will contribute towards the limbo balance.
			report.AddLimboStage1TimeoutHtlc(&baby)

		case bytes.HasPrefix(k, psclPrefix),
			bytes.HasPrefix(k, kndrPrefix),
			bytes.HasPrefix(k, gradPrefix):

			// All others states can be deserialized as kid outputs.
			var kid kidOutput
			err := kid.Decode(bytes.NewReader(v))
			if err != nil {
				return err
			}

			// Now, use the state prefixes to determine how the
			// this output should be represented in the nursery
			// report.  An output's funds are always in limbo until
			// reaching the graduate state.
			switch {
			case bytes.HasPrefix(k, psclPrefix):
				// Preschool outputs are awaiting the
				// confirmation of the commitment transaction.
				switch kid.WitnessType() {
				case lnwallet.CommitmentTimeLock:
					report.AddLimboCommitment(&kid)

				// An HTLC output on our commitment transaction
				// where the second-layer transaction hasn't
				// yet confirmed.
				case lnwallet.HtlcAcceptedSuccessSecondLevel:
					report.AddLimboStage1SuccessHtlc(&kid)
				}

			case bytes.HasPrefix(k, kndrPrefix):
				// Kindergarten outputs may originate from
				// either the commitment transaction or an htlc.
				// We can distinguish them via their witness
				// types.
				switch kid.WitnessType() {
				case lnwallet.CommitmentTimeLock:
					// The commitment transaction has been
					// confirmed, and we are waiting the CSV
					// delay to expire.
					report.AddLimboCommitment(&kid)

				case lnwallet.HtlcOfferedRemoteTimeout:
					// This is an HTLC output on the
					// commitment transaction of the remote
					// party. The CLTV timelock has
					// expired, and we only need to sweep
					// it.
					report.AddLimboDirectHtlc(&kid)

				case lnwallet.HtlcAcceptedSuccessSecondLevel:
					fallthrough
				case lnwallet.HtlcOfferedTimeoutSecondLevel:
					// The htlc timeout or success
					// transaction has confirmed, and the
					// CSV delay has begun ticking.
					report.AddLimboStage2Htlc(&kid)
				}

			case bytes.HasPrefix(k, gradPrefix):
				// Graduate outputs are those whose funds have
				// been swept back into the wallet. Each output
				// will contribute towards the recovered
				// balance.
				switch kid.WitnessType() {
				case lnwallet.CommitmentTimeLock:
					// The commitment output was
					// successfully swept back into a
					// regular p2wkh output.
					report.AddRecoveredCommitment(&kid)

				case lnwallet.HtlcAcceptedSuccessSecondLevel:
					fallthrough
				case lnwallet.HtlcOfferedTimeoutSecondLevel:
					fallthrough
				case lnwallet.HtlcOfferedRemoteTimeout:
					// This htlc output successfully
					// resides in a p2wkh output belonging
					// to the user.
					report.AddRecoveredHtlc(&kid)
				}
			}

		default:
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return report, nil
}

// reloadPreschool re-initializes the chain notifier with all of the outputs
// that had been saved to the "preschool" database bucket prior to shutdown.
func (u *utxoNursery) reloadPreschool() error {
	psclOutputs, err := u.cfg.Store.FetchPreschools()
	if err != nil {
		return err
	}

	// For each of the preschool outputs stored in the nursery store, load
	// its close summary from disk so that we can get an accurate height
	// hint from which to start our range for spend notifications.
	for i := range psclOutputs {
		kid := &psclOutputs[i]
		chanPoint := kid.OriginChanPoint()

		// Load the close summary for this output's channel point.
		closeSummary, err := u.cfg.DB.FetchClosedChannel(chanPoint)
		if err == channeldb.ErrClosedChannelNotFound {
			// This should never happen since the close summary
			// should only be removed after the channel has been
			// swept completely.
			utxnLog.Warnf("Close summary not found for "+
				"chan_point=%v, can't determine height hint"+
				"to sweep commit txn", chanPoint)
			continue

		} else if err != nil {
			return err
		}

		// Use the close height from the channel summary as our height
		// hint to drive our spend notifications, with our confirmation
		// depth as a buffer for reorgs.
		heightHint := closeSummary.CloseHeight - u.cfg.ConfDepth
		err = u.registerPreschoolConf(kid, heightHint)
		if err != nil {
			return err
		}
	}

	return nil
}

// reloadClasses reinitializes any height-dependent state transitions for which
// the utxonursery has not received confirmation, and replays the graduation of
// all kindergarten and crib outputs for heights that have not been finalized.
// This allows the nursery to reinitialize all state to continue sweeping
// outputs, even in the event that we missed blocks while offline.
// reloadClasses is called during the startup of the UTXO Nursery.
func (u *utxoNursery) reloadClasses(lastGradHeight uint32) error {
	// Begin by loading all of the still-active heights up to and including
	// the last height we successfully graduated.
	activeHeights, err := u.cfg.Store.HeightsBelowOrEqual(lastGradHeight)
	if err != nil {
		return err
	}

	if len(activeHeights) > 0 {
		utxnLog.Infof("Re-registering confirmations for %d already "+
			"graduated heights below height=%d", len(activeHeights),
			lastGradHeight)
	}

	// Attempt to re-register notifications for any outputs still at these
	// heights.
	for _, classHeight := range activeHeights {
		utxnLog.Debugf("Attempting to regraduate outputs at height=%v",
			classHeight)

		if err = u.regraduateClass(classHeight); err != nil {
			utxnLog.Errorf("Failed to regraduate outputs at "+
				"height=%v: %v", classHeight, err)
			return err
		}
	}

	// Get the most recently mined block.
	_, bestHeight, err := u.cfg.ChainIO.GetBestBlock()
	if err != nil {
		return err
	}

	// If we haven't yet seen any registered force closes, or we're already
	// caught up with the current best chain, then we can exit early.
	if lastGradHeight == 0 || uint32(bestHeight) == lastGradHeight {
		return nil
	}

	utxnLog.Infof("Processing outputs from missed blocks. Starting with "+
		"blockHeight=%v, to current blockHeight=%v", lastGradHeight,
		bestHeight)

	// Loop through and check for graduating outputs at each of the missed
	// block heights.
	for curHeight := lastGradHeight + 1; curHeight <= uint32(bestHeight); curHeight++ {
		utxnLog.Debugf("Attempting to graduate outputs at height=%v",
			curHeight)

		if err := u.graduateClass(curHeight); err != nil {
			utxnLog.Errorf("Failed to graduate outputs at "+
				"height=%v: %v", curHeight, err)
			return err
		}
	}

	utxnLog.Infof("UTXO Nursery is now fully synced")

	return nil
}

// regraduateClass handles the steps involved in re-registering for
// confirmations for all still-active outputs at a particular height. This is
// used during restarts to ensure that any still-pending state transitions are
// properly registered, so they can be driven by the chain notifier. No
// transactions or signing are done as a result of this step.
func (u *utxoNursery) regraduateClass(classHeight uint32) error {
	// Fetch all information about the crib and kindergarten outputs at
	// this height. In addition to the outputs, we also retrieve the
	// finalized kindergarten sweep txn, which will be nil if we have not
	// attempted this height before, or if no kindergarten outputs exist at
	// this height.
	finalTx, kgtnOutputs, cribOutputs, err := u.cfg.Store.FetchClass(
		classHeight)
	if err != nil {
		return err
	}

	if finalTx != nil {
		utxnLog.Infof("Re-registering confirmation for kindergarten "+
			"sweep transaction at height=%d ", classHeight)

		err = u.sweepMatureOutputs(classHeight, finalTx, kgtnOutputs)
		if err != nil {
			utxnLog.Errorf("Failed to re-register for kindergarten "+
				"sweep transaction at height=%d: %v",
				classHeight, err)
			return err
		}
	}

	if len(cribOutputs) == 0 {
		return nil
	}

	utxnLog.Infof("Re-registering confirmation for first-stage HTLC "+
		"outputs at height=%d ", classHeight)

	// Now, we broadcast all pre-signed htlc txns from the crib outputs at
	// this height. There is no need to finalize these txns, since the txid
	// is predetermined when signed in the wallet.
	for i := range cribOutputs {
		err := u.sweepCribOutput(classHeight, &cribOutputs[i])
		if err != nil {
			utxnLog.Errorf("Failed to re-register first-stage "+
				"HTLC output %v", cribOutputs[i].OutPoint())
			return err
		}
	}

	return nil
}

// incubator is tasked with driving all state transitions that are dependent on
// the current height of the blockchain. As new blocks arrive, the incubator
// will attempt spend outputs at the latest height. The asynchronous
// confirmation of these spends will either 1) move a crib output into the
// kindergarten bucket or 2) move a kindergarten output into the graduated
// bucket.
func (u *utxoNursery) incubator(newBlockChan *chainntnfs.BlockEpochEvent) {
	defer u.wg.Done()
	defer newBlockChan.Cancel()

	for {
		select {
		case epoch, ok := <-newBlockChan.Epochs:
			// If the epoch channel has been closed, then the
			// ChainNotifier is exiting which means the daemon is
			// as well. Therefore, we exit early also in order to
			// ensure the daemon shuts down gracefully, yet
			// swiftly.
			if !ok {
				return
			}

			// TODO(roasbeef): if the BlockChainIO is rescanning
			// will give stale data

			// A new block has just been connected to the main
			// chain, which means we might be able to graduate crib
			// or kindergarten outputs at this height. This involves
			// broadcasting any presigned htlc timeout txns, as well
			// as signing and broadcasting a sweep txn that spends
			// from all kindergarten outputs at this height.
			height := uint32(epoch.Height)
			if err := u.graduateClass(height); err != nil {
				utxnLog.Errorf("error while graduating "+
					"class at height=%d: %v", height, err)

				// TODO(conner): signal fatal error to daemon
			}

		case <-u.quit:
			return
		}
	}
}

// graduateClass handles the steps involved in spending outputs whose CSV or
// CLTV delay expires at the nursery's current height. This method is called
// each time a new block arrives, or during startup to catch up on heights we
// may have missed while the nursery was offline.
func (u *utxoNursery) graduateClass(classHeight uint32) error {
	// Record this height as the nursery's current best height.
	u.mu.Lock()
	defer u.mu.Unlock()

	u.bestHeight = classHeight

	// Fetch all information about the crib and kindergarten outputs at
	// this height. In addition to the outputs, we also retrieve the
	// finalized kindergarten sweep txn, which will be nil if we have not
	// attempted this height before, or if no kindergarten outputs exist at
	// this height.
	finalTx, kgtnOutputs, cribOutputs, err := u.cfg.Store.FetchClass(
		classHeight)
	if err != nil {
		return err
	}

	utxnLog.Infof("Attempting to graduate height=%v: num_kids=%v, "+
		"num_babies=%v", classHeight, len(kgtnOutputs), len(cribOutputs))

	// Load the last finalized height, so we can determine if the
	// kindergarten sweep txn should be crafted.
	lastFinalizedHeight, err := u.cfg.Store.LastFinalizedHeight()
	if err != nil {
		return err
	}

	// If we haven't processed this height before, we finalize the
	// graduating kindergarten outputs, by signing a sweep transaction that
	// spends from them. This txn is persisted such that we never broadcast
	// a different txn for the same height. This allows us to recover from
	// failures, and watch for the correct txid.
	if classHeight > lastFinalizedHeight {
		// If this height has never been finalized, we have never
		// generated a sweep txn for this height. Generate one if there
		// are kindergarten outputs or cltv crib outputs to be spent.
		if len(kgtnOutputs) > 0 {
			finalTx, err = u.createSweepTx(kgtnOutputs, classHeight)
			if err != nil {
				utxnLog.Errorf("Failed to create sweep txn at "+
					"height=%d", classHeight)
				return err
			}
		}

		// Persist the kindergarten sweep txn to the nursery store. It
		// is safe to store a nil finalTx, which happens if there are
		// no graduating kindergarten outputs.
		err = u.cfg.Store.FinalizeKinder(classHeight, finalTx)
		if err != nil {
			utxnLog.Errorf("Failed to finalize kindergarten at "+
				"height=%d", classHeight)

			return err
		}

		// Log if the finalized transaction is non-trivial.
		if finalTx != nil {
			utxnLog.Infof("Finalized kindergarten at height=%d ",
				classHeight)
		}
	}

	// Now that the kindergarten sweep txn has either been finalized or
	// restored, broadcast the txn, and set up notifications that will
	// transition the swept kindergarten outputs and cltvCrib into
	// graduated outputs.
	if finalTx != nil {
		err := u.sweepMatureOutputs(classHeight, finalTx, kgtnOutputs)
		if err != nil {
			utxnLog.Errorf("Failed to sweep %d kindergarten "+
				"outputs at height=%d: %v",
				len(kgtnOutputs), classHeight, err)
			return err
		}
	}

	// Now, we broadcast all pre-signed htlc txns from the csv crib outputs
	// at this height. There is no need to finalize these txns, since the
	// txid is predetermined when signed in the wallet.
	for i := range cribOutputs {
		err := u.sweepCribOutput(classHeight, &cribOutputs[i])
		if err != nil {
			utxnLog.Errorf("Failed to sweep first-stage HTLC "+
				"(CLTV-delayed) output %v",
				cribOutputs[i].OutPoint())
			return err
		}
	}

	return u.cfg.Store.GraduateHeight(classHeight)
}

// craftSweepTx accepts a list of kindergarten outputs, and baby
// outputs which don't require a second-layer claim, and signs and generates a
// signed txn that spends from them. This method also makes an accurate fee
// estimate before generating the required witnesses.
func (u *utxoNursery) createSweepTx(kgtnOutputs []kidOutput,
	classHeight uint32) (*wire.MsgTx, error) {

	// Create a transaction which sweeps all the newly mature outputs into
	// an output controlled by the wallet.

	// TODO(roasbeef): can be more intelligent about buffering outputs to
	// be more efficient on-chain.

	// Assemble the kindergarten class into a slice csv spendable outputs,
	// and also a set of regular spendable outputs. The set of regular
	// outputs are CLTV locked outputs that have had their timelocks
	// expire.
	var (
		csvOutputs     []CsvSpendableOutput
		cltvOutputs    []SpendableOutput
		weightEstimate lnwallet.TxWeightEstimator
	)

	// Allocate enough room for both types of kindergarten outputs.
	csvOutputs = make([]CsvSpendableOutput, 0, len(kgtnOutputs))
	cltvOutputs = make([]SpendableOutput, 0, len(kgtnOutputs))

	// Our sweep transaction will pay to a single segwit p2wkh address,
	// ensure it contributes to our weight estimate.
	weightEstimate.AddP2WKHOutput()

	// For each kindergarten output, use its witness type to determine the
	// estimate weight of its witness, and add it to the proper set of
	// spendable outputs.
	for i := range kgtnOutputs {
		input := &kgtnOutputs[i]

		switch input.WitnessType() {

		// Outputs on a past commitment transaction that pay directly
		// to us.
		case lnwallet.CommitmentTimeLock:
			weightEstimate.AddWitnessInput(
				lnwallet.ToLocalTimeoutWitnessSize,
			)
			csvOutputs = append(csvOutputs, input)

		// Outgoing second layer HTLC's that have confirmed within the
		// chain, and the output they produced is now mature enough to
		// sweep.
		case lnwallet.HtlcOfferedTimeoutSecondLevel:
			weightEstimate.AddWitnessInput(
				lnwallet.SecondLevelHtlcSuccessWitnessSize,
			)
			csvOutputs = append(csvOutputs, input)

		// Incoming second layer HTLC's that have confirmed within the
		// chain, and the output they produced is now mature enough to
		// sweep.
		case lnwallet.HtlcAcceptedSuccessSecondLevel:
			weightEstimate.AddWitnessInput(
				lnwallet.SecondLevelHtlcSuccessWitnessSize,
			)
			csvOutputs = append(csvOutputs, input)

		// An HTLC on the commitment transaction of the remote party,
		// that has had its absolute timelock expire.
		case lnwallet.HtlcOfferedRemoteTimeout:
			weightEstimate.AddWitnessInput(
				lnwallet.AcceptedHtlcTimeoutWitnessSize,
			)
			cltvOutputs = append(cltvOutputs, input)

		default:
			utxnLog.Warnf("kindergarten output in nursery store "+
				"contains unexpected witness type: %v",
				input.WitnessType())
			continue
		}
	}

	utxnLog.Infof("Creating sweep transaction for %v CSV inputs, %v CLTV "+
		"inputs", len(csvOutputs), len(cltvOutputs))

	txVSize := int64(weightEstimate.VSize())
	return u.populateSweepTx(txVSize, classHeight, csvOutputs, cltvOutputs)
}

// populateSweepTx populate the final sweeping transaction with all witnesses
// in place for all inputs using the provided txn fee. The created transaction
// has a single output sending all the funds back to the source wallet, after
// accounting for the fee estimate.
func (u *utxoNursery) populateSweepTx(txVSize int64, classHeight uint32,
	csvInputs []CsvSpendableOutput,
	cltvInputs []SpendableOutput) (*wire.MsgTx, error) {

	// Generate the receiving script to which the funds will be swept.
	pkScript, err := u.cfg.GenSweepScript()
	if err != nil {
		return nil, err
	}

	// Sum up the total value contained in the inputs.
	var totalSum btcutil.Amount
	for _, o := range csvInputs {
		totalSum += o.Amount()
	}
	for _, o := range cltvInputs {
		totalSum += o.Amount()
	}

	// Using the txn weight estimate, compute the required txn fee.
	feePerVSize, err := u.cfg.Estimator.EstimateFeePerVSize(6)
	if err != nil {
		return nil, err
	}
	txFee := feePerVSize.FeeForVSize(txVSize)

	// Sweep as much possible, after subtracting txn fees.
	sweepAmt := int64(totalSum - txFee)

	// Create the sweep transaction that we will be building. We use
	// version 2 as it is required for CSV. The txn will sweep the amount
	// after fees to the pkscript generated above.
	sweepTx := wire.NewMsgTx(2)
	sweepTx.AddTxOut(&wire.TxOut{
		PkScript: pkScript,
		Value:    sweepAmt,
	})

	// We'll also ensure that the transaction has the required lock time if
	// we're sweeping any cltvInputs.
	if len(cltvInputs) > 0 {
		sweepTx.LockTime = classHeight
	}

	// Add all inputs to the sweep transaction. Ensure that for each
	// csvInput, we set the sequence number properly.
	for _, input := range csvInputs {
		sweepTx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: *input.OutPoint(),
			Sequence:         input.BlocksToMaturity(),
		})
	}
	for _, input := range cltvInputs {
		sweepTx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: *input.OutPoint(),
		})
	}

	// Before signing the transaction, check to ensure that it meets some
	// basic validity requirements.
	// TODO(conner): add more control to sanity checks, allowing us to delay
	// spending "problem" outputs, e.g. possibly batching with other classes
	// if fees are too low.
	btx := btcutil.NewTx(sweepTx)
	if err := blockchain.CheckTransactionSanity(btx); err != nil {
		return nil, err
	}

	hashCache := txscript.NewTxSigHashes(sweepTx)

	// With all the inputs in place, use each output's unique witness
	// function to generate the final witness required for spending.
	addWitness := func(idx int, tso SpendableOutput) error {
		witness, err := tso.BuildWitness(
			u.cfg.Signer, sweepTx, hashCache, idx,
		)
		if err != nil {
			return err
		}

		sweepTx.TxIn[idx].Witness = witness

		return nil
	}

	// Finally we'll attach a valid witness to each csv and cltv input
	// within the sweeping transaction.
	for i, input := range csvInputs {
		if err := addWitness(i, input); err != nil {
			return nil, err
		}
	}

	// Add offset to relative indexes so cltv witnesses don't overwrite csv
	// witnesses.
	offset := len(csvInputs)
	for i, input := range cltvInputs {
		if err := addWitness(offset+i, input); err != nil {
			return nil, err
		}
	}

	return sweepTx, nil
}

// sweepMatureOutputs generates and broadcasts the transaction that transfers
// control of funds from a prior channel commitment transaction to the user's
// wallet. The outputs swept were previously time locked (either absolute or
// relative), but are not mature enough to sweep into the wallet.
func (u *utxoNursery) sweepMatureOutputs(classHeight uint32, finalTx *wire.MsgTx,
	kgtnOutputs []kidOutput) error {

	utxnLog.Infof("Sweeping %v CSV-delayed outputs with sweep tx "+
		"(txid=%v): %v", len(kgtnOutputs),
		finalTx.TxHash(), newLogClosure(func() string {
			return spew.Sdump(finalTx)
		}),
	)

	// With the sweep transaction fully signed, broadcast the transaction
	// to the network. Additionally, we can stop tracking these outputs as
	// they've just been swept.
	err := u.cfg.PublishTransaction(finalTx)
	if err != nil && err != lnwallet.ErrDoubleSpend {
		utxnLog.Errorf("unable to broadcast sweep tx: %v, %v",
			err, spew.Sdump(finalTx))
		return err
	}

	return u.registerSweepConf(finalTx, kgtnOutputs, classHeight)
}

// registerSweepConf is responsible for registering a finalized kindergarten
// sweep transaction for confirmation notifications. If the confirmation was
// successfully registered, a goroutine will be spawned that waits for the
// confirmation, and graduates the provided kindergarten class within the
// nursery store.
func (u *utxoNursery) registerSweepConf(finalTx *wire.MsgTx,
	kgtnOutputs []kidOutput, heightHint uint32) error {

	finalTxID := finalTx.TxHash()

	confChan, err := u.cfg.Notifier.RegisterConfirmationsNtfn(
		&finalTxID, u.cfg.ConfDepth, heightHint)
	if err != nil {
		utxnLog.Errorf("unable to register notification for "+
			"sweep confirmation: %v", finalTxID)
		return err
	}

	utxnLog.Infof("Registering sweep tx %v for confs at height=%d",
		finalTxID, heightHint)

	u.wg.Add(1)
	go u.waitForSweepConf(heightHint, kgtnOutputs, confChan)

	return nil
}

// waitForSweepConf watches for the confirmation of a sweep transaction
// containing a batch of kindergarten outputs. Once confirmation has been
// received, the nursery will mark those outputs as fully graduated, and proceed
// to mark any mature channels as fully closed in channeldb.
// NOTE(conner): this method MUST be called as a go routine.
func (u *utxoNursery) waitForSweepConf(classHeight uint32,
	kgtnOutputs []kidOutput, confChan *chainntnfs.ConfirmationEvent) {

	defer u.wg.Done()

	select {
	case _, ok := <-confChan.Confirmed:
		if !ok {
			utxnLog.Errorf("Notification chan closed, can't"+
				" advance %v graduating outputs",
				len(kgtnOutputs))
			return
		}

	case <-u.quit:
		return
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	// TODO(conner): add retry logic?

	// Mark the confirmed kindergarten outputs as graduated.
	if err := u.cfg.Store.GraduateKinder(classHeight); err != nil {
		utxnLog.Errorf("Unable to graduate %v kindergarten outputs: "+
			"%v", len(kgtnOutputs), err)
		return
	}

	utxnLog.Infof("Graduated %d kindergarten outputs from height=%d",
		len(kgtnOutputs), classHeight)

	// Iterate over the kid outputs and construct a set of all channel
	// points to which they belong.
	var possibleCloses = make(map[wire.OutPoint]struct{})
	for _, kid := range kgtnOutputs {
		possibleCloses[*kid.OriginChanPoint()] = struct{}{}

	}

	// Attempt to close each channel, only doing so if all of the channel's
	// outputs have been graduated.
	for chanPoint := range possibleCloses {
		if err := u.closeAndRemoveIfMature(&chanPoint); err != nil {
			utxnLog.Errorf("Failed to close and remove channel %v",
				chanPoint)
			return
		}
	}
}

// sweepCribOutput broadcasts the crib output's htlc timeout txn, and sets up a
// notification that will advance it to the kindergarten bucket upon
// confirmation.
func (u *utxoNursery) sweepCribOutput(classHeight uint32, baby *babyOutput) error {
	utxnLog.Infof("Publishing CLTV-delayed HTLC output using timeout tx "+
		"(txid=%v): %v", baby.timeoutTx.TxHash(),
		newLogClosure(func() string {
			return spew.Sdump(baby.timeoutTx)
		}),
	)

	// We'll now broadcast the HTLC transaction, then wait for it to be
	// confirmed before transitioning it to kindergarten.
	err := u.cfg.PublishTransaction(baby.timeoutTx)
	if err != nil && err != lnwallet.ErrDoubleSpend {
		utxnLog.Errorf("Unable to broadcast baby tx: "+
			"%v, %v", err, spew.Sdump(baby.timeoutTx))
		return err
	}

	return u.registerTimeoutConf(baby, classHeight)
}

// registerTimeoutConf is responsible for subscribing to confirmation
// notification for an htlc timeout transaction. If successful, a goroutine
// will be spawned that will transition the provided baby output into the
// kindergarten state within the nursery store.
func (u *utxoNursery) registerTimeoutConf(baby *babyOutput, heightHint uint32) error {

	birthTxID := baby.timeoutTx.TxHash()

	// Register for the confirmation of presigned htlc txn.
	confChan, err := u.cfg.Notifier.RegisterConfirmationsNtfn(
		&birthTxID, u.cfg.ConfDepth, heightHint)
	if err != nil {
		return err
	}

	utxnLog.Infof("Htlc output %v registered for promotion "+
		"notification.", baby.OutPoint())

	u.wg.Add(1)
	go u.waitForTimeoutConf(baby, confChan)

	return nil
}

// waitForTimeoutConf watches for the confirmation of an htlc timeout
// transaction, and attempts to move the htlc output from the crib bucket to the
// kindergarten bucket upon success.
func (u *utxoNursery) waitForTimeoutConf(baby *babyOutput,
	confChan *chainntnfs.ConfirmationEvent) {

	defer u.wg.Done()

	select {
	case txConfirmation, ok := <-confChan.Confirmed:
		if !ok {
			utxnLog.Errorf("Notification chan "+
				"closed, can't advance baby output %v",
				baby.OutPoint())
			return
		}

		baby.SetConfHeight(txConfirmation.BlockHeight)

	case <-u.quit:
		return
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	// TODO(conner): add retry logic?

	err := u.cfg.Store.CribToKinder(baby)
	if err != nil {
		utxnLog.Errorf("Unable to move htlc output from "+
			"crib to kindergarten bucket: %v", err)
		return
	}

	utxnLog.Infof("Htlc output %v promoted to "+
		"kindergarten", baby.OutPoint())
}

// registerPreschoolConf is responsible for subscribing to the confirmation of
// a commitment transaction, or an htlc success transaction for an incoming
// HTLC on our commitment transaction.. If successful, the provided preschool
// output will be moved persistently into the kindergarten state within the
// nursery store.
func (u *utxoNursery) registerPreschoolConf(kid *kidOutput, heightHint uint32) error {
	txID := kid.OutPoint().Hash

	// TODO(roasbeef): ensure we don't already have one waiting, need to
	// de-duplicate
	//  * need to do above?

	confChan, err := u.cfg.Notifier.RegisterConfirmationsNtfn(&txID,
		u.cfg.ConfDepth, heightHint)
	if err != nil {
		return err
	}

	var outputType string
	if kid.isHtlc {
		outputType = "HTLC"
	} else {
		outputType = "Commitment"
	}

	utxnLog.Infof("%v outpoint %v registered for "+
		"confirmation notification.", outputType, kid.OutPoint())

	u.wg.Add(1)
	go u.waitForPreschoolConf(kid, confChan)

	return nil
}

// waitForPreschoolConf is intended to be run as a goroutine that will wait until
// a channel force close commitment transaction, or a second layer HTLC success
// transaction has been included in a confirmed block. Once the transaction has
// been confirmed (as reported by the Chain Notifier), waitForPreschoolConf
// will delete the output from the "preschool" database bucket and atomically
// add it to the "kindergarten" database bucket.  This is the second step in
// the output incubation process.
func (u *utxoNursery) waitForPreschoolConf(kid *kidOutput,
	confChan *chainntnfs.ConfirmationEvent) {

	defer u.wg.Done()

	select {
	case txConfirmation, ok := <-confChan.Confirmed:
		if !ok {
			utxnLog.Errorf("Notification chan "+
				"closed, can't advance output %v",
				kid.OutPoint())
			return
		}

		kid.SetConfHeight(txConfirmation.BlockHeight)

	case <-u.quit:
		return
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	// TODO(conner): add retry logic?

	var outputType string
	if kid.isHtlc {
		outputType = "HTLC"
	} else {
		outputType = "Commitment"
	}

	err := u.cfg.Store.PreschoolToKinder(kid)
	if err != nil {
		utxnLog.Errorf("Unable to move %v output "+
			"from preschool to kindergarten bucket: %v",
			outputType, err)
		return
	}
}

// contractMaturityReport is a report that details the maturity progress of a
// particular force closed contract.
type contractMaturityReport struct {
	// chanPoint is the channel point of the original contract that is now
	// awaiting maturity within the utxoNursery.
	chanPoint wire.OutPoint

	// limboBalance is the total number of frozen coins within this
	// contract.
	limboBalance btcutil.Amount

	// recoveredBalance is the total value that has been successfully swept
	// back to the user's wallet.
	recoveredBalance btcutil.Amount

	// localAmount is the local value of the commitment output.
	localAmount btcutil.Amount

	// confHeight is the block height that this output originally confirmed.
	confHeight uint32

	// maturityRequirement is the input age required for this output to
	// reach maturity.
	maturityRequirement uint32

	// maturityHeight is the absolute block height that this output will
	// mature at.
	maturityHeight uint32

	// htlcs records a maturity report for each htlc output in this channel.
	htlcs []htlcMaturityReport
}

// htlcMaturityReport provides a summary of a single htlc output, and is
// embedded as party of the overarching contractMaturityReport
type htlcMaturityReport struct {
	// outpoint is the final output that will be swept back to the wallet.
	outpoint wire.OutPoint

	// amount is the final value that will be swept in back to the wallet.
	amount btcutil.Amount

	// confHeight is the block height that this output originally confirmed.
	confHeight uint32

	// maturityRequirement is the input age required for this output to
	// reach maturity.
	maturityRequirement uint32

	// maturityHeight is the absolute block height that this output will
	// mature at.
	maturityHeight uint32

	// stage indicates whether the htlc is in the CLTV-timeout stage (1) or
	// the CSV-delay stage (2). A stage 1 htlc's maturity height will be set
	// to its expiry height, while a stage 2 htlc's maturity height will be
	// set to its confirmation height plus the maturity requirement.
	stage uint32
}

// AddLimboCommitment adds an incubating commitment output to maturity
// report's htlcs, and contributes its amount to the limbo balance.
func (c *contractMaturityReport) AddLimboCommitment(kid *kidOutput) {
	c.limboBalance += kid.Amount()

	c.localAmount += kid.Amount()
	c.confHeight = kid.ConfHeight()
	c.maturityRequirement = kid.BlocksToMaturity()

	// If the confirmation height is set, then this means the contract has
	// been confirmed, and we know the final maturity height.
	if kid.ConfHeight() != 0 {
		c.maturityHeight = kid.BlocksToMaturity() + kid.ConfHeight()
	}
}

// AddRecoveredCommitment adds a graduated commitment output to maturity
// report's  htlcs, and contributes its amount to the recovered balance.
func (c *contractMaturityReport) AddRecoveredCommitment(kid *kidOutput) {
	c.recoveredBalance += kid.Amount()

	c.localAmount += kid.Amount()
	c.confHeight = kid.ConfHeight()
	c.maturityRequirement = kid.BlocksToMaturity()
	c.maturityHeight = kid.BlocksToMaturity() + kid.ConfHeight()
}

// AddLimboStage1TimeoutHtlc adds an htlc crib output to the maturity report's
// htlcs, and contributes its amount to the limbo balance.
func (c *contractMaturityReport) AddLimboStage1TimeoutHtlc(baby *babyOutput) {
	c.limboBalance += baby.Amount()

	// TODO(roasbeef): bool to indicate stage 1 vs stage 2?
	c.htlcs = append(c.htlcs, htlcMaturityReport{
		outpoint:       *baby.OutPoint(),
		amount:         baby.Amount(),
		confHeight:     baby.ConfHeight(),
		maturityHeight: baby.expiry,
		stage:          1,
	})
}

// AddLimboDirectHtlc adds a direct HTLC on the commitment transaction of the
// remote party to the maturity report. This a CLTV time-locked output that
// hasn't yet expired.
func (c *contractMaturityReport) AddLimboDirectHtlc(kid *kidOutput) {
	c.limboBalance += kid.Amount()

	htlcReport := htlcMaturityReport{
		outpoint:       *kid.OutPoint(),
		amount:         kid.Amount(),
		confHeight:     kid.ConfHeight(),
		maturityHeight: kid.absoluteMaturity,
		stage:          2,
	}

	c.htlcs = append(c.htlcs, htlcReport)
}

// AddLimboStage1SuccessHtlcHtlc adds an htlc crib output to the maturity
// report's set of HTLC's. We'll use this to report any incoming HTLC sweeps
// where the second level transaction hasn't yet confirmed.
func (c *contractMaturityReport) AddLimboStage1SuccessHtlc(kid *kidOutput) {
	c.limboBalance += kid.Amount()

	c.htlcs = append(c.htlcs, htlcMaturityReport{
		outpoint:            *kid.OutPoint(),
		amount:              kid.Amount(),
		confHeight:          kid.ConfHeight(),
		maturityRequirement: kid.BlocksToMaturity(),
		stage:               1,
	})
}

// AddLimboStage2Htlc adds an htlc kindergarten output to the maturity report's
// htlcs, and contributes its amount to the limbo balance.
func (c *contractMaturityReport) AddLimboStage2Htlc(kid *kidOutput) {
	c.limboBalance += kid.Amount()

	htlcReport := htlcMaturityReport{
		outpoint:            *kid.OutPoint(),
		amount:              kid.Amount(),
		confHeight:          kid.ConfHeight(),
		maturityRequirement: kid.BlocksToMaturity(),
		stage:               2,
	}

	// If the confirmation height is set, then this means the first stage
	// has been confirmed, and we know the final maturity height of the CSV
	// delay.
	if kid.ConfHeight() != 0 {
		htlcReport.maturityHeight = kid.ConfHeight() + kid.BlocksToMaturity()
	}

	c.htlcs = append(c.htlcs, htlcReport)
}

// AddRecoveredHtlc adds a graduate output to the maturity report's htlcs, and
// contributes its amount to the recovered balance.
func (c *contractMaturityReport) AddRecoveredHtlc(kid *kidOutput) {
	c.recoveredBalance += kid.Amount()

	c.htlcs = append(c.htlcs, htlcMaturityReport{
		outpoint:            *kid.OutPoint(),
		amount:              kid.Amount(),
		confHeight:          kid.ConfHeight(),
		maturityRequirement: kid.BlocksToMaturity(),
		maturityHeight:      kid.ConfHeight() + kid.BlocksToMaturity(),
	})
}

// closeAndRemoveIfMature removes a particular channel from the channel index
// if and only if all of its outputs have been marked graduated. If the channel
// still has ungraduated outputs, the method will succeed without altering the
// database state.
func (u *utxoNursery) closeAndRemoveIfMature(chanPoint *wire.OutPoint) error {
	isMature, err := u.cfg.Store.IsMatureChannel(chanPoint)
	if err == ErrContractNotFound {
		return nil
	} else if err != nil {
		utxnLog.Errorf("Unable to determine maturity of "+
			"channel=%s", chanPoint)
		return err
	}

	// Nothing to do if we are still incubating.
	if !isMature {
		return nil
	}

	// Now that the channel is fully closed, we remove the channel from the
	// nursery store here. This preserves the invariant that we never remove
	// a channel unless it is mature, as this is the only place the utxo
	// nursery removes a channel.
	if err := u.cfg.Store.RemoveChannel(chanPoint); err != nil {
		utxnLog.Errorf("Unable to remove channel=%s from "+
			"nursery store: %v", chanPoint, err)
		return err
	}

	utxnLog.Infof("Removed channel %v from nursery store", chanPoint)

	return nil
}

// newSweepPkScript creates a new public key script which should be used to
// sweep any time-locked, or contested channel funds into the wallet.
// Specifically, the script generated is a version 0, pay-to-witness-pubkey-hash
// (p2wkh) output.
func newSweepPkScript(wallet lnwallet.WalletController) ([]byte, error) {
	sweepAddr, err := wallet.NewAddress(lnwallet.WitnessPubKey, false)
	if err != nil {
		return nil, err
	}

	return txscript.PayToAddrScript(sweepAddr)
}

// CsvSpendableOutput is a SpendableOutput that contains all of the information
// necessary to construct, sign, and sweep an output locked with a CSV delay.
type CsvSpendableOutput interface {
	SpendableOutput

	// ConfHeight returns the height at which this output was confirmed.
	// A zero value indicates that the output has not been confirmed.
	ConfHeight() uint32

	// SetConfHeight marks the height at which the output is confirmed in
	// the chain.
	SetConfHeight(height uint32)

	// BlocksToMaturity returns the relative timelock, as a number of
	// blocks, that must be built on top of the confirmation height before
	// the output can be spent.
	BlocksToMaturity() uint32

	// OriginChanPoint returns the outpoint of the channel from which this
	// output is derived.
	OriginChanPoint() *wire.OutPoint
}

// babyOutput represents a two-stage CSV locked output, and is used to track
// htlc outputs through incubation. The first stage requires broadcasting a
// presigned timeout txn that spends from the CLTV locked output on the
// commitment txn. A babyOutput is treated as a subset of CsvSpendableOutputs,
// with the additional constraint that a transaction must be broadcast before
// it can be spent. Each baby transaction embeds the kidOutput that can later
// be used to spend the CSV output contained in the timeout txn.
//
// TODO(roasbeef): re-rename to timeout tx
//  * create CltvCsvSpendableOutput
type babyOutput struct {
	// expiry is the absolute block height at which the secondLevelTx
	// should be broadcast to the network.
	//
	// NOTE: This value will be zero if this is a baby output for a prior
	// incoming HTLC.
	expiry uint32

	// timeoutTx is a fully-signed transaction that, upon confirmation,
	// transitions the htlc into the delay+claim stage.
	timeoutTx *wire.MsgTx

	// kidOutput represents the CSV output to be swept from the
	// secondLevelTx after it has been broadcast and confirmed.
	kidOutput
}

// makeBabyOutput constructs a baby output that wraps a future kidOutput. The
// provided sign descriptors and witness types will be used once the output
// reaches the delay and claim stage.
func makeBabyOutput(chanPoint *wire.OutPoint,
	htlcResolution *lnwallet.OutgoingHtlcResolution) babyOutput {

	htlcOutpoint := htlcResolution.ClaimOutpoint
	blocksToMaturity := htlcResolution.CsvDelay
	witnessType := lnwallet.HtlcOfferedTimeoutSecondLevel

	kid := makeKidOutput(
		&htlcOutpoint, chanPoint, blocksToMaturity, witnessType,
		&htlcResolution.SweepSignDesc, 0,
	)

	return babyOutput{
		kidOutput: kid,
		expiry:    htlcResolution.Expiry,
		timeoutTx: htlcResolution.SignedTimeoutTx,
	}
}

// Encode writes the baby output to the given io.Writer.
func (bo *babyOutput) Encode(w io.Writer) error {
	var scratch [4]byte
	byteOrder.PutUint32(scratch[:], bo.expiry)
	if _, err := w.Write(scratch[:]); err != nil {
		return err
	}

	if err := bo.timeoutTx.Serialize(w); err != nil {
		return err
	}

	return bo.kidOutput.Encode(w)
}

// Decode reconstructs a baby output using the provided io.Reader.
func (bo *babyOutput) Decode(r io.Reader) error {
	var scratch [4]byte
	if _, err := r.Read(scratch[:]); err != nil {
		return err
	}
	bo.expiry = byteOrder.Uint32(scratch[:])

	bo.timeoutTx = new(wire.MsgTx)
	if err := bo.timeoutTx.Deserialize(r); err != nil {
		return err
	}

	return bo.kidOutput.Decode(r)
}

// kidOutput represents an output that's waiting for a required blockheight
// before its funds will be available to be moved into the user's wallet.  The
// struct includes a WitnessGenerator closure which will be used to generate
// the witness required to sweep the output once it's mature.
//
// TODO(roasbeef): rename to immatureOutput?
type kidOutput struct {
	breachedOutput

	originChanPoint wire.OutPoint

	// isHtlc denotes if this kid output is an HTLC output or not. This
	// value will be used to determine how to report this output within the
	// nursery report.
	isHtlc bool

	// blocksToMaturity is the relative CSV delay required after initial
	// confirmation of the commitment transaction before we can sweep this
	// output.
	//
	// NOTE: This will be set for: commitment outputs, and incoming HTLC's.
	// Otherwise, this will be zero.
	blocksToMaturity uint32

	// absoluteMaturity is the absolute height that this output will be
	// mature at. In order to sweep the output after this height, the
	// locktime of sweep transaction will need to be set to this value.
	//
	// NOTE: This will only be set for: outgoing HTLC's on the commitment
	// transaction of the remote party.
	absoluteMaturity uint32

	confHeight uint32
}

func makeKidOutput(outpoint, originChanPoint *wire.OutPoint,
	blocksToMaturity uint32, witnessType lnwallet.WitnessType,
	signDescriptor *lnwallet.SignDescriptor,
	absoluteMaturity uint32) kidOutput {

	// This is an HTLC either if it's an incoming HTLC on our commitment
	// transaction, or is an outgoing HTLC on the commitment transaction of
	// the remote peer.
	isHtlc := (witnessType == lnwallet.HtlcAcceptedSuccessSecondLevel ||
		witnessType == lnwallet.HtlcOfferedRemoteTimeout)

	return kidOutput{
		breachedOutput: makeBreachedOutput(
			outpoint, witnessType, nil, signDescriptor,
		),
		isHtlc:           isHtlc,
		originChanPoint:  *originChanPoint,
		blocksToMaturity: blocksToMaturity,
		absoluteMaturity: absoluteMaturity,
	}
}

func (k *kidOutput) OriginChanPoint() *wire.OutPoint {
	return &k.originChanPoint
}

func (k *kidOutput) BlocksToMaturity() uint32 {
	return k.blocksToMaturity
}

func (k *kidOutput) SetConfHeight(height uint32) {
	k.confHeight = height
}

func (k *kidOutput) ConfHeight() uint32 {
	return k.confHeight
}

// Encode converts a KidOutput struct into a form suitable for on-disk database
// storage. Note that the signDescriptor struct field is included so that the
// output's witness can be generated by createSweepTx() when the output becomes
// spendable.
func (k *kidOutput) Encode(w io.Writer) error {
	var scratch [8]byte
	byteOrder.PutUint64(scratch[:], uint64(k.Amount()))
	if _, err := w.Write(scratch[:]); err != nil {
		return err
	}

	if err := writeOutpoint(w, k.OutPoint()); err != nil {
		return err
	}
	if err := writeOutpoint(w, k.OriginChanPoint()); err != nil {
		return err
	}

	if err := binary.Write(w, byteOrder, k.isHtlc); err != nil {
		return err
	}

	byteOrder.PutUint32(scratch[:4], k.BlocksToMaturity())
	if _, err := w.Write(scratch[:4]); err != nil {
		return err
	}

	byteOrder.PutUint32(scratch[:4], k.absoluteMaturity)
	if _, err := w.Write(scratch[:4]); err != nil {
		return err
	}

	byteOrder.PutUint32(scratch[:4], k.ConfHeight())
	if _, err := w.Write(scratch[:4]); err != nil {
		return err
	}

	byteOrder.PutUint16(scratch[:2], uint16(k.WitnessType()))
	if _, err := w.Write(scratch[:2]); err != nil {
		return err
	}

	return lnwallet.WriteSignDescriptor(w, k.SignDesc())
}

// Decode takes a byte array representation of a kidOutput and converts it to an
// struct. Note that the witnessFunc method isn't added during deserialization
// and must be added later based on the value of the witnessType field.
func (k *kidOutput) Decode(r io.Reader) error {
	var scratch [8]byte

	if _, err := r.Read(scratch[:]); err != nil {
		return err
	}
	k.amt = btcutil.Amount(byteOrder.Uint64(scratch[:]))

	if err := readOutpoint(io.LimitReader(r, 40), &k.outpoint); err != nil {
		return err
	}

	err := readOutpoint(io.LimitReader(r, 40), &k.originChanPoint)
	if err != nil {
		return err
	}

	if err := binary.Read(r, byteOrder, &k.isHtlc); err != nil {
		return err
	}

	if _, err := r.Read(scratch[:4]); err != nil {
		return err
	}
	k.blocksToMaturity = byteOrder.Uint32(scratch[:4])

	if _, err := r.Read(scratch[:4]); err != nil {
		return err
	}
	k.absoluteMaturity = byteOrder.Uint32(scratch[:4])

	if _, err := r.Read(scratch[:4]); err != nil {
		return err
	}
	k.confHeight = byteOrder.Uint32(scratch[:4])

	if _, err := r.Read(scratch[:2]); err != nil {
		return err
	}
	k.witnessType = lnwallet.WitnessType(byteOrder.Uint16(scratch[:2]))

	return lnwallet.ReadSignDescriptor(r, &k.signDesc)
}

// TODO(bvu): copied from channeldb, remove repetition
func writeOutpoint(w io.Writer, o *wire.OutPoint) error {
	// TODO(roasbeef): make all scratch buffers on the stack
	scratch := make([]byte, 4)

	// TODO(roasbeef): write raw 32 bytes instead of wasting the extra
	// byte.
	if err := wire.WriteVarBytes(w, 0, o.Hash[:]); err != nil {
		return err
	}

	byteOrder.PutUint32(scratch, o.Index)
	_, err := w.Write(scratch)
	return err
}

// TODO(bvu): copied from channeldb, remove repetition
func readOutpoint(r io.Reader, o *wire.OutPoint) error {
	scratch := make([]byte, 4)

	txid, err := wire.ReadVarBytes(r, 0, 32, "prevout")
	if err != nil {
		return err
	}
	copy(o.Hash[:], txid)

	if _, err := r.Read(scratch); err != nil {
		return err
	}
	o.Index = byteOrder.Uint32(scratch)

	return nil
}

func writeTxOut(w io.Writer, txo *wire.TxOut) error {
	scratch := make([]byte, 8)

	byteOrder.PutUint64(scratch, uint64(txo.Value))
	if _, err := w.Write(scratch); err != nil {
		return err
	}

	if err := wire.WriteVarBytes(w, 0, txo.PkScript); err != nil {
		return err
	}

	return nil
}

func readTxOut(r io.Reader, txo *wire.TxOut) error {
	scratch := make([]byte, 8)

	if _, err := r.Read(scratch); err != nil {
		return err
	}
	txo.Value = int64(byteOrder.Uint64(scratch))

	pkScript, err := wire.ReadVarBytes(r, 0, 80, "pkScript")
	if err != nil {
		return err
	}
	txo.PkScript = pkScript

	return nil
}

// Compile-time constraint to ensure kidOutput and babyOutput implement the
// CsvSpendableOutput interface.
var _ CsvSpendableOutput = (*kidOutput)(nil)
var _ CsvSpendableOutput = (*babyOutput)(nil)
