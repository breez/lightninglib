package lnwallet

import (
	"net"
	"sync"

	"github.com/breez/lightninglib/channeldb"
	"github.com/breez/lightninglib/lnwire"
	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
)

// ChannelContribution is the primary constituent of the funding workflow
// within lnwallet. Each side first exchanges their respective contributions
// along with channel specific parameters like the min fee/KB. Once
// contributions have been exchanged, each side will then produce signatures
// for all their inputs to the funding transactions, and finally a signature
// for the other party's version of the commitment transaction.
type ChannelContribution struct {
	// FundingOutpoint is the amount of funds contributed to the funding
	// transaction.
	FundingAmount btcutil.Amount

	// Inputs to the funding transaction.
	Inputs []*wire.TxIn

	// ChangeOutputs are the Outputs to be used in the case that the total
	// value of the funding inputs is greater than the total potential
	// channel capacity.
	ChangeOutputs []*wire.TxOut

	// FirstCommitmentPoint is the first commitment point that will be used
	// to create the revocation key in the first commitment transaction we
	// send to the remote party.
	FirstCommitmentPoint *btcec.PublicKey

	// ChannelConfig is the concrete contribution that this node is
	// offering to the channel. This includes all the various constraints
	// such as the min HTLC, and also all the keys which will be used for
	// the duration of the channel.
	*channeldb.ChannelConfig
}

// toChanConfig returns the raw channel configuration generated by a node's
// contribution to the channel.
func (c *ChannelContribution) toChanConfig() channeldb.ChannelConfig {
	return *c.ChannelConfig
}

// InputScript represents any script inputs required to redeem a previous
// output. This struct is used rather than just a witness, or scripSig in
// order to accommodate nested p2sh which utilizes both types of input scripts.
type InputScript struct {
	Witness   [][]byte
	ScriptSig []byte
}

// ChannelReservation represents an intent to open a lightning payment channel
// with a counterparty. The funding processes from reservation to channel opening
// is a 3-step process. In order to allow for full concurrency during the
// reservation workflow, resources consumed by a contribution are "locked"
// themselves. This prevents a number of race conditions such as two funding
// transactions double-spending the same input. A reservation can also be
// cancelled, which removes the resources from limbo, allowing another
// reservation to claim them.
//
// The reservation workflow consists of the following three steps:
//  1. lnwallet.InitChannelReservation
//     * One requests the wallet to allocate the necessary resources for a
//       channel reservation. These resources are put in limbo for the lifetime
//       of a reservation.
//     * Once completed the reservation will have the wallet's contribution
//       accessible via the .OurContribution() method. This contribution
//       contains the necessary items to allow the remote party to build both
//       the funding, and commitment transactions.
//  2. ChannelReservation.ProcessContribution/ChannelReservation.ProcessSingleContribution
//     * The counterparty presents their contribution to the payment channel.
//       This allows us to build the funding, and commitment transactions
//       ourselves.
//     * We're now able to sign our inputs to the funding transactions, and
//       the counterparty's version of the commitment transaction.
//     * All signatures crafted by us, are now available via .OurSignatures().
//  3. ChannelReservation.CompleteReservation/ChannelReservation.CompleteReservationSingle
//     * The final step in the workflow. The counterparty presents the
//       signatures for all their inputs to the funding transaction, as well
//       as a signature to our version of the commitment transaction.
//     * We then verify the validity of all signatures before considering the
//       channel "open".
type ChannelReservation struct {
	// This mutex MUST be held when either reading or modifying any of the
	// fields below.
	sync.RWMutex

	// fundingTx is the funding transaction for this pending channel.
	fundingTx *wire.MsgTx

	// In order of sorted inputs. Sorting is done in accordance
	// to BIP-69: https://github.com/bitcoin/bips/blob/master/bip-0069.mediawiki.
	ourFundingInputScripts   []*InputScript
	theirFundingInputScripts []*InputScript

	// Our signature for their version of the commitment transaction.
	ourCommitmentSig   []byte
	theirCommitmentSig []byte

	ourContribution   *ChannelContribution
	theirContribution *ChannelContribution

	partialState *channeldb.OpenChannel
	nodeAddr     net.Addr

	// The ID of this reservation, used to uniquely track the reservation
	// throughout its lifetime.
	reservationID uint64

	// pushMSat the amount of milli-satoshis that should be pushed to the
	// responder of a single funding channel as part of the initial
	// commitment state.
	pushMSat lnwire.MilliSatoshi

	// chanOpen houses a struct containing the channel and additional
	// confirmation details will be sent on once the channel is considered
	// 'open'. A channel is open once the funding transaction has reached a
	// sufficient number of confirmations.
	chanOpen    chan *openChanDetails
	chanOpenErr chan error

	wallet *LightningWallet
}

// NewChannelReservation creates a new channel reservation. This function is
// used only internally by lnwallet. In order to concurrent safety, the
// creation of all channel reservations should be carried out via the
// lnwallet.InitChannelReservation interface.
func NewChannelReservation(capacity, fundingAmt btcutil.Amount,
	commitFeePerKw SatPerKWeight, wallet *LightningWallet,
	id uint64, pushMSat lnwire.MilliSatoshi, chainHash *chainhash.Hash,
	flags lnwire.FundingFlag) (*ChannelReservation, error) {

	var (
		ourBalance   lnwire.MilliSatoshi
		theirBalance lnwire.MilliSatoshi
		initiator    bool
	)

	commitFee := commitFeePerKw.FeeForWeight(CommitWeight)
	fundingMSat := lnwire.NewMSatFromSatoshis(fundingAmt)
	capacityMSat := lnwire.NewMSatFromSatoshis(capacity)
	feeMSat := lnwire.NewMSatFromSatoshis(commitFee)

	// If we're the responder to a single-funder reservation, then we have
	// no initial balance in the channel unless the remote party is pushing
	// some funds to us within the first commitment state.
	if fundingAmt == 0 {
		ourBalance = pushMSat
		theirBalance = capacityMSat - feeMSat - pushMSat
		initiator = false

		// If the responder doesn't have enough funds to actually pay
		// the fees, then we'll bail our early.
		if int64(theirBalance) < 0 {
			return nil, ErrFunderBalanceDust(
				int64(commitFee), int64(theirBalance.ToSatoshis()),
				int64(2*DefaultDustLimit()),
			)
		}
	} else {
		// TODO(roasbeef): need to rework fee structure in general and
		// also when we "unlock" dual funder within the daemon

		if capacity == fundingAmt {
			// If we're initiating a single funder workflow, then
			// we pay all the initial fees within the commitment
			// transaction. We also deduct our balance by the
			// amount pushed as part of the initial state.
			ourBalance = capacityMSat - feeMSat - pushMSat
			theirBalance = pushMSat
		} else {
			// Otherwise, this is a dual funder workflow where both
			// slides split the amount funded and the commitment
			// fee.
			ourBalance = fundingMSat - (feeMSat / 2)
			theirBalance = capacityMSat - fundingMSat - (feeMSat / 2) + pushMSat
		}

		initiator = true

		// If we, the initiator don't have enough funds to actually pay
		// the fees, then we'll exit with an error.
		if int64(ourBalance) < 0 {
			return nil, ErrFunderBalanceDust(
				int64(commitFee), int64(ourBalance),
				int64(2*DefaultDustLimit()),
			)
		}
	}

	// If we're the initiator and our starting balance within the channel
	// after we take account of fees is below 2x the dust limit, then we'll
	// reject this channel creation request.
	//
	// TODO(roasbeef): reject if 30% goes to fees? dust channel
	if initiator && ourBalance.ToSatoshis() <= 2*DefaultDustLimit() {
		return nil, ErrFunderBalanceDust(
			int64(commitFee),
			int64(ourBalance.ToSatoshis()),
			int64(2*DefaultDustLimit()),
		)
	}

	// Next we'll set the channel type based on what we can ascertain about
	// the balances/push amount within the channel.
	var chanType channeldb.ChannelType

	// If either of the balances are zero at this point, or we have a
	// non-zero push amt (there's no pushing for dual funder), then this is
	// a single-funder channel.
	if ourBalance == 0 || theirBalance == 0 || pushMSat != 0 {
		chanType = channeldb.SingleFunder
	} else {
		// Otherwise, this is a dual funder channel, and no side is
		// technically the "initiator"
		initiator = false
		chanType = channeldb.DualFunder
	}

	return &ChannelReservation{
		ourContribution: &ChannelContribution{
			FundingAmount: ourBalance.ToSatoshis(),
			ChannelConfig: &channeldb.ChannelConfig{},
		},
		theirContribution: &ChannelContribution{
			FundingAmount: theirBalance.ToSatoshis(),
			ChannelConfig: &channeldb.ChannelConfig{},
		},
		partialState: &channeldb.OpenChannel{
			ChanType:     chanType,
			ChainHash:    *chainHash,
			IsPending:    true,
			IsInitiator:  initiator,
			ChannelFlags: flags,
			Capacity:     capacity,
			LocalCommitment: channeldb.ChannelCommitment{
				LocalBalance:  ourBalance,
				RemoteBalance: theirBalance,
				FeePerKw:      btcutil.Amount(commitFeePerKw),
				CommitFee:     commitFee,
			},
			RemoteCommitment: channeldb.ChannelCommitment{
				LocalBalance:  ourBalance,
				RemoteBalance: theirBalance,
				FeePerKw:      btcutil.Amount(commitFeePerKw),
				CommitFee:     commitFee,
			},
			Db: wallet.Cfg.Database,
		},
		pushMSat:      pushMSat,
		reservationID: id,
		chanOpen:      make(chan *openChanDetails, 1),
		chanOpenErr:   make(chan error, 1),
		wallet:        wallet,
	}, nil
}

// SetNumConfsRequired sets the number of confirmations that are required for
// the ultimate funding transaction before the channel can be considered open.
// This is distinct from the main reservation workflow as it allows
// implementations a bit more flexibility w.r.t to if the responder of the
// initiator sets decides the number of confirmations needed.
func (r *ChannelReservation) SetNumConfsRequired(numConfs uint16) {
	r.Lock()
	defer r.Unlock()

	r.partialState.NumConfsRequired = numConfs
}

// CommitConstraints takes the constraints that the remote party specifies for
// the type of commitments that we can generate for them. These constraints
// include several parameters that serve as flow control restricting the amount
// of satoshis that can be transferred in a single commitment. This function
// will also attempt to verify the constraints for sanity, returning an error
// if the parameters are seemed unsound.
func (r *ChannelReservation) CommitConstraints(c *channeldb.ChannelConstraints) error {
	r.Lock()
	defer r.Unlock()

	// Fail if we consider csvDelay excessively large.
	// TODO(halseth): find a more scientific choice of value.
	const maxDelay = 10000
	if c.CsvDelay > maxDelay {
		return ErrCsvDelayTooLarge(c.CsvDelay, maxDelay)
	}

	// The dust limit should always be greater or equal to the channel
	// reserve. The reservation request should be denied if otherwise.
	if c.DustLimit > c.ChanReserve {
		return ErrChanReserveTooSmall(c.ChanReserve, c.DustLimit)
	}

	// Fail if we consider the channel reserve to be too large.  We
	// currently fail if it is greater than 20% of the channel capacity.
	maxChanReserve := r.partialState.Capacity / 5
	if c.ChanReserve > maxChanReserve {
		return ErrChanReserveTooLarge(c.ChanReserve, maxChanReserve)
	}

	// Fail if the minimum HTLC value is too large. If this is too large,
	// the channel won't be useful for sending small payments. This limit
	// is currently set to maxValueInFlight, effectively letting the remote
	// setting this as large as it wants.
	if c.MinHTLC > c.MaxPendingAmount {
		return ErrMinHtlcTooLarge(c.MinHTLC, c.MaxPendingAmount)
	}

	// Fail if maxHtlcs is above the maximum allowed number of 483.  This
	// number is specified in BOLT-02.
	if c.MaxAcceptedHtlcs > uint16(MaxHTLCNumber/2) {
		return ErrMaxHtlcNumTooLarge(
			c.MaxAcceptedHtlcs, uint16(MaxHTLCNumber/2),
		)
	}

	// Fail if we consider maxHtlcs too small. If this is too small we
	// cannot offer many HTLCs to the remote.
	const minNumHtlc = 5
	if c.MaxAcceptedHtlcs < minNumHtlc {
		return ErrMaxHtlcNumTooSmall(c.MaxAcceptedHtlcs, minNumHtlc)
	}

	// Fail if we consider maxValueInFlight too small. We currently require
	// the remote to at least allow minNumHtlc * minHtlc in flight.
	if c.MaxPendingAmount < minNumHtlc*c.MinHTLC {
		return ErrMaxValueInFlightTooSmall(
			c.MaxPendingAmount, minNumHtlc*c.MinHTLC,
		)
	}

	// Our dust limit should always be less than or equal to our proposed
	// channel reserve.
	if r.ourContribution.DustLimit > c.ChanReserve {
		r.ourContribution.DustLimit = c.ChanReserve
	}

	r.ourContribution.ChanReserve = c.ChanReserve
	r.ourContribution.MaxPendingAmount = c.MaxPendingAmount
	r.ourContribution.MinHTLC = c.MinHTLC
	r.ourContribution.MaxAcceptedHtlcs = c.MaxAcceptedHtlcs
	r.ourContribution.CsvDelay = c.CsvDelay

	return nil
}

// OurContribution returns the wallet's fully populated contribution to the
// pending payment channel. See 'ChannelContribution' for further details
// regarding the contents of a contribution.
//
// NOTE: This SHOULD NOT be modified.
// TODO(roasbeef): make copy?
func (r *ChannelReservation) OurContribution() *ChannelContribution {
	r.RLock()
	defer r.RUnlock()

	return r.ourContribution
}

// ProcessContribution verifies the counterparty's contribution to the pending
// payment channel. As a result of this incoming message, lnwallet is able to
// build the funding transaction, and both commitment transactions. Once this
// message has been processed, all signatures to inputs to the funding
// transaction belonging to the wallet are available. Additionally, the wallet
// will generate a signature to the counterparty's version of the commitment
// transaction.
func (r *ChannelReservation) ProcessContribution(theirContribution *ChannelContribution) error {
	errChan := make(chan error, 1)

	r.wallet.msgChan <- &addContributionMsg{
		pendingFundingID: r.reservationID,
		contribution:     theirContribution,
		err:              errChan,
	}

	return <-errChan
}

// ProcessSingleContribution verifies, and records the initiator's contribution
// to this pending single funder channel. Internally, no further action is
// taken other than recording the initiator's contribution to the single funder
// channel.
func (r *ChannelReservation) ProcessSingleContribution(theirContribution *ChannelContribution) error {
	errChan := make(chan error, 1)

	r.wallet.msgChan <- &addSingleContributionMsg{
		pendingFundingID: r.reservationID,
		contribution:     theirContribution,
		err:              errChan,
	}

	return <-errChan
}

// TheirContribution returns the counterparty's pending contribution to the
// payment channel. See 'ChannelContribution' for further details regarding the
// contents of a contribution. This attribute will ONLY be available after a
// call to .ProcessContribution().
//
// NOTE: This SHOULD NOT be modified.
func (r *ChannelReservation) TheirContribution() *ChannelContribution {
	r.RLock()
	defer r.RUnlock()
	return r.theirContribution
}

// OurSignatures retrieves the wallet's signatures to all inputs to the funding
// transaction belonging to itself, and also a signature for the counterparty's
// version of the commitment transaction. The signatures for the wallet's
// inputs to the funding transaction are returned in sorted order according to
// BIP-69: https://github.com/bitcoin/bips/blob/master/bip-0069.mediawiki.
//
// NOTE: These signatures will only be populated after a call to
// .ProcessContribution()
func (r *ChannelReservation) OurSignatures() ([]*InputScript, []byte) {
	r.RLock()
	defer r.RUnlock()
	return r.ourFundingInputScripts, r.ourCommitmentSig
}

// CompleteReservation finalizes the pending channel reservation, transitioning
// from a pending payment channel, to an open payment channel. All passed
// signatures to the counterparty's inputs to the funding transaction will be
// fully verified. Signatures are expected to be passed in sorted order
// according to BIP-69:
// https://github.com/bitcoin/bips/blob/master/bip-0069.mediawiki.
// Additionally, verification is performed in order to ensure that the
// counterparty supplied a valid signature to our version of the commitment
// transaction.  Once this method returns, caller's should broadcast the
// created funding transaction, then call .WaitForChannelOpen() which will
// block until the funding transaction obtains the configured number of
// confirmations. Once the method unblocks, a LightningChannel instance is
// returned, marking the channel available for updates.
func (r *ChannelReservation) CompleteReservation(fundingInputScripts []*InputScript,
	commitmentSig []byte) (*channeldb.OpenChannel, error) {

	// TODO(roasbeef): add flag for watch or not?
	errChan := make(chan error, 1)
	completeChan := make(chan *channeldb.OpenChannel, 1)

	r.wallet.msgChan <- &addCounterPartySigsMsg{
		pendingFundingID:         r.reservationID,
		theirFundingInputScripts: fundingInputScripts,
		theirCommitmentSig:       commitmentSig,
		completeChan:             completeChan,
		err:                      errChan,
	}

	return <-completeChan, <-errChan
}

// CompleteReservationSingle finalizes the pending single funder channel
// reservation. Using the funding outpoint of the constructed funding
// transaction, and the initiator's signature for our version of the commitment
// transaction, we are able to verify the correctness of our commitment
// transaction as crafted by the initiator. Once this method returns, our
// signature for the initiator's version of the commitment transaction is
// available via the .OurSignatures() method. As this method should only be
// called as a response to a single funder channel, only a commitment signature
// will be populated.
func (r *ChannelReservation) CompleteReservationSingle(fundingPoint *wire.OutPoint,
	commitSig []byte) (*channeldb.OpenChannel, error) {

	errChan := make(chan error, 1)
	completeChan := make(chan *channeldb.OpenChannel, 1)

	r.wallet.msgChan <- &addSingleFunderSigsMsg{
		pendingFundingID:   r.reservationID,
		fundingOutpoint:    fundingPoint,
		theirCommitmentSig: commitSig,
		completeChan:       completeChan,
		err:                errChan,
	}

	return <-completeChan, <-errChan
}

// TheirSignatures returns the counterparty's signatures to all inputs to the
// funding transaction belonging to them, as well as their signature for the
// wallet's version of the commitment transaction. This methods is provided for
// additional verification, such as needed by tests.
//
// NOTE: These attributes will be unpopulated before a call to
// .CompleteReservation().
func (r *ChannelReservation) TheirSignatures() ([]*InputScript, []byte) {
	r.RLock()
	defer r.RUnlock()
	return r.theirFundingInputScripts, r.theirCommitmentSig
}

// FinalFundingTx returns the finalized, fully signed funding transaction for
// this reservation.
//
// NOTE: If this reservation was created as the non-initiator to a single
// funding workflow, then the full funding transaction will not be available.
// Instead we will only have the final outpoint of the funding transaction.
func (r *ChannelReservation) FinalFundingTx() *wire.MsgTx {
	r.RLock()
	defer r.RUnlock()
	return r.fundingTx
}

// FundingOutpoint returns the outpoint of the funding transaction.
//
// NOTE: The pointer returned will only be set once the .ProcessContribution()
// method is called in the case of the initiator of a single funder workflow,
// and after the .CompleteReservationSingle() method is called in the case of
// a responder to a single funder workflow.
func (r *ChannelReservation) FundingOutpoint() *wire.OutPoint {
	r.RLock()
	defer r.RUnlock()
	return &r.partialState.FundingOutpoint
}

// Cancel abandons this channel reservation. This method should be called in
// the scenario that communications with the counterparty break down. Upon
// cancellation, all resources previously reserved for this pending payment
// channel are returned to the free pool, allowing subsequent reservations to
// utilize the now freed resources.
func (r *ChannelReservation) Cancel() error {
	errChan := make(chan error, 1)
	r.wallet.msgChan <- &fundingReserveCancelMsg{
		pendingFundingID: r.reservationID,
		err:              errChan,
	}

	return <-errChan
}

// OpenChannelDetails wraps the finalized fully confirmed channel which
// resulted from a ChannelReservation instance with details concerning exactly
// _where_ in the chain the channel was ultimately opened.
type OpenChannelDetails struct {
	// Channel is the active channel created by an instance of a
	// ChannelReservation and the required funding workflow.
	Channel *LightningChannel

	// ConfirmationHeight is the block height within the chain that
	// included the channel.
	ConfirmationHeight uint32

	// TransactionIndex is the index within the confirming block that the
	// transaction resides.
	TransactionIndex uint32
}
