package contractcourt

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/breez/lightninglib/chainntnfs"
	"github.com/breez/lightninglib/input"
	"github.com/breez/lightninglib/lntypes"
	"github.com/breez/lightninglib/lnwallet"
	"github.com/breez/lightninglib/lnwire"
	"github.com/btcsuite/btcd/wire"
	"github.com/davecgh/go-spew/spew"
)

// htlcTimeoutResolver is a ContractResolver that's capable of resolving an
// outgoing HTLC. The HTLC may be on our commitment transaction, or on the
// commitment transaction of the remote party. An output on our commitment
// transaction is considered fully resolved once the second-level transaction
// has been confirmed (and reached a sufficient depth). An output on the
// commitment transaction of the remote party is resolved once we detect a
// spend of the direct HTLC output using the timeout clause.
type htlcTimeoutResolver struct {
	// htlcResolution contains all the information required to properly
	// resolve this outgoing HTLC.
	htlcResolution lnwallet.OutgoingHtlcResolution

	// outputIncubating returns true if we've sent the output to the output
	// incubator (utxo nursery).
	outputIncubating bool

	// resolved reflects if the contract has been fully resolved or not.
	resolved bool

	// broadcastHeight is the height that the original contract was
	// broadcast to the main-chain at. We'll use this value to bound any
	// historical queries to the chain for spends/confirmations.
	//
	// TODO(roasbeef): wrap above into definite resolution embedding?
	broadcastHeight uint32

	// htlcIndex is the index of this HTLC within the trace of the
	// additional commitment state machine.
	htlcIndex uint64

	// htlcAmt is the original amount of the htlc, not taking into
	// account any fees that may have to be paid if it goes on chain.
	htlcAmt lnwire.MilliSatoshi

	ResolverKit
}

// ResolverKey returns an identifier which should be globally unique for this
// particular resolver within the chain the original contract resides within.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) ResolverKey() []byte {
	// The primary key for this resolver will be the outpoint of the HTLC
	// on the commitment transaction itself. If this is our commitment,
	// then the output can be found within the signed timeout tx,
	// otherwise, it's just the ClaimOutpoint.
	var op wire.OutPoint
	if h.htlcResolution.SignedTimeoutTx != nil {
		op = h.htlcResolution.SignedTimeoutTx.TxIn[0].PreviousOutPoint
	} else {
		op = h.htlcResolution.ClaimOutpoint
	}

	key := newResolverID(op)
	return key[:]
}

const (
	// expectedRemoteWitnessSuccessSize is the expected size of the witness
	// on the remote commitment transaction for an outgoing HTLC that is
	// swept on-chain by them with pre-image.
	expectedRemoteWitnessSuccessSize = 5

	// remotePreimageIndex index within the witness on the remote
	// commitment transaction that will hold they pre-image if they go to
	// sweep it on chain.
	remotePreimageIndex = 3

	// localPreimageIndex is the index within the witness on the local
	// commitment transaction for an outgoing HTLC that will hold the
	// pre-image if the remote party sweeps it.
	localPreimageIndex = 1
)

// claimCleanUp is a helper method that's called once the HTLC output is spent
// by the remote party. It'll extract the preimage, add it to the global cache,
// and finally send the appropriate clean up message.
func (h *htlcTimeoutResolver) claimCleanUp(
	commitSpend *chainntnfs.SpendDetail) (ContractResolver, error) {

	// Depending on if this is our commitment or not, then we'll be looking
	// for a different witness pattern.
	spenderIndex := commitSpend.SpenderInputIndex
	spendingInput := commitSpend.SpendingTx.TxIn[spenderIndex]

	log.Infof("%T(%v): extracting preimage! remote party spent "+
		"HTLC with tx=%v", h, h.htlcResolution.ClaimOutpoint,
		spew.Sdump(commitSpend.SpendingTx))

	// If this is the remote party's commitment, then we'll be looking for
	// them to spend using the second-level success transaction.
	var preimageBytes []byte
	if h.htlcResolution.SignedTimeoutTx == nil {
		// The witness stack when the remote party sweeps the output to
		// them looks like:
		//
		//  * <0> <sender sig> <recvr sig> <preimage> <witness script>
		preimageBytes = spendingInput.Witness[remotePreimageIndex]
	} else {
		// Otherwise, they'll be spending directly from our commitment
		// output. In which case the witness stack looks like:
		//
		//  * <sig> <preimage> <witness script>
		preimageBytes = spendingInput.Witness[localPreimageIndex]
	}

	preimage, err := lntypes.MakePreimage(preimageBytes)
	if err != nil {
		return nil, fmt.Errorf("unable to create pre-image from "+
			"witness: %v", err)
	}

	log.Infof("%T(%v): extracting preimage=%v from on-chain "+
		"spend!", h, h.htlcResolution.ClaimOutpoint, preimage)

	// With the preimage obtained, we can now add it to the global cache.
	if err := h.PreimageDB.AddPreimages(preimage); err != nil {
		log.Errorf("%T(%v): unable to add witness to cache",
			h, h.htlcResolution.ClaimOutpoint)
	}

	var pre [32]byte
	copy(pre[:], preimage[:])

	// Finally, we'll send the clean up message, mark ourselves as
	// resolved, then exit.
	if err := h.DeliverResolutionMsg(ResolutionMsg{
		SourceChan: h.ShortChanID,
		HtlcIndex:  h.htlcIndex,
		PreImage:   &pre,
	}); err != nil {
		return nil, err
	}
	h.resolved = true
	return nil, h.Checkpoint(h)
}

// chainDetailsToWatch returns the output and script which we use to watch for
// spends from the direct HTLC output on the commitment transaction.
//
// TODO(joostjager): output already set properly in
// lnwallet.newOutgoingHtlcResolution? And script too?
func (h *htlcTimeoutResolver) chainDetailsToWatch() (*wire.OutPoint, []byte, error) {
	// If there's no timeout transaction, then the claim output is the
	// output directly on the commitment transaction, so we'll just use
	// that.
	if h.htlcResolution.SignedTimeoutTx == nil {
		outPointToWatch := h.htlcResolution.ClaimOutpoint
		scriptToWatch := h.htlcResolution.SweepSignDesc.Output.PkScript

		return &outPointToWatch, scriptToWatch, nil
	}

	// If this is the remote party's commitment, then we'll need to grab
	// watch the output that our timeout transaction points to. We can
	// directly grab the outpoint, then also extract the witness script
	// (the last element of the witness stack) to re-construct the pkScript
	// we need to watch.
	outPointToWatch := h.htlcResolution.SignedTimeoutTx.TxIn[0].PreviousOutPoint
	witness := h.htlcResolution.SignedTimeoutTx.TxIn[0].Witness
	scriptToWatch, err := input.WitnessScriptHash(witness[len(witness)-1])
	if err != nil {
		return nil, nil, err
	}

	return &outPointToWatch, scriptToWatch, nil
}

// isSuccessSpend returns true if the passed spend on the specified commitment
// is a success spend that reveals the pre-image or not.
func isSuccessSpend(spend *chainntnfs.SpendDetail, localCommit bool) bool {
	// Based on the spending input index and transaction, obtain the
	// witness that tells us what type of spend this is.
	spenderIndex := spend.SpenderInputIndex
	spendingInput := spend.SpendingTx.TxIn[spenderIndex]
	spendingWitness := spendingInput.Witness

	// If this is the remote commitment then the only possible spends for
	// outgoing HTLCs are:
	//
	//  RECVR: <0> <sender sig> <recvr sig> <preimage> (2nd level success spend)
	//  REVOK: <sig> <key>
	//  SENDR: <sig> 0
	//
	// In this case, if 5 witness elements are present (factoring the
	// witness script), and the 3rd element is the size of the pre-image,
	// then this is a remote spend. If not, then we swept it ourselves, or
	// revoked their output.
	if !localCommit {
		return len(spendingWitness) == expectedRemoteWitnessSuccessSize &&
			len(spendingWitness[remotePreimageIndex]) == lntypes.HashSize
	}

	// Otherwise, for our commitment, the only possible spends for an
	// outgoing HTLC are:
	//
	//  SENDR: <0> <sendr sig>  <recvr sig> <0> (2nd level timeout)
	//  RECVR: <recvr sig>  <preimage>
	//  REVOK: <revoke sig> <revoke key>
	//
	// So the only success case has the pre-image as the 2nd (index 1)
	// element in the witness.
	return len(spendingWitness[localPreimageIndex]) == lntypes.HashSize
}

// Resolve kicks off full resolution of an outgoing HTLC output. If it's our
// commitment, it isn't resolved until we see the second level HTLC txn
// confirmed. If it's the remote party's commitment, we don't resolve until we
// see a direct sweep via the timeout clause.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) Resolve() (ContractResolver, error) {
	// If we're already resolved, then we can exit early.
	if h.resolved {
		return nil, nil
	}

	// If we haven't already sent the output to the utxo nursery, then
	// we'll do so now.
	if !h.outputIncubating {
		log.Tracef("%T(%v): incubating htlc output", h,
			h.htlcResolution.ClaimOutpoint)

		err := h.IncubateOutputs(
			h.ChanPoint, nil, &h.htlcResolution, nil,
			h.broadcastHeight,
		)
		if err != nil {
			return nil, err
		}

		h.outputIncubating = true

		if err := h.Checkpoint(h); err != nil {
			log.Errorf("unable to Checkpoint: %v", err)
			return nil, err
		}
	}

	// waitForOutputResolution waits for the HTLC output to be fully
	// resolved. The output is considered fully resolved once it has been
	// spent, and the spending transaction has been fully confirmed.
	waitForOutputResolution := func() error {
		// We first need to register to see when the HTLC output itself
		// has been spent by a confirmed transaction.
		spendNtfn, err := h.Notifier.RegisterSpendNtfn(
			&h.htlcResolution.ClaimOutpoint,
			h.htlcResolution.SweepSignDesc.Output.PkScript,
			h.broadcastHeight,
		)
		if err != nil {
			return err
		}

		select {
		case _, ok := <-spendNtfn.Spend:
			if !ok {
				return fmt.Errorf("notifier quit")
			}

		case <-h.Quit:
			return fmt.Errorf("quitting")
		}

		return nil
	}

	// Now that we've handed off the HTLC to the nursery, we'll watch for a
	// spend of the output, and make our next move off of that. Depending
	// on if this is our commitment, or the remote party's commitment,
	// we'll be watching a different outpoint and script.
	outpointToWatch, scriptToWatch, err := h.chainDetailsToWatch()
	if err != nil {
		return nil, err
	}
	spendNtfn, err := h.Notifier.RegisterSpendNtfn(
		outpointToWatch, scriptToWatch, h.broadcastHeight,
	)
	if err != nil {
		return nil, err
	}

	log.Infof("%T(%v): waiting for HTLC output %v to be spent"+
		"fully confirmed", h, h.htlcResolution.ClaimOutpoint,
		outpointToWatch)

	// We'll block here until either we exit, or the HTLC output on the
	// commitment transaction has been spent.
	var (
		spend *chainntnfs.SpendDetail
		ok    bool
	)
	select {
	case spend, ok = <-spendNtfn.Spend:
		if !ok {
			return nil, fmt.Errorf("quitting")
		}

	case <-h.Quit:
		return nil, fmt.Errorf("quitting")
	}

	// If the spend reveals the pre-image, then we'll enter the clean up
	// workflow to pass the pre-image back to the incoming link, add it to
	// the witness cache, and exit.
	if isSuccessSpend(spend, h.htlcResolution.SignedTimeoutTx != nil) {
		log.Infof("%T(%v): HTLC has been swept with pre-image by "+
			"remote party during timeout flow! Adding pre-image to "+
			"witness cache", h.htlcResolution.ClaimOutpoint)

		return h.claimCleanUp(spend)
	}

	log.Infof("%T(%v): resolving htlc with incoming fail msg, fully "+
		"confirmed", h, h.htlcResolution.ClaimOutpoint)

	// At this point, the second-level transaction is sufficiently
	// confirmed, or a transaction directly spending the output is.
	// Therefore, we can now send back our clean up message, failing the
	// HTLC on the incoming link.
	failureMsg := &lnwire.FailPermanentChannelFailure{}
	if err := h.DeliverResolutionMsg(ResolutionMsg{
		SourceChan: h.ShortChanID,
		HtlcIndex:  h.htlcIndex,
		Failure:    failureMsg,
	}); err != nil {
		return nil, err
	}

	// Finally, if this was an output on our commitment transaction, we'll
	// wait for the second-level HTLC output to be spent, and for that
	// transaction itself to confirm.
	if h.htlcResolution.SignedTimeoutTx != nil {
		log.Infof("%T(%v): waiting for nursery to spend CSV delayed "+
			"output", h, h.htlcResolution.ClaimOutpoint)
		if err := waitForOutputResolution(); err != nil {
			return nil, err
		}
	}

	// With the clean up message sent, we'll now mark the contract
	// resolved, and wait.
	h.resolved = true
	return nil, h.Checkpoint(h)
}

// Stop signals the resolver to cancel any current resolution processes, and
// suspend.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) Stop() {
	close(h.Quit)
}

// IsResolved returns true if the stored state in the resolve is fully
// resolved. In this case the target output can be forgotten.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) IsResolved() bool {
	return h.resolved
}

// Encode writes an encoded version of the ContractResolver into the passed
// Writer.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) Encode(w io.Writer) error {
	// First, we'll write out the relevant fields of the
	// OutgoingHtlcResolution to the writer.
	if err := encodeOutgoingResolution(w, &h.htlcResolution); err != nil {
		return err
	}

	// With that portion written, we can now write out the fields specific
	// to the resolver itself.
	if err := binary.Write(w, endian, h.outputIncubating); err != nil {
		return err
	}
	if err := binary.Write(w, endian, h.resolved); err != nil {
		return err
	}
	if err := binary.Write(w, endian, h.broadcastHeight); err != nil {
		return err
	}

	if err := binary.Write(w, endian, h.htlcIndex); err != nil {
		return err
	}

	return nil
}

// Decode attempts to decode an encoded ContractResolver from the passed Reader
// instance, returning an active ContractResolver instance.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) Decode(r io.Reader) error {
	// First, we'll read out all the mandatory fields of the
	// OutgoingHtlcResolution that we store.
	if err := decodeOutgoingResolution(r, &h.htlcResolution); err != nil {
		return err
	}

	// With those fields read, we can now read back the fields that are
	// specific to the resolver itself.
	if err := binary.Read(r, endian, &h.outputIncubating); err != nil {
		return err
	}
	if err := binary.Read(r, endian, &h.resolved); err != nil {
		return err
	}
	if err := binary.Read(r, endian, &h.broadcastHeight); err != nil {
		return err
	}

	if err := binary.Read(r, endian, &h.htlcIndex); err != nil {
		return err
	}

	return nil
}

// AttachResolverKit should be called once a resolved is successfully decoded
// from its stored format. This struct delivers a generic tool kit that
// resolvers need to complete their duty.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) AttachResolverKit(r ResolverKit) {
	h.ResolverKit = r
}

// A compile time assertion to ensure htlcTimeoutResolver meets the
// ContractResolver interface.
var _ ContractResolver = (*htlcTimeoutResolver)(nil)
