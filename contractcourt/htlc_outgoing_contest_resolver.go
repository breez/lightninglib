package contractcourt

import (
	"fmt"
	"io"

	"github.com/btcsuite/btcutil"
)

// htlcOutgoingContestResolver is a ContractResolver that's able to resolve an
// outgoing HTLC that is still contested. An HTLC is still contested, if at the
// time that we broadcast the commitment transaction, it isn't able to be fully
// resolved. In this case, we'll either wait for the HTLC to timeout, or for
// us to learn of the preimage.
type htlcOutgoingContestResolver struct {
	// htlcTimeoutResolver is the inner solver that this resolver may turn
	// into. This only happens if the HTLC expires on-chain.
	htlcTimeoutResolver
}

// Resolve commences the resolution of this contract. As this contract hasn't
// yet timed out, we'll wait for one of two things to happen
//
//   1. The HTLC expires. In this case, we'll sweep the funds and send a clean
//      up cancel message to outside sub-systems.
//
//   2. The remote party sweeps this HTLC on-chain, in which case we'll add the
//      pre-image to our global cache, then send a clean up settle message
//      backwards.
//
// When either of these two things happens, we'll create a new resolver which
// is able to handle the final resolution of the contract. We're only the pivot
// point.
func (h *htlcOutgoingContestResolver) Resolve() (ContractResolver, error) {
	// If we're already full resolved, then we don't have anything further
	// to do.
	if h.resolved {
		return nil, nil
	}

	// Otherwise, we'll watch for two external signals to decide if we'll
	// morph into another resolver, or fully resolve the contract.
	//
	// The output we'll be watching for is the *direct* spend from the HTLC
	// output. If this isn't our commitment transaction, it'll be right on
	// the resolution. Otherwise, we fetch this pointer from the input of
	// the time out transaction.
	outPointToWatch, scriptToWatch, err := h.chainDetailsToWatch()
	if err != nil {
		return nil, err
	}

	// First, we'll register for a spend notification for this output. If
	// the remote party sweeps with the pre-image, we'll be notified.
	spendNtfn, err := h.Notifier.RegisterSpendNtfn(
		outPointToWatch, scriptToWatch, h.broadcastHeight,
	)
	if err != nil {
		return nil, err
	}

	// We'll quickly check to see if the output has already been spent.
	select {
	// If the output has already been spent, then we can stop early and
	// sweep the pre-image from the output.
	case commitSpend, ok := <-spendNtfn.Spend:
		if !ok {
			return nil, fmt.Errorf("quitting")
		}

		// TODO(roasbeef): Checkpoint?
		return h.claimCleanUp(commitSpend)

	// If it hasn't, then we'll watch for both the expiration, and the
	// sweeping out this output.
	default:
	}

	// We'll check the current height, if the HTLC has already expired,
	// then we'll morph immediately into a resolver that can sweep the
	// HTLC.
	//
	// TODO(roasbeef): use grace period instead?
	_, currentHeight, err := h.ChainIO.GetBestBlock()
	if err != nil {
		return nil, err
	}

	// If the current height is >= expiry-1, then a spend will be valid to
	// be included in the next block, and we can immediately return the
	// resolver.
	//
	// TODO(joostjager): Statement above may not be valid. For CLTV locks,
	// the expiry value is the last _invalid_ block. The likely reason that
	// this does not create a problem, is that utxonursery is checking the
	// expiry again (in the proper way). Same holds for minus one operation
	// below.
	//
	// Source:
	// https://github.com/btcsuite/btcd/blob/991d32e72fe84d5fbf9c47cd604d793a0cd3a072/blockchain/validate.go#L154
	if uint32(currentHeight) >= h.htlcResolution.Expiry-1 {
		log.Infof("%T(%v): HTLC has expired (height=%v, expiry=%v), "+
			"transforming into timeout resolver", h,
			h.htlcResolution.ClaimOutpoint, currentHeight,
			h.htlcResolution.Expiry)
		return &h.htlcTimeoutResolver, nil
	}

	// If we reach this point, then we can't fully act yet, so we'll await
	// either of our signals triggering: the HTLC expires, or we learn of
	// the preimage.
	blockEpochs, err := h.Notifier.RegisterBlockEpochNtfn(nil)
	if err != nil {
		return nil, err
	}
	defer blockEpochs.Cancel()

	for {
		select {

		// A new block has arrived, we'll check to see if this leads to
		// HTLC expiration.
		case newBlock, ok := <-blockEpochs.Epochs:
			if !ok {
				return nil, fmt.Errorf("quitting")
			}

			// If this new height expires the HTLC, then we can
			// exit early and create a resolver that's capable of
			// handling the time locked output.
			newHeight := uint32(newBlock.Height)
			if newHeight >= h.htlcResolution.Expiry-1 {
				log.Infof("%T(%v): HTLC has expired "+
					"(height=%v, expiry=%v), transforming "+
					"into timeout resolver", h,
					h.htlcResolution.ClaimOutpoint,
					newHeight, h.htlcResolution.Expiry)
				return &h.htlcTimeoutResolver, nil
			}

		// The output has been spent! This means the preimage has been
		// revealed on-chain.
		case commitSpend, ok := <-spendNtfn.Spend:
			if !ok {
				return nil, fmt.Errorf("quitting")
			}

			// The only way this output can be spent by the remote
			// party is by revealing the preimage. So we'll perform
			// our duties to clean up the contract once it has been
			// claimed.
			return h.claimCleanUp(commitSpend)

		case <-h.Quit:
			return nil, fmt.Errorf("resolver cancelled")
		}
	}
}

// report returns a report on the resolution state of the contract.
func (h *htlcOutgoingContestResolver) report() *ContractReport {
	// No locking needed as these values are read-only.

	finalAmt := h.htlcAmt.ToSatoshis()
	if h.htlcResolution.SignedTimeoutTx != nil {
		finalAmt = btcutil.Amount(
			h.htlcResolution.SignedTimeoutTx.TxOut[0].Value,
		)
	}

	return &ContractReport{
		Outpoint:       h.htlcResolution.ClaimOutpoint,
		Incoming:       false,
		Amount:         finalAmt,
		MaturityHeight: h.htlcResolution.Expiry,
		LimboBalance:   finalAmt,
		Stage:          1,
	}
}

// Stop signals the resolver to cancel any current resolution processes, and
// suspend.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcOutgoingContestResolver) Stop() {
	close(h.Quit)
}

// IsResolved returns true if the stored state in the resolve is fully
// resolved. In this case the target output can be forgotten.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcOutgoingContestResolver) IsResolved() bool {
	return h.resolved
}

// Encode writes an encoded version of the ContractResolver into the passed
// Writer.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcOutgoingContestResolver) Encode(w io.Writer) error {
	return h.htlcTimeoutResolver.Encode(w)
}

// Decode attempts to decode an encoded ContractResolver from the passed Reader
// instance, returning an active ContractResolver instance.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcOutgoingContestResolver) Decode(r io.Reader) error {
	return h.htlcTimeoutResolver.Decode(r)
}

// AttachResolverKit should be called once a resolved is successfully decoded
// from its stored format. This struct delivers a generic tool kit that
// resolvers need to complete their duty.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcOutgoingContestResolver) AttachResolverKit(r ResolverKit) {
	h.ResolverKit = r
}

// A compile time assertion to ensure htlcOutgoingContestResolver meets the
// ContractResolver interface.
var _ ContractResolver = (*htlcOutgoingContestResolver)(nil)
