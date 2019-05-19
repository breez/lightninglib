package lnwire

import "io"

// CommitSig is sent by either side to stage any pending HTLC's in the
// receiver's pending set into a new commitment state.  Implicitly, the new
// commitment transaction constructed which has been signed by CommitSig
// includes all HTLC's in the remote node's pending set. A CommitSig message
// may be sent after a series of UpdateAddHTLC/UpdateFulfillHTLC messages in
// order to batch add several HTLC's with a single signature covering all
// implicitly accepted HTLC's.
type CommitSig struct {
	// ChanID uniquely identifies to which currently active channel this
	// CommitSig applies to.
	ChanID ChannelID

	// CommitSig is Alice's signature for Bob's new commitment transaction.
	// Alice is able to send this signature without requesting any
	// additional data due to the piggybacking of Bob's next revocation
	// hash in his prior RevokeAndAck message, as well as the canonical
	// ordering used for all inputs/outputs within commitment transactions.
	// If initiating a new commitment state, this signature should ONLY
	// cover all of the sending party's pending log updates, and the log
	// updates of the remote party that have been ACK'd.
	CommitSig Sig

	// HtlcSigs is a signature for each relevant HTLC output within the
	// created commitment. The order of the signatures is expected to be
	// identical to the placement of the HTLC's within the BIP 69 sorted
	// commitment transaction. For each outgoing HTLC (from the PoV of the
	// sender of this message), a signature for an HTLC timeout transaction
	// should be signed, for each incoming HTLC the HTLC timeout
	// transaction should be signed.
	HtlcSigs []Sig
}

// NewCommitSig creates a new empty CommitSig message.
func NewCommitSig() *CommitSig {
	return &CommitSig{}
}

// A compile time check to ensure CommitSig implements the lnwire.Message
// interface.
var _ Message = (*CommitSig)(nil)

// Decode deserializes a serialized CommitSig message stored in the
// passed io.Reader observing the specified protocol version.
//
// This is part of the lnwire.Message interface.
func (c *CommitSig) Decode(r io.Reader, pver uint32) error {
	return ReadElements(r,
		&c.ChanID,
		&c.CommitSig,
		&c.HtlcSigs,
	)
}

// Encode serializes the target CommitSig into the passed io.Writer
// observing the protocol version specified.
//
// This is part of the lnwire.Message interface.
func (c *CommitSig) Encode(w io.Writer, pver uint32) error {
	return WriteElements(w,
		c.ChanID,
		c.CommitSig,
		c.HtlcSigs,
	)
}

// MsgType returns the integer uniquely identifying this message type on the
// wire.
//
// This is part of the lnwire.Message interface.
func (c *CommitSig) MsgType() MessageType {
	return MsgCommitSig
}

// MaxPayloadLength returns the maximum allowed payload size for a
// CommitSig complete message observing the specified protocol version.
//
// This is part of the lnwire.Message interface.
func (c *CommitSig) MaxPayloadLength(uint32) uint32 {
	// 32 + 64 + 2 + max_allowed_htlcs
	return MaxMessagePayload
}
