package lnwire

import "io"

// FundingSigned is sent from Bob (the responder) to Alice (the initiator)
// after receiving the funding outpoint and her signature for Bob's version of
// the commitment transaction.
type FundingSigned struct {
	// ChannelPoint is the particular active channel that this
	// FundingSigned is bound to.
	ChanID ChannelID

	// CommitSig is Bob's signature for Alice's version of the commitment
	// transaction.
	CommitSig Sig
}

// A compile time check to ensure FundingSigned implements the lnwire.Message
// interface.
var _ Message = (*FundingSigned)(nil)

// Encode serializes the target FundingSigned into the passed io.Writer
// implementation. Serialization will observe the rules defined by the passed
// protocol version.
//
// This is part of the lnwire.Message interface.
func (f *FundingSigned) Encode(w io.Writer, pver uint32) error {
	return WriteElements(w, f.ChanID, f.CommitSig)
}

// Decode deserializes the serialized FundingSigned stored in the passed
// io.Reader into the target FundingSigned using the deserialization rules
// defined by the passed protocol version.
//
// This is part of the lnwire.Message interface.
func (f *FundingSigned) Decode(r io.Reader, pver uint32) error {
	return ReadElements(r, &f.ChanID, &f.CommitSig)
}

// MsgType returns the uint32 code which uniquely identifies this message as a
// FundingSigned on the wire.
//
// This is part of the lnwire.Message interface.
func (f *FundingSigned) MsgType() MessageType {
	return MsgFundingSigned
}

// MaxPayloadLength returns the maximum allowed payload length for a
// FundingSigned message.
//
// This is part of the lnwire.Message interface.
func (f *FundingSigned) MaxPayloadLength(uint32) uint32 {
	// 32 + 64
	return 96
}
