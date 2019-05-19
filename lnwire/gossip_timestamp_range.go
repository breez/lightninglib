package lnwire

import (
	"io"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
)

// GossipTimestampRange is a message that allows the sender to restrict the set
// of future gossip announcements sent by the receiver. Nodes should send this
// if they have the gossip-queries feature bit active. Nodes are able to send
// new GossipTimestampRange messages to replace the prior window.
type GossipTimestampRange struct {
	// ChainHash denotes the chain that the sender wishes to restrict the
	// set of received announcements of.
	ChainHash chainhash.Hash

	// FirstTimestamp is the timestamp of the earliest announcement message
	// that should be sent by the receiver.
	FirstTimestamp uint32

	// TimestampRange is the horizon beyond the FirstTimestamp that any
	// announcement messages should be sent for. The receiving node MUST
	// NOT send any announcements that have a timestamp greater than
	// FirstTimestamp + TimestampRange.
	TimestampRange uint32
}

// NewGossipTimestampRange creates a new empty GossipTimestampRange message.
func NewGossipTimestampRange() *GossipTimestampRange {
	return &GossipTimestampRange{}
}

// A compile time check to ensure GossipTimestampRange implements the
// lnwire.Message interface.
var _ Message = (*GossipTimestampRange)(nil)

// Decode deserializes a serialized GossipTimestampRange message stored in the
// passed io.Reader observing the specified protocol version.
//
// This is part of the lnwire.Message interface.
func (g *GossipTimestampRange) Decode(r io.Reader, pver uint32) error {
	return ReadElements(r,
		g.ChainHash[:],
		&g.FirstTimestamp,
		&g.TimestampRange,
	)
}

// Encode serializes the target GossipTimestampRange into the passed io.Writer
// observing the protocol version specified.
//
// This is part of the lnwire.Message interface.
func (g *GossipTimestampRange) Encode(w io.Writer, pver uint32) error {
	return WriteElements(w,
		g.ChainHash[:],
		g.FirstTimestamp,
		g.TimestampRange,
	)
}

// MsgType returns the integer uniquely identifying this message type on the
// wire.
//
// This is part of the lnwire.Message interface.
func (g *GossipTimestampRange) MsgType() MessageType {
	return MsgGossipTimestampRange
}

// MaxPayloadLength returns the maximum allowed payload size for a
// GossipTimestampRange complete message observing the specified protocol
// version.
//
// This is part of the lnwire.Message interface.
func (g *GossipTimestampRange) MaxPayloadLength(uint32) uint32 {
	// 32 + 4 + 4
	//
	// TODO(roasbeef): update to 8 byte timestmaps?
	return 40
}
