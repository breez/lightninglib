package main

import (
	"time"

	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/discovery"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing"
	"github.com/roasbeef/btcd/chaincfg/chainhash"
)

// chanSeries is an implementation of the discovery.ChannelGraphTimeSeries
// interface backed by the channeldb ChannelGraph database. We'll provide this
// implementation to the AuthenticatedGossiper so it can properly use the
// in-protocol channel range queries to quickly and efficiently synchronize our
// channel state with all peers.
type chanSeries struct {
	graph *channeldb.ChannelGraph
}

// HighestChanID should return is the channel ID of the channel we know of
// that's furthest in the target chain. This channel will have a block height
// that's close to the current tip of the main chain as we know it.  We'll use
// this to start our QueryChannelRange dance with the remote node.
//
// NOTE: This is part of the discovery.ChannelGraphTimeSeries interface.
func (c *chanSeries) HighestChanID(chain chainhash.Hash) (*lnwire.ShortChannelID, error) {
	chanID, err := c.graph.HighestChanID()
	if err != nil {
		return nil, err
	}

	shortChanID := lnwire.NewShortChanIDFromInt(chanID)
	return &shortChanID, nil
}

// UpdatesInHorizon returns all known channel and node updates with an update
// timestamp between the start time and end time. We'll use this to catch up a
// remote node to the set of channel updates that they may have missed out on
// within the target chain.
//
// NOTE: This is part of the discovery.ChannelGraphTimeSeries interface.
func (c *chanSeries) UpdatesInHorizon(chain chainhash.Hash,
	startTime time.Time, endTime time.Time) ([]lnwire.Message, error) {

	var updates []lnwire.Message

	// First, we'll query for all the set of channels that have an update
	// that falls within the specified horizon.
	chansInHorizon, err := c.graph.ChanUpdatesInHorizon(
		startTime, endTime,
	)
	if err != nil {
		return nil, err
	}
	for _, channel := range chansInHorizon {
		// If the channel hasn't been fully advertised yet, or is a
		// private channel, then we'll skip it as we can't construct a
		// full authentication proof if one is requested.
		if channel.Info.AuthProof == nil {
			continue
		}

		chanAnn, edge1, edge2, err := discovery.CreateChanAnnouncement(
			channel.Info.AuthProof, channel.Info, channel.Policy1,
			channel.Policy2,
		)
		if err != nil {
			return nil, err
		}

		updates = append(updates, chanAnn)
		if edge1 != nil {
			updates = append(updates, edge1)
		}
		if edge2 != nil {
			updates = append(updates, edge2)
		}
	}

	// Next, we'll send out all the node announcements that have an update
	// within the horizon as well. We send these second to ensure that they
	// follow any active channels they have.
	nodeAnnsInHorizon, err := c.graph.NodeUpdatesInHorizon(
		startTime, endTime,
	)
	if err != nil {
		return nil, err
	}
	for _, nodeAnn := range nodeAnnsInHorizon {
		nodeUpdate, err := makeNodeAnn(&nodeAnn)
		if err != nil {
			return nil, err
		}

		updates = append(updates, nodeUpdate)
	}

	return updates, nil
}

// FilterKnownChanIDs takes a target chain, and a set of channel ID's, and
// returns a filtered set of chan ID's. This filtered set of chan ID's
// represents the ID's that we don't know of which were in the passed superSet.
//
// NOTE: This is part of the discovery.ChannelGraphTimeSeries interface.
func (c *chanSeries) FilterKnownChanIDs(chain chainhash.Hash,
	superSet []lnwire.ShortChannelID) ([]lnwire.ShortChannelID, error) {

	chanIDs := make([]uint64, 0, len(superSet))
	for _, chanID := range superSet {
		chanIDs = append(chanIDs, chanID.ToUint64())
	}

	newChanIDs, err := c.graph.FilterKnownChanIDs(chanIDs)
	if err != nil {
		return nil, err
	}

	filteredIDs := make([]lnwire.ShortChannelID, 0, len(newChanIDs))
	for _, chanID := range newChanIDs {
		filteredIDs = append(
			filteredIDs, lnwire.NewShortChanIDFromInt(chanID),
		)
	}

	return filteredIDs, nil
}

// FilterChannelRange returns the set of channels that we created between the
// start height and the end height. We'll use this respond to a remote peer's
// QueryChannelRange message.
//
// NOTE: This is part of the discovery.ChannelGraphTimeSeries interface.
func (c *chanSeries) FilterChannelRange(chain chainhash.Hash,
	startHeight, endHeight uint32) ([]lnwire.ShortChannelID, error) {

	chansInRange, err := c.graph.FilterChannelRange(startHeight, endHeight)
	if err != nil {
		return nil, err
	}

	chanResp := make([]lnwire.ShortChannelID, 0, len(chansInRange))
	for _, chanID := range chansInRange {
		chanResp = append(
			chanResp, lnwire.NewShortChanIDFromInt(chanID),
		)
	}

	return chanResp, nil
}

func makeNodeAnn(n *channeldb.LightningNode) (*lnwire.NodeAnnouncement, error) {
	alias, _ := lnwire.NewNodeAlias(n.Alias)

	wireSig, err := lnwire.NewSigFromRawSignature(n.AuthSigBytes)
	if err != nil {
		return nil, err
	}
	return &lnwire.NodeAnnouncement{
		Signature: wireSig,
		Timestamp: uint32(n.LastUpdate.Unix()),
		Addresses: n.Addresses,
		NodeID:    n.PubKeyBytes,
		Features:  n.Features.RawFeatureVector,
		RGBColor:  n.Color,
		Alias:     alias,
	}, nil
}

// FetchChanAnns returns a full set of channel announcements as well as their
// updates that match the set of specified short channel ID's.  We'll use this
// to reply to a QueryShortChanIDs message sent by a remote peer. The response
// will contain a unique set of ChannelAnnouncements, the latest ChannelUpdate
// for each of the announcements, and a unique set of NodeAnnouncements.
//
// NOTE: This is part of the discovery.ChannelGraphTimeSeries interface.
func (c *chanSeries) FetchChanAnns(chain chainhash.Hash,
	shortChanIDs []lnwire.ShortChannelID) ([]lnwire.Message, error) {

	chanIDs := make([]uint64, 0, len(shortChanIDs))
	for _, chanID := range shortChanIDs {
		chanIDs = append(chanIDs, chanID.ToUint64())
	}

	channels, err := c.graph.FetchChanInfos(chanIDs)
	if err != nil {
		return nil, err
	}

	// We'll use this map to ensure we don't send the same node
	// announcement more than one time as one node may have many channel
	// anns we'll need to send.
	nodePubsSent := make(map[routing.Vertex]struct{})

	chanAnns := make([]lnwire.Message, 0, len(channels)*3)
	for _, channel := range channels {
		// If the channel doesn't have an authentication proof, then we
		// won't send it over as it may not yet be finalized, or be a
		// non-advertised channel.
		if channel.Info.AuthProof == nil {
			continue
		}

		chanAnn, edge1, edge2, err := discovery.CreateChanAnnouncement(
			channel.Info.AuthProof, channel.Info, channel.Policy1,
			channel.Policy2,
		)
		if err != nil {
			return nil, err
		}

		chanAnns = append(chanAnns, chanAnn)
		if edge1 != nil {
			chanAnns = append(chanAnns, edge1)

			// If this edge has a validated node announcement, that
			// we haven't yet sent, then we'll send that as well.
			nodePub := channel.Policy1.Node.PubKeyBytes
			hasNodeAnn := channel.Policy1.Node.HaveNodeAnnouncement
			if _, ok := nodePubsSent[nodePub]; !ok && hasNodeAnn {
				nodeAnn, err := makeNodeAnn(channel.Policy1.Node)
				if err != nil {
					return nil, err
				}

				chanAnns = append(chanAnns, nodeAnn)
				nodePubsSent[nodePub] = struct{}{}
			}
		}
		if edge2 != nil {
			chanAnns = append(chanAnns, edge2)

			// If this edge has a validated node announcement, that
			// we haven't yet sent, then we'll send that as well.
			nodePub := channel.Policy2.Node.PubKeyBytes
			hasNodeAnn := channel.Policy2.Node.HaveNodeAnnouncement
			if _, ok := nodePubsSent[nodePub]; !ok && hasNodeAnn {
				nodeAnn, err := makeNodeAnn(channel.Policy2.Node)
				if err != nil {
					return nil, err
				}

				chanAnns = append(chanAnns, nodeAnn)
				nodePubsSent[nodePub] = struct{}{}
			}
		}
	}

	return chanAnns, nil
}

// FetchChanUpdates returns the latest channel update messages for the
// specified short channel ID. If no channel updates are known for the channel,
// then an empty slice will be returned.
//
// NOTE: This is part of the discovery.ChannelGraphTimeSeries interface.
func (c *chanSeries) FetchChanUpdates(chain chainhash.Hash,
	shortChanID lnwire.ShortChannelID) ([]*lnwire.ChannelUpdate, error) {

	chanInfo, e1, e2, err := c.graph.FetchChannelEdgesByID(
		shortChanID.ToUint64(),
	)
	if err != nil {
		return nil, err
	}

	chanUpdates := make([]*lnwire.ChannelUpdate, 0, 2)
	if e1 != nil {
		chanUpdate := &lnwire.ChannelUpdate{
			ChainHash:       chanInfo.ChainHash,
			ShortChannelID:  shortChanID,
			Timestamp:       uint32(e1.LastUpdate.Unix()),
			Flags:           e1.Flags,
			TimeLockDelta:   e1.TimeLockDelta,
			HtlcMinimumMsat: e1.MinHTLC,
			BaseFee:         uint32(e1.FeeBaseMSat),
			FeeRate:         uint32(e1.FeeProportionalMillionths),
		}
		chanUpdate.Signature, err = lnwire.NewSigFromRawSignature(e1.SigBytes)
		if err != nil {
			return nil, err
		}

		chanUpdates = append(chanUpdates, chanUpdate)
	}
	if e2 != nil {
		chanUpdate := &lnwire.ChannelUpdate{
			ChainHash:       chanInfo.ChainHash,
			ShortChannelID:  shortChanID,
			Timestamp:       uint32(e2.LastUpdate.Unix()),
			Flags:           e2.Flags,
			TimeLockDelta:   e2.TimeLockDelta,
			HtlcMinimumMsat: e2.MinHTLC,
			BaseFee:         uint32(e2.FeeBaseMSat),
			FeeRate:         uint32(e2.FeeProportionalMillionths),
		}
		chanUpdate.Signature, err = lnwire.NewSigFromRawSignature(e2.SigBytes)
		if err != nil {
			return nil, err
		}

		chanUpdates = append(chanUpdates, chanUpdate)
	}

	return chanUpdates, nil
}

// A compile-time assertion to ensure that chanSeries meets the
// discovery.ChannelGraphTimeSeries interface.
var _ discovery.ChannelGraphTimeSeries = (*chanSeries)(nil)
