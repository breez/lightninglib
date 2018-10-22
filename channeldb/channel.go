package channeldb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/breez/lightninglib/keychain"
	"github.com/breez/lightninglib/lnwire"
	"github.com/breez/lightninglib/shachain"
	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/coreos/bbolt"
)

var (
	// closedChannelBucket stores summarization information concerning
	// previously open, but now closed channels.
	closedChannelBucket = []byte("closed-chan-bucket")

	// openChanBucket stores all the currently open channels. This bucket
	// has a second, nested bucket which is keyed by a node's ID. Within
	// that node ID bucket, all attributes required to track, update, and
	// close a channel are stored.
	//
	// openChan -> nodeID -> chanPoint
	//
	// TODO(roasbeef): flesh out comment
	openChannelBucket = []byte("open-chan-bucket")

	// chanInfoKey can be accessed within the bucket for a channel
	// (identified by its chanPoint). This key stores all the static
	// information for a channel which is decided at the end of  the
	// funding flow.
	chanInfoKey = []byte("chan-info-key")

	// chanCommitmentKey can be accessed within the sub-bucket for a
	// particular channel. This key stores the up to date commitment state
	// for a particular channel party. Appending a 0 to the end of this key
	// indicates it's the commitment for the local party, and appending a 1
	// to the end of this key indicates it's the commitment for the remote
	// party.
	chanCommitmentKey = []byte("chan-commitment-key")

	// revocationStateKey stores their current revocation hash, our
	// preimage producer and their preimage store.
	revocationStateKey = []byte("revocation-state-key")

	// dataLossCommitPointKey stores the commitment point received from the
	// remote peer during a channel sync in case we have lost channel state.
	dataLossCommitPointKey = []byte("data-loss-commit-point-key")

	// commitDiffKey stores the current pending commitment state we've
	// extended to the remote party (if any). Each time we propose a new
	// state, we store the information necessary to reconstruct this state
	// from the prior commitment. This allows us to resync the remote party
	// to their expected state in the case of message loss.
	//
	// TODO(roasbeef): rename to commit chain?
	commitDiffKey = []byte("commit-diff-key")

	// revocationLogBucket is dedicated for storing the necessary delta
	// state between channel updates required to re-construct a past state
	// in order to punish a counterparty attempting a non-cooperative
	// channel closure. This key should be accessed from within the
	// sub-bucket of a target channel, identified by its channel point.
	revocationLogBucket = []byte("revocation-log-key")
)

var (
	// ErrNoCommitmentsFound is returned when a channel has not set
	// commitment states.
	ErrNoCommitmentsFound = fmt.Errorf("no commitments found")

	// ErrNoChanInfoFound is returned when a particular channel does not
	// have any channels state.
	ErrNoChanInfoFound = fmt.Errorf("no chan info found")

	// ErrNoRevocationsFound is returned when revocation state for a
	// particular channel cannot be found.
	ErrNoRevocationsFound = fmt.Errorf("no revocations found")

	// ErrNoPendingCommit is returned when there is not a pending
	// commitment for a remote party. A new commitment is written to disk
	// each time we write a new state in order to be properly fault
	// tolerant.
	ErrNoPendingCommit = fmt.Errorf("no pending commits found")

	// ErrInvalidCircuitKeyLen signals that a circuit key could not be
	// decoded because the byte slice is of an invalid length.
	ErrInvalidCircuitKeyLen = fmt.Errorf(
		"length of serialized circuit key must be 16 bytes")

	// ErrNoCommitPoint is returned when no data loss commit point is found
	// in the database.
	ErrNoCommitPoint = fmt.Errorf("no commit point found")
)

// ChannelType is an enum-like type that describes one of several possible
// channel types. Each open channel is associated with a particular type as the
// channel type may determine how higher level operations are conducted such as
// fee negotiation, channel closing, the format of HTLCs, etc.
// TODO(roasbeef): split up per-chain?
type ChannelType uint8

const (
	// NOTE: iota isn't used here for this enum needs to be stable
	// long-term as it will be persisted to the database.

	// SingleFunder represents a channel wherein one party solely funds the
	// entire capacity of the channel.
	SingleFunder = 0

	// DualFunder represents a channel wherein both parties contribute
	// funds towards the total capacity of the channel. The channel may be
	// funded symmetrically or asymmetrically.
	DualFunder = 1
)

// ChannelConstraints represents a set of constraints meant to allow a node to
// limit their exposure, enact flow control and ensure that all HTLCs are
// economically relevant. This struct will be mirrored for both sides of the
// channel, as each side will enforce various constraints that MUST be adhered
// to for the life time of the channel. The parameters for each of these
// constraints are static for the duration of the channel, meaning the channel
// must be torn down for them to change.
type ChannelConstraints struct {
	// DustLimit is the threshold (in satoshis) below which any outputs
	// should be trimmed. When an output is trimmed, it isn't materialized
	// as an actual output, but is instead burned to miner's fees.
	DustLimit btcutil.Amount

	// ChanReserve is an absolute reservation on the channel for the
	// owner of this set of constraints. This means that the current
	// settled balance for this node CANNOT dip below the reservation
	// amount. This acts as a defense against costless attacks when
	// either side no longer has any skin in the game.
	ChanReserve btcutil.Amount

	// MaxPendingAmount is the maximum pending HTLC value that the
	// owner of these constraints can offer the remote node at a
	// particular time.
	MaxPendingAmount lnwire.MilliSatoshi

	// MinHTLC is the minimum HTLC value that the owner of these
	// constraints can offer the remote node. If any HTLCs below this
	// amount are offered, then the HTLC will be rejected. This, in
	// tandem with the dust limit allows a node to regulate the
	// smallest HTLC that it deems economically relevant.
	MinHTLC lnwire.MilliSatoshi

	// MaxAcceptedHtlcs is the maximum number of HTLCs that the owner of
	// this set of constraints can offer the remote node. This allows each
	// node to limit their over all exposure to HTLCs that may need to be
	// acted upon in the case of a unilateral channel closure or a contract
	// breach.
	MaxAcceptedHtlcs uint16
}

// ChannelConfig is a struct that houses the various configuration opens for
// channels. Each side maintains an instance of this configuration file as it
// governs: how the funding and commitment transaction to be created, the
// nature of HTLC's allotted, the keys to be used for delivery, and relative
// time lock parameters.
type ChannelConfig struct {
	// ChannelConstraints is the set of constraints that must be upheld for
	// the duration of the channel for the owner of this channel
	// configuration. Constraints govern a number of flow control related
	// parameters, also including the smallest HTLC that will be accepted
	// by a participant.
	ChannelConstraints

	// CsvDelay is the relative time lock delay expressed in blocks. Any
	// settled outputs that pay to the owner of this channel configuration
	// MUST ensure that the delay branch uses this value as the relative
	// time lock. Similarly, any HTLC's offered by this node should use
	// this value as well.
	CsvDelay uint16

	// MultiSigKey is the key to be used within the 2-of-2 output script
	// for the owner of this channel config.
	MultiSigKey keychain.KeyDescriptor

	// RevocationBasePoint is the base public key to be used when deriving
	// revocation keys for the remote node's commitment transaction. This
	// will be combined along with a per commitment secret to derive a
	// unique revocation key for each state.
	RevocationBasePoint keychain.KeyDescriptor

	// PaymentBasePoint is the base public key to be used when deriving
	// the key used within the non-delayed pay-to-self output on the
	// commitment transaction for a node. This will be combined with a
	// tweak derived from the per-commitment point to ensure unique keys
	// for each commitment transaction.
	PaymentBasePoint keychain.KeyDescriptor

	// DelayBasePoint is the base public key to be used when deriving the
	// key used within the delayed pay-to-self output on the commitment
	// transaction for a node. This will be combined with a tweak derived
	// from the per-commitment point to ensure unique keys for each
	// commitment transaction.
	DelayBasePoint keychain.KeyDescriptor

	// HtlcBasePoint is the base public key to be used when deriving the
	// local HTLC key. The derived key (combined with the tweak derived
	// from the per-commitment point) is used within the "to self" clause
	// within any HTLC output scripts.
	HtlcBasePoint keychain.KeyDescriptor
}

// ChannelCommitment is a snapshot of the commitment state at a particular
// point in the commitment chain. With each state transition, a snapshot of the
// current state along with all non-settled HTLCs are recorded. These snapshots
// detail the state of the _remote_ party's commitment at a particular state
// number.  For ourselves (the local node) we ONLY store our most recent
// (unrevoked) state for safety purposes.
type ChannelCommitment struct {
	// CommitHeight is the update number that this ChannelDelta represents
	// the total number of commitment updates to this point. This can be
	// viewed as sort of a "commitment height" as this number is
	// monotonically increasing.
	CommitHeight uint64

	// LocalLogIndex is the cumulative log index index of the local node at
	// this point in the commitment chain. This value will be incremented
	// for each _update_ added to the local update log.
	LocalLogIndex uint64

	// LocalHtlcIndex is the current local running HTLC index. This value
	// will be incremented for each outgoing HTLC the local node offers.
	LocalHtlcIndex uint64

	// RemoteLogIndex is the cumulative log index index of the remote node
	// at this point in the commitment chain. This value will be
	// incremented for each _update_ added to the remote update log.
	RemoteLogIndex uint64

	// RemoteHtlcIndex is the current remote running HTLC index. This value
	// will be incremented for each outgoing HTLC the remote node offers.
	RemoteHtlcIndex uint64

	// LocalBalance is the current available settled balance within the
	// channel directly spendable by us.
	LocalBalance lnwire.MilliSatoshi

	// RemoteBalance is the current available settled balance within the
	// channel directly spendable by the remote node.
	RemoteBalance lnwire.MilliSatoshi

	// CommitFee is the amount calculated to be paid in fees for the
	// current set of commitment transactions. The fee amount is persisted
	// with the channel in order to allow the fee amount to be removed and
	// recalculated with each channel state update, including updates that
	// happen after a system restart.
	CommitFee btcutil.Amount

	// FeePerKw is the min satoshis/kilo-weight that should be paid within
	// the commitment transaction for the entire duration of the channel's
	// lifetime. This field may be updated during normal operation of the
	// channel as on-chain conditions change.
	//
	// TODO(halseth): make this SatPerKWeight. Cannot be done atm because
	// this will cause the import cycle lnwallet<->channeldb. Fee
	// estimation stuff should be in its own package.
	FeePerKw btcutil.Amount

	// CommitTx is the latest version of the commitment state, broadcast
	// able by us.
	CommitTx *wire.MsgTx

	// CommitSig is one half of the signature required to fully complete
	// the script for the commitment transaction above. This is the
	// signature signed by the remote party for our version of the
	// commitment transactions.
	CommitSig []byte

	// Htlcs is the set of HTLC's that are pending at this particular
	// commitment height.
	Htlcs []HTLC

	// TODO(roasbeef): pending commit pointer?
	//  * lets just walk through
}

// ChannelStatus is a bit vector used to indicate whether an OpenChannel is in
// the default usable state, or a state where it shouldn't be used.
type ChannelStatus uint8

var (
	// Default is the normal state of an open channel.
	Default ChannelStatus

	// Borked indicates that the channel has entered an irreconcilable
	// state, triggered by a state desynchronization or channel breach.
	// Channels in this state should never be added to the htlc switch.
	Borked ChannelStatus = 1

	// CommitmentBroadcasted indicates that a commitment for this channel
	// has been broadcasted.
	CommitmentBroadcasted ChannelStatus = 1 << 1

	// LocalDataLoss indicates that we have lost channel state for this
	// channel, and broadcasting our latest commitment might be considered
	// a breach.
	// TODO(halseh): actually enforce that we are not force closing such a
	// channel.
	LocalDataLoss ChannelStatus = 1 << 2
)

// String returns a human-readable representation of the ChannelStatus.
func (c ChannelStatus) String() string {
	switch c {
	case Default:
		return "Default"
	case Borked:
		return "Borked"
	case CommitmentBroadcasted:
		return "CommitmentBroadcasted"
	case LocalDataLoss:
		return "LocalDataLoss"
	default:
		return fmt.Sprintf("Unknown(%08b)", c)
	}
}

// OpenChannel encapsulates the persistent and dynamic state of an open channel
// with a remote node. An open channel supports several options for on-disk
// serialization depending on the exact context. Full (upon channel creation)
// state commitments, and partial (due to a commitment update) writes are
// supported. Each partial write due to a state update appends the new update
// to an on-disk log, which can then subsequently be queried in order to
// "time-travel" to a prior state.
type OpenChannel struct {
	// ChanType denotes which type of channel this is.
	ChanType ChannelType

	// ChainHash is a hash which represents the blockchain that this
	// channel will be opened within. This value is typically the genesis
	// hash. In the case that the original chain went through a contentious
	// hard-fork, then this value will be tweaked using the unique fork
	// point on each branch.
	ChainHash chainhash.Hash

	// FundingOutpoint is the outpoint of the final funding transaction.
	// This value uniquely and globally identities the channel within the
	// target blockchain as specified by the chain hash parameter.
	FundingOutpoint wire.OutPoint

	// ShortChannelID encodes the exact location in the chain in which the
	// channel was initially confirmed. This includes: the block height,
	// transaction index, and the output within the target transaction.
	ShortChannelID lnwire.ShortChannelID

	// IsPending indicates whether a channel's funding transaction has been
	// confirmed.
	IsPending bool

	// IsInitiator is a bool which indicates if we were the original
	// initiator for the channel. This value may affect how higher levels
	// negotiate fees, or close the channel.
	IsInitiator bool

	// chanStatus is the current status of this channel. If it is not in
	// the state Default, it should not be used for forwarding payments.
	chanStatus ChannelStatus

	// FundingBroadcastHeight is the height in which the funding
	// transaction was broadcast. This value can be used by higher level
	// sub-systems to determine if a channel is stale and/or should have
	// been confirmed before a certain height.
	FundingBroadcastHeight uint32

	// NumConfsRequired is the number of confirmations a channel's funding
	// transaction must have received in order to be considered available
	// for normal transactional use.
	NumConfsRequired uint16

	// ChannelFlags holds the flags that were sent as part of the
	// open_channel message.
	ChannelFlags lnwire.FundingFlag

	// IdentityPub is the identity public key of the remote node this
	// channel has been established with.
	IdentityPub *btcec.PublicKey

	// Capacity is the total capacity of this channel.
	Capacity btcutil.Amount

	// TotalMSatSent is the total number of milli-satoshis we've sent
	// within this channel.
	TotalMSatSent lnwire.MilliSatoshi

	// TotalMSatReceived is the total number of milli-satoshis we've
	// received within this channel.
	TotalMSatReceived lnwire.MilliSatoshi

	// LocalChanCfg is the channel configuration for the local node.
	LocalChanCfg ChannelConfig

	// RemoteChanCfg is the channel configuration for the remote node.
	RemoteChanCfg ChannelConfig

	// LocalCommitment is the current local commitment state for the local
	// party. This is stored distinct from the state of the remote party
	// as there are certain asymmetric parameters which affect the
	// structure of each commitment.
	LocalCommitment ChannelCommitment

	// RemoteCommitment is the current remote commitment state for the
	// remote party. This is stored distinct from the state of the local
	// party as there are certain asymmetric parameters which affect the
	// structure of each commitment.
	RemoteCommitment ChannelCommitment

	// RemoteCurrentRevocation is the current revocation for their
	// commitment transaction. However, since this the derived public key,
	// we don't yet have the private key so we aren't yet able to verify
	// that it's actually in the hash chain.
	RemoteCurrentRevocation *btcec.PublicKey

	// RemoteNextRevocation is the revocation key to be used for the *next*
	// commitment transaction we create for the local node. Within the
	// specification, this value is referred to as the
	// per-commitment-point.
	RemoteNextRevocation *btcec.PublicKey

	// RevocationProducer is used to generate the revocation in such a way
	// that remote side might store it efficiently and have the ability to
	// restore the revocation by index if needed. Current implementation of
	// secret producer is shachain producer.
	RevocationProducer shachain.Producer

	// RevocationStore is used to efficiently store the revocations for
	// previous channels states sent to us by remote side. Current
	// implementation of secret store is shachain store.
	RevocationStore shachain.Store

	// Packager is used to create and update forwarding packages for this
	// channel, which encodes all necessary information to recover from
	// failures and reforward HTLCs that were not fully processed.
	Packager FwdPackager

	// FundingTxn is the transaction containing this channel's funding
	// outpoint. Upon restarts, this txn will be rebroadcast if the channel
	// is found to be pending.
	//
	// NOTE: This value will only be populated for single-funder channels
	// for which we are the initiator.
	FundingTxn *wire.MsgTx

	// TODO(roasbeef): eww
	Db *DB

	// TODO(roasbeef): just need to store local and remote HTLC's?

	sync.RWMutex
}

// FullSync serializes, and writes to disk the *full* channel state, using
// both the active channel bucket to store the prefixed column fields, and the
// remote node's ID to store the remainder of the channel state.
func (c *OpenChannel) FullSync() error {
	c.Lock()
	defer c.Unlock()

	return c.Db.Update(c.fullSync)
}

// ShortChanID returns the current ShortChannelID of this channel.
func (c *OpenChannel) ShortChanID() lnwire.ShortChannelID {
	c.RLock()
	defer c.RUnlock()

	return c.ShortChannelID
}

// ChanStatus returns the current ChannelStatus of this channel.
func (c *OpenChannel) ChanStatus() ChannelStatus {
	c.RLock()
	defer c.RUnlock()

	return c.chanStatus
}

// RefreshShortChanID updates the in-memory short channel ID using the latest
// value observed on disk.
func (c *OpenChannel) RefreshShortChanID() error {
	c.Lock()
	defer c.Unlock()

	var sid lnwire.ShortChannelID
	err := c.Db.View(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		channel, err := fetchOpenChannel(chanBucket, &c.FundingOutpoint)
		if err != nil {
			return err
		}

		sid = channel.ShortChannelID

		return nil
	})
	if err != nil {
		return err
	}

	c.ShortChannelID = sid
	c.Packager = NewChannelPackager(sid)

	return nil
}

// fetchChanBucket is a helper function that returns the bucket where a
// channel's data resides in given: the public key for the node, the outpoint,
// and the chainhash that the channel resides on.
func fetchChanBucket(tx *bolt.Tx, nodeKey *btcec.PublicKey,
	outPoint *wire.OutPoint, chainHash chainhash.Hash) (*bolt.Bucket, error) {

	// First fetch the top level bucket which stores all data related to
	// current, active channels.
	openChanBucket := tx.Bucket(openChannelBucket)
	if openChanBucket == nil {
		return nil, ErrNoChanDBExists
	}

	// Within this top level bucket, fetch the bucket dedicated to storing
	// open channel data specific to the remote node.
	nodePub := nodeKey.SerializeCompressed()
	nodeChanBucket := openChanBucket.Bucket(nodePub)
	if nodeChanBucket == nil {
		return nil, ErrNoActiveChannels
	}

	// We'll then recurse down an additional layer in order to fetch the
	// bucket for this particular chain.
	chainBucket := nodeChanBucket.Bucket(chainHash[:])
	if chainBucket == nil {
		return nil, ErrNoActiveChannels
	}

	// With the bucket for the node and chain fetched, we can now go down
	// another level, for this channel itself.
	var chanPointBuf bytes.Buffer
	if err := writeOutpoint(&chanPointBuf, outPoint); err != nil {
		return nil, err
	}
	chanBucket := chainBucket.Bucket(chanPointBuf.Bytes())
	if chanBucket == nil {
		return nil, ErrChannelNotFound
	}

	return chanBucket, nil
}

// fullSync is an internal version of the FullSync method which allows callers
// to sync the contents of an OpenChannel while re-using an existing database
// transaction.
func (c *OpenChannel) fullSync(tx *bolt.Tx) error {
	// First fetch the top level bucket which stores all data related to
	// current, active channels.
	openChanBucket, err := tx.CreateBucketIfNotExists(openChannelBucket)
	if err != nil {
		return err
	}

	// Within this top level bucket, fetch the bucket dedicated to storing
	// open channel data specific to the remote node.
	nodePub := c.IdentityPub.SerializeCompressed()
	nodeChanBucket, err := openChanBucket.CreateBucketIfNotExists(nodePub)
	if err != nil {
		return err
	}

	// We'll then recurse down an additional layer in order to fetch the
	// bucket for this particular chain.
	chainBucket, err := nodeChanBucket.CreateBucketIfNotExists(c.ChainHash[:])
	if err != nil {
		return err
	}

	// With the bucket for the node fetched, we can now go down another
	// level, creating the bucket (if it doesn't exist), for this channel
	// itself.
	var chanPointBuf bytes.Buffer
	if err := writeOutpoint(&chanPointBuf, &c.FundingOutpoint); err != nil {
		return err
	}
	chanBucket, err := chainBucket.CreateBucketIfNotExists(
		chanPointBuf.Bytes(),
	)
	if err != nil {
		return err
	}

	return putOpenChannel(chanBucket, c)
}

// MarkAsOpen marks a channel as fully open given a locator that uniquely
// describes its location within the chain.
func (c *OpenChannel) MarkAsOpen(openLoc lnwire.ShortChannelID) error {
	c.Lock()
	defer c.Unlock()

	if err := c.Db.Update(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		channel, err := fetchOpenChannel(chanBucket, &c.FundingOutpoint)
		if err != nil {
			return err
		}

		channel.IsPending = false
		channel.ShortChannelID = openLoc

		return putOpenChannel(chanBucket, channel)
	}); err != nil {
		return err
	}

	c.IsPending = false
	c.ShortChannelID = openLoc
	c.Packager = NewChannelPackager(openLoc)

	return nil
}

// MarkDataLoss marks sets the channel status to LocalDataLoss and stores the
// passed commitPoint for use to retrieve funds in case the remote force closes
// the channel.
func (c *OpenChannel) MarkDataLoss(commitPoint *btcec.PublicKey) error {
	c.Lock()
	defer c.Unlock()

	var status ChannelStatus
	if err := c.Db.Update(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		channel, err := fetchOpenChannel(chanBucket, &c.FundingOutpoint)
		if err != nil {
			return err
		}

		// Add status LocalDataLoss to the existing bitvector found in
		// the DB.
		status = channel.chanStatus | LocalDataLoss
		channel.chanStatus = status

		var b bytes.Buffer
		if err := WriteElement(&b, commitPoint); err != nil {
			return err
		}

		err = chanBucket.Put(dataLossCommitPointKey, b.Bytes())
		if err != nil {
			return err
		}

		return putOpenChannel(chanBucket, channel)
	}); err != nil {
		return err
	}

	// Update the in-memory representation to keep it in sync with the DB.
	c.chanStatus = status

	return nil
}

// DataLossCommitPoint retrieves the stored commit point set during
// MarkDataLoss. If not found ErrNoCommitPoint is returned.
func (c *OpenChannel) DataLossCommitPoint() (*btcec.PublicKey, error) {
	var commitPoint *btcec.PublicKey

	err := c.Db.View(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		switch err {
		case nil:
		case ErrNoChanDBExists, ErrNoActiveChannels, ErrChannelNotFound:
			return ErrNoCommitPoint
		default:
			return err
		}

		bs := chanBucket.Get(dataLossCommitPointKey)
		if bs == nil {
			return ErrNoCommitPoint
		}
		r := bytes.NewReader(bs)
		if err := ReadElements(r, &commitPoint); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return commitPoint, nil
}

// MarkBorked marks the event when the channel as reached an irreconcilable
// state, such as a channel breach or state desynchronization. Borked channels
// should never be added to the switch.
func (c *OpenChannel) MarkBorked() error {
	c.Lock()
	defer c.Unlock()

	return c.putChanStatus(Borked)
}

// MarkCommitmentBroadcasted marks the channel as a commitment transaction has
// been broadcast, either our own or the remote, and we should watch the chain
// for it to confirm before taking any further action.
func (c *OpenChannel) MarkCommitmentBroadcasted() error {
	c.Lock()
	defer c.Unlock()

	return c.putChanStatus(CommitmentBroadcasted)
}

func (c *OpenChannel) putChanStatus(status ChannelStatus) error {
	if err := c.Db.Update(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		channel, err := fetchOpenChannel(chanBucket, &c.FundingOutpoint)
		if err != nil {
			return err
		}

		// Add this status to the existing bitvector found in the DB.
		status = channel.chanStatus | status
		channel.chanStatus = status

		return putOpenChannel(chanBucket, channel)
	}); err != nil {
		return err
	}

	// Update the in-memory representation to keep it in sync with the DB.
	c.chanStatus = status

	return nil
}

// putChannel serializes, and stores the current state of the channel in its
// entirety.
func putOpenChannel(chanBucket *bolt.Bucket, channel *OpenChannel) error {
	// First, we'll write out all the relatively static fields, that are
	// decided upon initial channel creation.
	if err := putChanInfo(chanBucket, channel); err != nil {
		return fmt.Errorf("unable to store chan info: %v", err)
	}

	// With the static channel info written out, we'll now write out the
	// current commitment state for both parties.
	if err := putChanCommitments(chanBucket, channel); err != nil {
		return fmt.Errorf("unable to store chan commitments: %v", err)
	}

	// Finally, we'll write out the revocation state for both parties
	// within a distinct key space.
	if err := putChanRevocationState(chanBucket, channel); err != nil {
		return fmt.Errorf("unable to store chan revocations: %v", err)
	}

	return nil
}

// fetchOpenChannel retrieves, and deserializes (including decrypting
// sensitive) the complete channel currently active with the passed nodeID.
func fetchOpenChannel(chanBucket *bolt.Bucket,
	chanPoint *wire.OutPoint) (*OpenChannel, error) {

	channel := &OpenChannel{
		FundingOutpoint: *chanPoint,
	}

	// First, we'll read all the static information that changes less
	// frequently from disk.
	if err := fetchChanInfo(chanBucket, channel); err != nil {
		return nil, fmt.Errorf("unable to fetch chan info: %v", err)
	}

	// With the static information read, we'll now read the current
	// commitment state for both sides of the channel.
	if err := fetchChanCommitments(chanBucket, channel); err != nil {
		return nil, fmt.Errorf("unable to fetch chan commitments: %v", err)
	}

	// Finally, we'll retrieve the current revocation state so we can
	// properly
	if err := fetchChanRevocationState(chanBucket, channel); err != nil {
		return nil, fmt.Errorf("unable to fetch chan revocations: %v", err)
	}

	channel.Packager = NewChannelPackager(channel.ShortChannelID)

	return channel, nil
}

// SyncPending writes the contents of the channel to the database while it's in
// the pending (waiting for funding confirmation) state. The IsPending flag
// will be set to true. When the channel's funding transaction is confirmed,
// the channel should be marked as "open" and the IsPending flag set to false.
// Note that this function also creates a LinkNode relationship between this
// newly created channel and a new LinkNode instance. This allows listing all
// channels in the database globally, or according to the LinkNode they were
// created with.
//
// TODO(roasbeef): addr param should eventually be an lnwire.NetAddress type
// that includes service bits.
func (c *OpenChannel) SyncPending(addr net.Addr, pendingHeight uint32) error {
	c.Lock()
	defer c.Unlock()

	c.FundingBroadcastHeight = pendingHeight

	return c.Db.Update(func(tx *bolt.Tx) error {
		// First, sync all the persistent channel state to disk.
		if err := c.fullSync(tx); err != nil {
			return err
		}

		nodeInfoBucket, err := tx.CreateBucketIfNotExists(nodeInfoBucket)
		if err != nil {
			return err
		}

		// If a LinkNode for this identity public key already exists,
		// then we can exit early.
		nodePub := c.IdentityPub.SerializeCompressed()
		if nodeInfoBucket.Get(nodePub) != nil {
			return nil
		}

		// Next, we need to establish a (possibly) new LinkNode
		// relationship for this channel. The LinkNode metadata
		// contains reachability, up-time, and service bits related
		// information.
		linkNode := c.Db.NewLinkNode(wire.MainNet, c.IdentityPub, addr)

		// TODO(roasbeef): do away with link node all together?

		return putLinkNode(nodeInfoBucket, linkNode)
	})
}

// UpdateCommitment updates the commitment state for the specified party
// (remote or local). The commitment stat completely describes the balance
// state at this point in the commitment chain. This method its to be called on
// two occasions: when we revoke our prior commitment state, and when the
// remote party revokes their prior commitment state.
func (c *OpenChannel) UpdateCommitment(newCommitment *ChannelCommitment) error {
	c.Lock()
	defer c.Unlock()

	err := c.Db.Update(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		if err = putChanInfo(chanBucket, c); err != nil {
			return fmt.Errorf("unable to store chan info: %v", err)
		}

		// With the proper bucket fetched, we'll now write toe latest
		// commitment state to dis for the target party.
		err = putChanCommitment(chanBucket, newCommitment, true)
		if err != nil {
			return fmt.Errorf("unable to store chan "+
				"revocations: %v", err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	c.LocalCommitment = *newCommitment

	return nil
}

// HTLC is the on-disk representation of a hash time-locked contract. HTLCs are
// contained within ChannelDeltas which encode the current state of the
// commitment between state updates.
//
// TODO(roasbeef): save space by using smaller ints at tail end?
type HTLC struct {
	// Signature is the signature for the second level covenant transaction
	// for this HTLC. The second level transaction is a timeout tx in the
	// case that this is an outgoing HTLC, and a success tx in the case
	// that this is an incoming HTLC.
	//
	// TODO(roasbeef): make [64]byte instead?
	Signature []byte

	// RHash is the payment hash of the HTLC.
	RHash [32]byte

	// Amt is the amount of milli-satoshis this HTLC escrows.
	Amt lnwire.MilliSatoshi

	// RefundTimeout is the absolute timeout on the HTLC that the sender
	// must wait before reclaiming the funds in limbo.
	RefundTimeout uint32

	// OutputIndex is the output index for this particular HTLC output
	// within the commitment transaction.
	OutputIndex int32

	// Incoming denotes whether we're the receiver or the sender of this
	// HTLC.
	Incoming bool

	// OnionBlob is an opaque blob which is used to complete multi-hop
	// routing.
	OnionBlob []byte

	// HtlcIndex is the HTLC counter index of this active, outstanding
	// HTLC. This differs from the LogIndex, as the HtlcIndex is only
	// incremented for each offered HTLC, while they LogIndex is
	// incremented for each update (includes settle+fail).
	HtlcIndex uint64

	// LogIndex is the cumulative log index of this HTLC. This differs
	// from the HtlcIndex as this will be incremented for each new log
	// update added.
	LogIndex uint64
}

// SerializeHtlcs writes out the passed set of HTLC's into the passed writer
// using the current default on-disk serialization format.
//
// NOTE: This API is NOT stable, the on-disk format will likely change in the
// future.
func SerializeHtlcs(b io.Writer, htlcs ...HTLC) error {
	numHtlcs := uint16(len(htlcs))
	if err := WriteElement(b, numHtlcs); err != nil {
		return err
	}

	for _, htlc := range htlcs {
		if err := WriteElements(b,
			htlc.Signature, htlc.RHash, htlc.Amt, htlc.RefundTimeout,
			htlc.OutputIndex, htlc.Incoming, htlc.OnionBlob[:],
			htlc.HtlcIndex, htlc.LogIndex,
		); err != nil {
			return err
		}
	}

	return nil
}

// DeserializeHtlcs attempts to read out a slice of HTLC's from the passed
// io.Reader. The bytes within the passed reader MUST have been previously
// written to using the SerializeHtlcs function.
//
// NOTE: This API is NOT stable, the on-disk format will likely change in the
// future.
func DeserializeHtlcs(r io.Reader) ([]HTLC, error) {
	var numHtlcs uint16
	if err := ReadElement(r, &numHtlcs); err != nil {
		return nil, err
	}

	var htlcs []HTLC
	if numHtlcs == 0 {
		return htlcs, nil
	}

	htlcs = make([]HTLC, numHtlcs)
	for i := uint16(0); i < numHtlcs; i++ {
		if err := ReadElements(r,
			&htlcs[i].Signature, &htlcs[i].RHash, &htlcs[i].Amt,
			&htlcs[i].RefundTimeout, &htlcs[i].OutputIndex,
			&htlcs[i].Incoming, &htlcs[i].OnionBlob,
			&htlcs[i].HtlcIndex, &htlcs[i].LogIndex,
		); err != nil {
			return htlcs, err
		}
	}

	return htlcs, nil
}

// Copy returns a full copy of the target HTLC.
func (h *HTLC) Copy() HTLC {
	clone := HTLC{
		Incoming:      h.Incoming,
		Amt:           h.Amt,
		RefundTimeout: h.RefundTimeout,
		OutputIndex:   h.OutputIndex,
	}
	copy(clone.Signature[:], h.Signature)
	copy(clone.RHash[:], h.RHash[:])

	return clone
}

// LogUpdate represents a pending update to the remote commitment chain. The
// log update may be an add, fail, or settle entry. We maintain this data in
// order to be able to properly retransmit our proposed
// state if necessary.
type LogUpdate struct {
	// LogIndex is the log index of this proposed commitment update entry.
	LogIndex uint64

	// UpdateMsg is the update message that was included within the our
	// local update log. The LogIndex value denotes the log index of this
	// update which will be used when restoring our local update log if
	// we're left with a dangling update on restart.
	UpdateMsg lnwire.Message
}

// Encode writes a log update to the provided io.Writer.
func (l *LogUpdate) Encode(w io.Writer) error {
	return WriteElements(w, l.LogIndex, l.UpdateMsg)
}

// Decode reads a log update from the provided io.Reader.
func (l *LogUpdate) Decode(r io.Reader) error {
	return ReadElements(r, &l.LogIndex, &l.UpdateMsg)
}

// CircuitKey is used by a channel to uniquely identify the HTLCs it receives
// from the switch, and is used to purge our in-memory state of HTLCs that have
// already been processed by a link. Two list of CircuitKeys are included in
// each CommitDiff to allow a link to determine which in-memory htlcs directed
// the opening and closing of circuits in the switch's circuit map.
type CircuitKey struct {
	// ChanID is the short chanid indicating the HTLC's origin.
	//
	// NOTE: It is fine for this value to be blank, as this indicates a
	// locally-sourced payment.
	ChanID lnwire.ShortChannelID

	// HtlcID is the unique htlc index predominately assigned by links,
	// though can also be assigned by switch in the case of locally-sourced
	// payments.
	HtlcID uint64
}

// SetBytes deserializes the given bytes into this CircuitKey.
func (k *CircuitKey) SetBytes(bs []byte) error {
	if len(bs) != 16 {
		return ErrInvalidCircuitKeyLen
	}

	k.ChanID = lnwire.NewShortChanIDFromInt(
		binary.BigEndian.Uint64(bs[:8]))
	k.HtlcID = binary.BigEndian.Uint64(bs[8:])

	return nil
}

// Bytes returns the serialized bytes for this circuit key.
func (k CircuitKey) Bytes() []byte {
	var bs = make([]byte, 16)
	binary.BigEndian.PutUint64(bs[:8], k.ChanID.ToUint64())
	binary.BigEndian.PutUint64(bs[8:], k.HtlcID)
	return bs
}

// Encode writes a CircuitKey to the provided io.Writer.
func (k *CircuitKey) Encode(w io.Writer) error {
	var scratch [16]byte
	binary.BigEndian.PutUint64(scratch[:8], k.ChanID.ToUint64())
	binary.BigEndian.PutUint64(scratch[8:], k.HtlcID)

	_, err := w.Write(scratch[:])
	return err
}

// Decode reads a CircuitKey from the provided io.Reader.
func (k *CircuitKey) Decode(r io.Reader) error {
	var scratch [16]byte

	if _, err := io.ReadFull(r, scratch[:]); err != nil {
		return err
	}
	k.ChanID = lnwire.NewShortChanIDFromInt(
		binary.BigEndian.Uint64(scratch[:8]))
	k.HtlcID = binary.BigEndian.Uint64(scratch[8:])

	return nil
}

// String returns a string representation of the CircuitKey.
func (k CircuitKey) String() string {
	return fmt.Sprintf("(Chan ID=%s, HTLC ID=%d)", k.ChanID, k.HtlcID)
}

// CommitDiff represents the delta needed to apply the state transition between
// two subsequent commitment states. Given state N and state N+1, one is able
// to apply the set of messages contained within the CommitDiff to N to arrive
// at state N+1. Each time a new commitment is extended, we'll write a new
// commitment (along with the full commitment state) to disk so we can
// re-transmit the state in the case of a connection loss or message drop.
type CommitDiff struct {
	// ChannelCommitment is the full commitment state that one would arrive
	// at by applying the set of messages contained in the UpdateDiff to
	// the prior accepted commitment.
	Commitment ChannelCommitment

	// LogUpdates is the set of messages sent prior to the commitment state
	// transition in question. Upon reconnection, if we detect that they
	// don't have the commitment, then we re-send this along with the
	// proper signature.
	LogUpdates []LogUpdate

	// CommitSig is the exact CommitSig message that should be sent after
	// the set of LogUpdates above has been retransmitted. The signatures
	// within this message should properly cover the new commitment state
	// and also the HTLC's within the new commitment state.
	CommitSig *lnwire.CommitSig

	// OpenedCircuitKeys is a set of unique identifiers for any downstream
	// Add packets included in this commitment txn. After a restart, this
	// set of htlcs is acked from the link's incoming mailbox to ensure
	// there isn't an attempt to re-add them to this commitment txn.
	OpenedCircuitKeys []CircuitKey

	// ClosedCircuitKeys records the unique identifiers for any settle/fail
	// packets that were resolved by this commitment txn. After a restart,
	// this is used to ensure those circuits are removed from the circuit
	// map, and the downstream packets in the link's mailbox are removed.
	ClosedCircuitKeys []CircuitKey

	// AddAcks specifies the locations (commit height, pkg index) of any
	// Adds that were failed/settled in this commit diff. This will ack
	// entries in *this* channel's forwarding packages.
	//
	// NOTE: This value is not serialized, it is used to atomically mark the
	// resolution of adds, such that they will not be reprocessed after a
	// restart.
	AddAcks []AddRef

	// SettleFailAcks specifies the locations (chan id, commit height, pkg
	// index) of any Settles or Fails that were locked into this commit
	// diff, and originate from *another* channel, i.e. the outgoing link.
	//
	// NOTE: This value is not serialized, it is used to atomically acks
	// settles and fails from the forwarding packages of other channels,
	// such that they will not be reforwarded internally after a restart.
	SettleFailAcks []SettleFailRef
}

func serializeCommitDiff(w io.Writer, diff *CommitDiff) error {
	if err := serializeChanCommit(w, &diff.Commitment); err != nil {
		return err
	}

	if err := diff.CommitSig.Encode(w, 0); err != nil {
		return err
	}

	numUpdates := uint16(len(diff.LogUpdates))
	if err := binary.Write(w, byteOrder, numUpdates); err != nil {
		return err
	}

	for _, diff := range diff.LogUpdates {
		err := WriteElements(w, diff.LogIndex, diff.UpdateMsg)
		if err != nil {
			return err
		}
	}

	numOpenRefs := uint16(len(diff.OpenedCircuitKeys))
	if err := binary.Write(w, byteOrder, numOpenRefs); err != nil {
		return err
	}

	for _, openRef := range diff.OpenedCircuitKeys {
		err := WriteElements(w, openRef.ChanID, openRef.HtlcID)
		if err != nil {
			return err
		}
	}

	numClosedRefs := uint16(len(diff.ClosedCircuitKeys))
	if err := binary.Write(w, byteOrder, numClosedRefs); err != nil {
		return err
	}

	for _, closedRef := range diff.ClosedCircuitKeys {
		err := WriteElements(w, closedRef.ChanID, closedRef.HtlcID)
		if err != nil {
			return err
		}
	}

	return nil
}

func deserializeCommitDiff(r io.Reader) (*CommitDiff, error) {
	var (
		d   CommitDiff
		err error
	)

	d.Commitment, err = deserializeChanCommit(r)
	if err != nil {
		return nil, err
	}

	d.CommitSig = &lnwire.CommitSig{}
	if err := d.CommitSig.Decode(r, 0); err != nil {
		return nil, err
	}

	var numUpdates uint16
	if err := binary.Read(r, byteOrder, &numUpdates); err != nil {
		return nil, err
	}

	d.LogUpdates = make([]LogUpdate, numUpdates)
	for i := 0; i < int(numUpdates); i++ {
		err := ReadElements(r,
			&d.LogUpdates[i].LogIndex, &d.LogUpdates[i].UpdateMsg,
		)
		if err != nil {
			return nil, err
		}
	}

	var numOpenRefs uint16
	if err := binary.Read(r, byteOrder, &numOpenRefs); err != nil {
		return nil, err
	}

	d.OpenedCircuitKeys = make([]CircuitKey, numOpenRefs)
	for i := 0; i < int(numOpenRefs); i++ {
		err := ReadElements(r,
			&d.OpenedCircuitKeys[i].ChanID,
			&d.OpenedCircuitKeys[i].HtlcID)
		if err != nil {
			return nil, err
		}
	}

	var numClosedRefs uint16
	if err := binary.Read(r, byteOrder, &numClosedRefs); err != nil {
		return nil, err
	}

	d.ClosedCircuitKeys = make([]CircuitKey, numClosedRefs)
	for i := 0; i < int(numClosedRefs); i++ {
		err := ReadElements(r,
			&d.ClosedCircuitKeys[i].ChanID,
			&d.ClosedCircuitKeys[i].HtlcID)
		if err != nil {
			return nil, err
		}
	}

	return &d, nil
}

// AppendRemoteCommitChain appends a new CommitDiff to the end of the
// commitment chain for the remote party. This method is to be used once we
// have prepared a new commitment state for the remote party, but before we
// transmit it to the remote party. The contents of the argument should be
// sufficient to retransmit the updates and signature needed to reconstruct the
// state in full, in the case that we need to retransmit.
func (c *OpenChannel) AppendRemoteCommitChain(diff *CommitDiff) error {
	c.Lock()
	defer c.Unlock()

	return c.Db.Update(func(tx *bolt.Tx) error {
		// First, we'll grab the writable bucket where this channel's
		// data resides.
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		// Any outgoing settles and fails necessarily have a
		// corresponding adds in this channel's forwarding packages.
		// Mark all of these as being fully processed in our forwarding
		// package, which prevents us from reprocessing them after
		// startup.
		err = c.Packager.AckAddHtlcs(tx, diff.AddAcks...)
		if err != nil {
			return err
		}

		// Additionally, we ack from any fails or settles that are
		// persisted in another channel's forwarding package. This
		// prevents the same fails and settles from being retransmitted
		// after restarts. The actual fail or settle we need to
		// propagate to the remote party is now in the commit diff.
		err = c.Packager.AckSettleFails(tx, diff.SettleFailAcks...)
		if err != nil {
			return err
		}

		// TODO(roasbeef): use seqno to derive key for later LCP

		// With the bucket retrieved, we'll now serialize the commit
		// diff itself, and write it to disk.
		var b bytes.Buffer
		if err := serializeCommitDiff(&b, diff); err != nil {
			return err
		}
		return chanBucket.Put(commitDiffKey, b.Bytes())
	})
}

// RemoteCommitChainTip returns the "tip" of the current remote commitment
// chain. This value will be non-nil iff, we've created a new commitment for
// the remote party that they haven't yet ACK'd. In this case, their commitment
// chain will have a length of two: their current unrevoked commitment, and
// this new pending commitment. Once they revoked their prior state, we'll swap
// these pointers, causing the tip and the tail to point to the same entry.
func (c *OpenChannel) RemoteCommitChainTip() (*CommitDiff, error) {
	var cd *CommitDiff
	err := c.Db.View(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		switch err {
		case nil:
		case ErrNoChanDBExists, ErrNoActiveChannels, ErrChannelNotFound:
			return ErrNoPendingCommit
		default:
			return err
		}

		tipBytes := chanBucket.Get(commitDiffKey)
		if tipBytes == nil {
			return ErrNoPendingCommit
		}

		tipReader := bytes.NewReader(tipBytes)
		dcd, err := deserializeCommitDiff(tipReader)
		if err != nil {
			return err
		}

		cd = dcd
		return nil
	})
	if err != nil {
		return nil, err
	}

	return cd, err
}

// InsertNextRevocation inserts the _next_ commitment point (revocation) into
// the database, and also modifies the internal RemoteNextRevocation attribute
// to point to the passed key. This method is to be using during final channel
// set up, _after_ the channel has been fully confirmed.
//
// NOTE: If this method isn't called, then the target channel won't be able to
// propose new states for the commitment state of the remote party.
func (c *OpenChannel) InsertNextRevocation(revKey *btcec.PublicKey) error {
	c.Lock()
	defer c.Unlock()

	c.RemoteNextRevocation = revKey

	err := c.Db.Update(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		return putChanRevocationState(chanBucket, c)
	})
	if err != nil {
		return err
	}

	return nil
}

// AdvanceCommitChainTail records the new state transition within an on-disk
// append-only log which records all state transitions by the remote peer. In
// the case of an uncooperative broadcast of a prior state by the remote peer,
// this log can be consulted in order to reconstruct the state needed to
// rectify the situation. This method will add the current commitment for the
// remote party to the revocation log, and promote the current pending
// commitment to the current remote commitment.
func (c *OpenChannel) AdvanceCommitChainTail(fwdPkg *FwdPkg) error {
	c.Lock()
	defer c.Unlock()

	var newRemoteCommit *ChannelCommitment

	err := c.Db.Update(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		// Persist the latest preimage state to disk as the remote peer
		// has just added to our local preimage store, and given us a
		// new pending revocation key.
		if err := putChanRevocationState(chanBucket, c); err != nil {
			return err
		}

		// With the current preimage producer/store state updated,
		// append a new log entry recording this the delta of this
		// state transition.
		//
		// TODO(roasbeef): could make the deltas relative, would save
		// space, but then tradeoff for more disk-seeks to recover the
		// full state.
		logKey := revocationLogBucket
		logBucket, err := chanBucket.CreateBucketIfNotExists(logKey)
		if err != nil {
			return err
		}

		// Before we append this revoked state to the revocation log,
		// we'll swap out what's currently the tail of the commit tip,
		// with the current locked-in commitment for the remote party.
		tipBytes := chanBucket.Get(commitDiffKey)
		tipReader := bytes.NewReader(tipBytes)
		newCommit, err := deserializeCommitDiff(tipReader)
		if err != nil {
			return err
		}
		err = putChanCommitment(chanBucket, &newCommit.Commitment, false)
		if err != nil {
			return err
		}
		if err := chanBucket.Delete(commitDiffKey); err != nil {
			return err
		}

		// With the commitment pointer swapped, we can now add the
		// revoked (prior) state to the revocation log.
		//
		// TODO(roasbeef): store less
		err = appendChannelLogEntry(logBucket, &c.RemoteCommitment)
		if err != nil {
			return err
		}

		// Lastly, we write the forwarding package to disk so that we
		// can properly recover from failures and reforward HTLCs that
		// have not received a corresponding settle/fail.
		if err := c.Packager.AddFwdPkg(tx, fwdPkg); err != nil {
			return err
		}

		newRemoteCommit = &newCommit.Commitment

		return nil
	})
	if err != nil {
		return err
	}

	// With the db transaction complete, we'll swap over the in-memory
	// pointer of the new remote commitment, which was previously the tip
	// of the commit chain.
	c.RemoteCommitment = *newRemoteCommit

	return nil
}

// NextLocalHtlcIndex returns the next unallocated local htlc index. To ensure
// this always returns the next index that has been not been allocated, this
// will first try to examine any pending commitments, before falling back to the
// last locked-in local commitment.
func (c *OpenChannel) NextLocalHtlcIndex() (uint64, error) {
	// First, load the most recent commit diff that we initiated for the
	// remote party. If no pending commit is found, this is not treated as
	// a critical error, since we can always fall back.
	pendingRemoteCommit, err := c.RemoteCommitChainTip()
	if err != nil && err != ErrNoPendingCommit {
		return 0, err
	}

	// If a pending commit was found, its local htlc index will be at least
	// as large as the one on our local commitment.
	if pendingRemoteCommit != nil {
		return pendingRemoteCommit.Commitment.LocalHtlcIndex, nil
	}

	// Otherwise, fallback to using the local htlc index of our commitment.
	return c.LocalCommitment.LocalHtlcIndex, nil
}

// LoadFwdPkgs scans the forwarding log for any packages that haven't been
// processed, and returns their deserialized log updates in map indexed by the
// remote commitment height at which the updates were locked in.
func (c *OpenChannel) LoadFwdPkgs() ([]*FwdPkg, error) {
	c.RLock()
	defer c.RUnlock()

	var fwdPkgs []*FwdPkg
	if err := c.Db.View(func(tx *bolt.Tx) error {
		var err error
		fwdPkgs, err = c.Packager.LoadFwdPkgs(tx)
		return err
	}); err != nil {
		return nil, err
	}

	return fwdPkgs, nil
}

// AckAddHtlcs updates the AckAddFilter containing any of the provided AddRefs
// indicating that a response to this Add has been committed to the remote party.
// Doing so will prevent these Add HTLCs from being reforwarded internally.
func (c *OpenChannel) AckAddHtlcs(addRefs ...AddRef) error {
	c.Lock()
	defer c.Unlock()

	return c.Db.Update(func(tx *bolt.Tx) error {
		return c.Packager.AckAddHtlcs(tx, addRefs...)
	})
}

// AckSettleFails updates the SettleFailFilter containing any of the provided
// SettleFailRefs, indicating that the response has been delivered to the
// incoming link, corresponding to a particular AddRef. Doing so will prevent
// the responses from being retransmitted internally.
func (c *OpenChannel) AckSettleFails(settleFailRefs ...SettleFailRef) error {
	c.Lock()
	defer c.Unlock()

	return c.Db.Update(func(tx *bolt.Tx) error {
		return c.Packager.AckSettleFails(tx, settleFailRefs...)
	})
}

// SetFwdFilter atomically sets the forwarding filter for the forwarding package
// identified by `height`.
func (c *OpenChannel) SetFwdFilter(height uint64, fwdFilter *PkgFilter) error {
	c.Lock()
	defer c.Unlock()

	return c.Db.Update(func(tx *bolt.Tx) error {
		return c.Packager.SetFwdFilter(tx, height, fwdFilter)
	})
}

// RemoveFwdPkg atomically removes a forwarding package specified by the remote
// commitment height.
//
// NOTE: This method should only be called on packages marked FwdStateCompleted.
func (c *OpenChannel) RemoveFwdPkg(height uint64) error {
	c.Lock()
	defer c.Unlock()

	return c.Db.Update(func(tx *bolt.Tx) error {
		return c.Packager.RemovePkg(tx, height)
	})
}

// RevocationLogTail returns the "tail", or the end of the current revocation
// log. This entry represents the last previous state for the remote node's
// commitment chain. The ChannelDelta returned by this method will always lag one state behind the most current (unrevoked) state of the remote node's
// commitment chain.
func (c *OpenChannel) RevocationLogTail() (*ChannelCommitment, error) {
	c.RLock()
	defer c.RUnlock()

	// If we haven't created any state updates yet, then we'll exit early as
	// there's nothing to be found on disk in the revocation bucket.
	if c.RemoteCommitment.CommitHeight == 0 {
		return nil, nil
	}

	var commit ChannelCommitment
	if err := c.Db.View(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		logBucket := chanBucket.Bucket(revocationLogBucket)
		if logBucket == nil {
			return ErrNoPastDeltas
		}

		// Once we have the bucket that stores the revocation log from
		// this channel, we'll jump to the _last_ key in bucket. As we
		// store the update number on disk in a big-endian format,
		// this will retrieve the latest entry.
		cursor := logBucket.Cursor()
		_, tailLogEntry := cursor.Last()
		logEntryReader := bytes.NewReader(tailLogEntry)

		// Once we have the entry, we'll decode it into the channel
		// delta pointer we created above.
		var dbErr error
		commit, dbErr = deserializeChanCommit(logEntryReader)
		if dbErr != nil {
			return dbErr
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return &commit, nil
}

// CommitmentHeight returns the current commitment height. The commitment
// height represents the number of updates to the commitment state to data.
// This value is always monotonically increasing. This method is provided in
// order to allow multiple instances of a particular open channel to obtain a
// consistent view of the number of channel updates to data.
func (c *OpenChannel) CommitmentHeight() (uint64, error) {
	c.RLock()
	defer c.RUnlock()

	var height uint64
	err := c.Db.View(func(tx *bolt.Tx) error {
		// Get the bucket dedicated to storing the metadata for open
		// channels.
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		commit, err := fetchChanCommitment(chanBucket, true)
		if err != nil {
			return err
		}

		height = commit.CommitHeight
		return nil
	})
	if err != nil {
		return 0, nil
	}

	return height, nil
}

// FindPreviousState scans through the append-only log in an attempt to recover
// the previous channel state indicated by the update number. This method is
// intended to be used for obtaining the relevant data needed to claim all
// funds rightfully spendable in the case of an on-chain broadcast of the
// commitment transaction.
func (c *OpenChannel) FindPreviousState(updateNum uint64) (*ChannelCommitment, error) {
	c.RLock()
	defer c.RUnlock()

	var commit ChannelCommitment
	err := c.Db.View(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		logBucket := chanBucket.Bucket(revocationLogBucket)
		if logBucket == nil {
			return ErrNoPastDeltas
		}

		c, err := fetchChannelLogEntry(logBucket, updateNum)
		if err != nil {
			return err
		}

		commit = c
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &commit, nil
}

// ClosureType is an enum like structure that details exactly _how_ a channel
// was closed. Three closure types are currently possible: none, cooperative,
// local force close, remote force close, and (remote) breach.
type ClosureType uint8

const (
	// CooperativeClose indicates that a channel has been closed
	// cooperatively.  This means that both channel peers were online and
	// signed a new transaction paying out the settled balance of the
	// contract.
	CooperativeClose ClosureType = 0

	// LocalForceClose indicates that we have unilaterally broadcast our
	// current commitment state on-chain.
	LocalForceClose ClosureType = 1

	// RemoteForceClose indicates that the remote peer has unilaterally
	// broadcast their current commitment state on-chain.
	RemoteForceClose ClosureType = 4

	// BreachClose indicates that the remote peer attempted to broadcast a
	// prior _revoked_ channel state.
	BreachClose ClosureType = 2

	// FundingCanceled indicates that the channel never was fully opened
	// before it was marked as closed in the database. This can happen if
	// we or the remote fail at some point during the opening workflow, or
	// we timeout waiting for the funding transaction to be confirmed.
	FundingCanceled ClosureType = 3

	// Abandoned indicates that the channel state was removed without
	// any further actions. This is intended to clean up unusable
	// channels during development.
	Abandoned ClosureType = 5
)

// ChannelCloseSummary contains the final state of a channel at the point it
// was closed. Once a channel is closed, all the information pertaining to that
// channel within the openChannelBucket is deleted, and a compact summary is
// put in place instead.
type ChannelCloseSummary struct {
	// ChanPoint is the outpoint for this channel's funding transaction,
	// and is used as a unique identifier for the channel.
	ChanPoint wire.OutPoint

	// ShortChanID encodes the exact location in the chain in which the
	// channel was initially confirmed. This includes: the block height,
	// transaction index, and the output within the target transaction.
	ShortChanID lnwire.ShortChannelID

	// ChainHash is the hash of the genesis block that this channel resides
	// within.
	ChainHash chainhash.Hash

	// ClosingTXID is the txid of the transaction which ultimately closed
	// this channel.
	ClosingTXID chainhash.Hash

	// RemotePub is the public key of the remote peer that we formerly had
	// a channel with.
	RemotePub *btcec.PublicKey

	// Capacity was the total capacity of the channel.
	Capacity btcutil.Amount

	// CloseHeight is the height at which the funding transaction was
	// spent.
	CloseHeight uint32

	// SettledBalance is our total balance settled balance at the time of
	// channel closure. This _does not_ include the sum of any outputs that
	// have been time-locked as a result of the unilateral channel closure.
	SettledBalance btcutil.Amount

	// TimeLockedBalance is the sum of all the time-locked outputs at the
	// time of channel closure. If we triggered the force closure of this
	// channel, then this value will be non-zero if our settled output is
	// above the dust limit. If we were on the receiving side of a channel
	// force closure, then this value will be non-zero if we had any
	// outstanding outgoing HTLC's at the time of channel closure.
	TimeLockedBalance btcutil.Amount

	// CloseType details exactly _how_ the channel was closed. Five closure
	// types are possible: cooperative, local force, remote force, breach
	// and funding canceled.
	CloseType ClosureType

	// IsPending indicates whether this channel is in the 'pending close'
	// state, which means the channel closing transaction has been
	// confirmed, but not yet been fully resolved. In the case of a channel
	// that has been cooperatively closed, it will go straight into the
	// fully resolved state as soon as the closing transaction has been
	// confirmed. However, for channel that have been force closed, they'll
	// stay marked as "pending" until _all_ the pending funds have been
	// swept.
	IsPending bool

	// RemoteCurrentRevocation is the current revocation for their
	// commitment transaction. However, since this the derived public key,
	// we don't yet have the private key so we aren't yet able to verify
	// that it's actually in the hash chain.
	RemoteCurrentRevocation *btcec.PublicKey

	// RemoteNextRevocation is the revocation key to be used for the *next*
	// commitment transaction we create for the local node. Within the
	// specification, this value is referred to as the
	// per-commitment-point.
	RemoteNextRevocation *btcec.PublicKey

	// LocalChanCfg is the channel configuration for the local node.
	LocalChanConfig ChannelConfig
}

// CloseChannel closes a previously active Lightning channel. Closing a channel
// entails deleting all saved state within the database concerning this
// channel. This method also takes a struct that summarizes the state of the
// channel at closing, this compact representation will be the only component
// of a channel left over after a full closing.
func (c *OpenChannel) CloseChannel(summary *ChannelCloseSummary) error {
	c.Lock()
	defer c.Unlock()

	return c.Db.Update(func(tx *bolt.Tx) error {
		openChanBucket := tx.Bucket(openChannelBucket)
		if openChanBucket == nil {
			return ErrNoChanDBExists
		}

		nodePub := c.IdentityPub.SerializeCompressed()
		nodeChanBucket := openChanBucket.Bucket(nodePub)
		if nodeChanBucket == nil {
			return ErrNoActiveChannels
		}

		chainBucket := nodeChanBucket.Bucket(c.ChainHash[:])
		if chainBucket == nil {
			return ErrNoActiveChannels
		}

		var chanPointBuf bytes.Buffer
		err := writeOutpoint(&chanPointBuf, &c.FundingOutpoint)
		if err != nil {
			return err
		}
		chanBucket := chainBucket.Bucket(chanPointBuf.Bytes())
		if chanBucket == nil {
			return ErrNoActiveChannels
		}

		// Before we delete the channel state, we'll read out the full
		// details, as we'll also store portions of this information
		// for record keeping.
		chanState, err := fetchOpenChannel(
			chanBucket, &c.FundingOutpoint,
		)
		if err != nil {
			return err
		}

		// Now that the index to this channel has been deleted, purge
		// the remaining channel metadata from the database.
		err = deleteOpenChannel(chanBucket, chanPointBuf.Bytes())
		if err != nil {
			return err
		}

		// With the base channel data deleted, attempt to delete the
		// information stored within the revocation log.
		logBucket := chanBucket.Bucket(revocationLogBucket)
		if logBucket != nil {
			err := wipeChannelLogEntries(logBucket)
			if err != nil {
				return err
			}
			err = chanBucket.DeleteBucket(revocationLogBucket)
			if err != nil {
				return err
			}
		}

		err = chainBucket.DeleteBucket(chanPointBuf.Bytes())
		if err != nil {
			return err
		}

		// Finally, create a summary of this channel in the closed
		// channel bucket for this node.
		return putChannelCloseSummary(
			tx, chanPointBuf.Bytes(), summary, chanState,
		)
	})
}

// ChannelSnapshot is a frozen snapshot of the current channel state. A
// snapshot is detached from the original channel that generated it, providing
// read-only access to the current or prior state of an active channel.
//
// TODO(roasbeef): remove all together? pretty much just commitment
type ChannelSnapshot struct {
	// RemoteIdentity is the identity public key of the remote node that we
	// are maintaining the open channel with.
	RemoteIdentity btcec.PublicKey

	// ChanPoint is the outpoint that created the channel. This output is
	// found within the funding transaction and uniquely identified the
	// channel on the resident chain.
	ChannelPoint wire.OutPoint

	// ChainHash is the genesis hash of the chain that the channel resides
	// within.
	ChainHash chainhash.Hash

	// Capacity is the total capacity of the channel.
	Capacity btcutil.Amount

	// TotalMSatSent is the total number of milli-satoshis we've sent
	// within this channel.
	TotalMSatSent lnwire.MilliSatoshi

	// TotalMSatReceived is the total number of milli-satoshis we've
	// received within this channel.
	TotalMSatReceived lnwire.MilliSatoshi

	// ChannelCommitment is the current up-to-date commitment for the
	// target channel.
	ChannelCommitment
}

// Snapshot returns a read-only snapshot of the current channel state. This
// snapshot includes information concerning the current settled balance within
// the channel, metadata detailing total flows, and any outstanding HTLCs.
func (c *OpenChannel) Snapshot() *ChannelSnapshot {
	c.RLock()
	defer c.RUnlock()

	localCommit := c.LocalCommitment
	snapshot := &ChannelSnapshot{
		RemoteIdentity:    *c.IdentityPub,
		ChannelPoint:      c.FundingOutpoint,
		Capacity:          c.Capacity,
		TotalMSatSent:     c.TotalMSatSent,
		TotalMSatReceived: c.TotalMSatReceived,
		ChainHash:         c.ChainHash,
		ChannelCommitment: ChannelCommitment{
			LocalBalance:  localCommit.LocalBalance,
			RemoteBalance: localCommit.RemoteBalance,
			CommitHeight:  localCommit.CommitHeight,
			CommitFee:     localCommit.CommitFee,
		},
	}

	// Copy over the current set of HTLCs to ensure the caller can't mutate
	// our internal state.
	snapshot.Htlcs = make([]HTLC, len(localCommit.Htlcs))
	for i, h := range localCommit.Htlcs {
		snapshot.Htlcs[i] = h.Copy()
	}

	return snapshot
}

// LatestCommitments returns the two latest commitments for both the local and
// remote party. These commitments are read from disk to ensure that only the
// latest fully committed state is returned. The first commitment returned is
// the local commitment, and the second returned is the remote commitment.
func (c *OpenChannel) LatestCommitments() (*ChannelCommitment, *ChannelCommitment, error) {
	err := c.Db.View(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		return fetchChanCommitments(chanBucket, c)
	})
	if err != nil {
		return nil, nil, err
	}

	return &c.LocalCommitment, &c.RemoteCommitment, nil
}

// RemoteRevocationStore returns the most up to date commitment version of the
// revocation storage tree for the remote party. This method can be used when
// acting on a possible contract breach to ensure, that the caller has the most
// up to date information required to deliver justice.
func (c *OpenChannel) RemoteRevocationStore() (shachain.Store, error) {
	err := c.Db.View(func(tx *bolt.Tx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		return fetchChanRevocationState(chanBucket, c)
	})
	if err != nil {
		return nil, err
	}

	return c.RevocationStore, nil
}

func putChannelCloseSummary(tx *bolt.Tx, chanID []byte,
	summary *ChannelCloseSummary, lastChanState *OpenChannel) error {

	closedChanBucket, err := tx.CreateBucketIfNotExists(closedChannelBucket)
	if err != nil {
		return err
	}

	summary.RemoteCurrentRevocation = lastChanState.RemoteCurrentRevocation
	summary.RemoteNextRevocation = lastChanState.RemoteNextRevocation
	summary.LocalChanConfig = lastChanState.LocalChanCfg

	var b bytes.Buffer
	if err := serializeChannelCloseSummary(&b, summary); err != nil {
		return err
	}

	return closedChanBucket.Put(chanID, b.Bytes())
}

func serializeChannelCloseSummary(w io.Writer, cs *ChannelCloseSummary) error {
	err := WriteElements(w,
		cs.ChanPoint, cs.ShortChanID, cs.ChainHash, cs.ClosingTXID,
		cs.CloseHeight, cs.RemotePub, cs.Capacity, cs.SettledBalance,
		cs.TimeLockedBalance, cs.CloseType, cs.IsPending,
	)
	if err != nil {
		return err
	}

	// If this is a close channel summary created before the addition of
	// the new fields, then we can exit here.
	if cs.RemoteCurrentRevocation == nil {
		return nil
	}

	if err := WriteElements(w, cs.RemoteCurrentRevocation); err != nil {
		return err
	}

	if err := writeChanConfig(w, &cs.LocalChanConfig); err != nil {
		return err
	}

	// We'll write this field last, as it's possible for a channel to be
	// closed before we learn of the next unrevoked revocation point for
	// the remote party.
	if cs.RemoteNextRevocation == nil {
		return nil
	}

	return WriteElements(w, cs.RemoteNextRevocation)
}

func fetchChannelCloseSummary(tx *bolt.Tx,
	chanID []byte) (*ChannelCloseSummary, error) {

	closedChanBucket, err := tx.CreateBucketIfNotExists(closedChannelBucket)
	if err != nil {
		return nil, err
	}

	summaryBytes := closedChanBucket.Get(chanID)
	if summaryBytes == nil {
		return nil, fmt.Errorf("closed channel summary not found")
	}

	summaryReader := bytes.NewReader(summaryBytes)
	return deserializeCloseChannelSummary(summaryReader)
}

func deserializeCloseChannelSummary(r io.Reader) (*ChannelCloseSummary, error) {
	c := &ChannelCloseSummary{}

	err := ReadElements(r,
		&c.ChanPoint, &c.ShortChanID, &c.ChainHash, &c.ClosingTXID,
		&c.CloseHeight, &c.RemotePub, &c.Capacity, &c.SettledBalance,
		&c.TimeLockedBalance, &c.CloseType, &c.IsPending,
	)
	if err != nil {
		return nil, err
	}

	// We'll now check to see if the channel close summary was encoded with
	// any of the additional optional fields.
	err = ReadElements(r, &c.RemoteCurrentRevocation)
	switch {
	case err == io.EOF:
		return c, nil

	// If we got a non-eof error, then we know there's an actually issue.
	// Otherwise, it may have been the case that this summary didn't have
	// the set of optional fields.
	case err != nil:
		return nil, err
	}

	if err := readChanConfig(r, &c.LocalChanConfig); err != nil {
		return nil, err
	}

	// Finally, we'll attempt to read the next unrevoked commitment point
	// for the remote party. If we closed the channel before receiving a
	// funding locked message, then this can be nil. As a result, we'll use
	// the same technique to read the field, only if there's still data
	// left in the buffer.
	err = ReadElements(r, &c.RemoteNextRevocation)
	if err != nil && err != io.EOF {
		// If we got a non-eof error, then we know there's an actually
		// issue. Otherwise, it may have been the case that this
		// summary didn't have the set of optional fields.
		return nil, err
	}

	return c, nil
}

func writeChanConfig(b io.Writer, c *ChannelConfig) error {
	return WriteElements(b,
		c.DustLimit, c.MaxPendingAmount, c.ChanReserve, c.MinHTLC,
		c.MaxAcceptedHtlcs, c.CsvDelay, c.MultiSigKey,
		c.RevocationBasePoint, c.PaymentBasePoint, c.DelayBasePoint,
		c.HtlcBasePoint,
	)
}

func putChanInfo(chanBucket *bolt.Bucket, channel *OpenChannel) error {
	var w bytes.Buffer
	if err := WriteElements(&w,
		channel.ChanType, channel.ChainHash, channel.FundingOutpoint,
		channel.ShortChannelID, channel.IsPending, channel.IsInitiator,
		channel.chanStatus, channel.FundingBroadcastHeight,
		channel.NumConfsRequired, channel.ChannelFlags,
		channel.IdentityPub, channel.Capacity, channel.TotalMSatSent,
		channel.TotalMSatReceived,
	); err != nil {
		return err
	}

	// For single funder channels that we initiated, write the funding txn.
	if channel.ChanType == SingleFunder && channel.IsInitiator {
		if err := WriteElement(&w, channel.FundingTxn); err != nil {
			return err
		}
	}

	if err := writeChanConfig(&w, &channel.LocalChanCfg); err != nil {
		return err
	}
	if err := writeChanConfig(&w, &channel.RemoteChanCfg); err != nil {
		return err
	}

	return chanBucket.Put(chanInfoKey, w.Bytes())
}

func serializeChanCommit(w io.Writer, c *ChannelCommitment) error {
	if err := WriteElements(w,
		c.CommitHeight, c.LocalLogIndex, c.LocalHtlcIndex,
		c.RemoteLogIndex, c.RemoteHtlcIndex, c.LocalBalance,
		c.RemoteBalance, c.CommitFee, c.FeePerKw, c.CommitTx,
		c.CommitSig,
	); err != nil {
		return err
	}

	return SerializeHtlcs(w, c.Htlcs...)
}

func putChanCommitment(chanBucket *bolt.Bucket, c *ChannelCommitment,
	local bool) error {

	var commitKey []byte
	if local {
		commitKey = append(chanCommitmentKey, byte(0x00))
	} else {
		commitKey = append(chanCommitmentKey, byte(0x01))
	}

	var b bytes.Buffer
	if err := serializeChanCommit(&b, c); err != nil {
		return err
	}

	return chanBucket.Put(commitKey, b.Bytes())
}

func putChanCommitments(chanBucket *bolt.Bucket, channel *OpenChannel) error {
	err := putChanCommitment(chanBucket, &channel.LocalCommitment, true)
	if err != nil {
		return err
	}

	return putChanCommitment(chanBucket, &channel.RemoteCommitment, false)
}

func putChanRevocationState(chanBucket *bolt.Bucket, channel *OpenChannel) error {

	var b bytes.Buffer
	err := WriteElements(
		&b, channel.RemoteCurrentRevocation, channel.RevocationProducer,
		channel.RevocationStore,
	)
	if err != nil {
		return err
	}

	// TODO(roasbeef): don't keep producer on disk

	// If the next revocation is present, which is only the case after the
	// FundingLocked message has been sent, then we'll write it to disk.
	if channel.RemoteNextRevocation != nil {
		err = WriteElements(&b, channel.RemoteNextRevocation)
		if err != nil {
			return err
		}
	}

	return chanBucket.Put(revocationStateKey, b.Bytes())
}

func readChanConfig(b io.Reader, c *ChannelConfig) error {
	return ReadElements(b,
		&c.DustLimit, &c.MaxPendingAmount, &c.ChanReserve,
		&c.MinHTLC, &c.MaxAcceptedHtlcs, &c.CsvDelay,
		&c.MultiSigKey, &c.RevocationBasePoint,
		&c.PaymentBasePoint, &c.DelayBasePoint,
		&c.HtlcBasePoint,
	)
}

func fetchChanInfo(chanBucket *bolt.Bucket, channel *OpenChannel) error {
	infoBytes := chanBucket.Get(chanInfoKey)
	if infoBytes == nil {
		return ErrNoChanInfoFound
	}
	r := bytes.NewReader(infoBytes)

	if err := ReadElements(r,
		&channel.ChanType, &channel.ChainHash, &channel.FundingOutpoint,
		&channel.ShortChannelID, &channel.IsPending, &channel.IsInitiator,
		&channel.chanStatus, &channel.FundingBroadcastHeight,
		&channel.NumConfsRequired, &channel.ChannelFlags,
		&channel.IdentityPub, &channel.Capacity, &channel.TotalMSatSent,
		&channel.TotalMSatReceived,
	); err != nil {
		return err
	}

	// For single funder channels that we initiated, read the funding txn.
	if channel.ChanType == SingleFunder && channel.IsInitiator {
		if err := ReadElement(r, &channel.FundingTxn); err != nil {
			return err
		}
	}

	if err := readChanConfig(r, &channel.LocalChanCfg); err != nil {
		return err
	}
	if err := readChanConfig(r, &channel.RemoteChanCfg); err != nil {
		return err
	}

	channel.Packager = NewChannelPackager(channel.ShortChannelID)

	return nil
}

func deserializeChanCommit(r io.Reader) (ChannelCommitment, error) {
	var c ChannelCommitment

	err := ReadElements(r,
		&c.CommitHeight, &c.LocalLogIndex, &c.LocalHtlcIndex, &c.RemoteLogIndex,
		&c.RemoteHtlcIndex, &c.LocalBalance, &c.RemoteBalance,
		&c.CommitFee, &c.FeePerKw, &c.CommitTx, &c.CommitSig,
	)
	if err != nil {
		return c, err
	}

	c.Htlcs, err = DeserializeHtlcs(r)
	if err != nil {
		return c, err
	}

	return c, nil
}

func fetchChanCommitment(chanBucket *bolt.Bucket, local bool) (ChannelCommitment, error) {
	var commitKey []byte
	if local {
		commitKey = append(chanCommitmentKey, byte(0x00))
	} else {
		commitKey = append(chanCommitmentKey, byte(0x01))
	}

	commitBytes := chanBucket.Get(commitKey)
	if commitBytes == nil {
		return ChannelCommitment{}, ErrNoCommitmentsFound
	}

	r := bytes.NewReader(commitBytes)
	return deserializeChanCommit(r)
}

func fetchChanCommitments(chanBucket *bolt.Bucket, channel *OpenChannel) error {
	var err error

	channel.LocalCommitment, err = fetchChanCommitment(chanBucket, true)
	if err != nil {
		return err
	}
	channel.RemoteCommitment, err = fetchChanCommitment(chanBucket, false)
	if err != nil {
		return err
	}

	return nil
}

func fetchChanRevocationState(chanBucket *bolt.Bucket, channel *OpenChannel) error {
	revBytes := chanBucket.Get(revocationStateKey)
	if revBytes == nil {
		return ErrNoRevocationsFound
	}
	r := bytes.NewReader(revBytes)

	err := ReadElements(
		r, &channel.RemoteCurrentRevocation, &channel.RevocationProducer,
		&channel.RevocationStore,
	)
	if err != nil {
		return err
	}

	// If there aren't any bytes left in the buffer, then we don't yet have
	// the next remote revocation, so we can exit early here.
	if r.Len() == 0 {
		return nil
	}

	// Otherwise we'll read the next revocation for the remote party which
	// is always the last item within the buffer.
	return ReadElements(r, &channel.RemoteNextRevocation)
}

func deleteOpenChannel(chanBucket *bolt.Bucket, chanPointBytes []byte) error {

	if err := chanBucket.Delete(chanInfoKey); err != nil {
		return err
	}

	err := chanBucket.Delete(append(chanCommitmentKey, byte(0x00)))
	if err != nil {
		return err
	}
	err = chanBucket.Delete(append(chanCommitmentKey, byte(0x01)))
	if err != nil {
		return err
	}

	if err := chanBucket.Delete(revocationStateKey); err != nil {
		return err
	}

	if diff := chanBucket.Get(commitDiffKey); diff != nil {
		return chanBucket.Delete(commitDiffKey)
	}

	return nil

}

// makeLogKey converts a uint64 into an 8 byte array.
func makeLogKey(updateNum uint64) [8]byte {
	var key [8]byte
	byteOrder.PutUint64(key[:], updateNum)
	return key
}

// readLogKey parse the first 8- bytes of a byte slice into a uint64.
//
// NOTE: The slice must be at least 8 bytes long.
func readLogKey(b []byte) uint64 {
	return byteOrder.Uint64(b)
}

func appendChannelLogEntry(log *bolt.Bucket,
	commit *ChannelCommitment) error {

	var b bytes.Buffer
	if err := serializeChanCommit(&b, commit); err != nil {
		return err
	}

	logEntrykey := makeLogKey(commit.CommitHeight)
	return log.Put(logEntrykey[:], b.Bytes())
}

func fetchChannelLogEntry(log *bolt.Bucket,
	updateNum uint64) (ChannelCommitment, error) {

	logEntrykey := makeLogKey(updateNum)
	commitBytes := log.Get(logEntrykey[:])
	if commitBytes == nil {
		return ChannelCommitment{}, fmt.Errorf("log entry not found")
	}

	commitReader := bytes.NewReader(commitBytes)
	return deserializeChanCommit(commitReader)
}

func wipeChannelLogEntries(log *bolt.Bucket) error {
	// TODO(roasbeef): comment

	logCursor := log.Cursor()
	for k, _ := logCursor.First(); k != nil; k, _ = logCursor.Next() {
		if err := logCursor.Delete(); err != nil {
			return err
		}
	}

	return nil
}
