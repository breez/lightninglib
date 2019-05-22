package wtclient

import (
	"net"

	"github.com/breez/lightninglib/brontide"
	"github.com/breez/lightninglib/keychain"
	"github.com/breez/lightninglib/lnwire"
	"github.com/breez/lightninglib/watchtower/wtdb"
	"github.com/breez/lightninglib/watchtower/wtserver"
	"github.com/btcsuite/btcd/btcec"
)

// DB abstracts the required database operations required by the watchtower
// client.
type DB interface {
	// CreateTower initialize an address record used to communicate with a
	// watchtower. Each Tower is assigned a unique ID, that is used to
	// amortize storage costs of the public key when used by multiple
	// sessions.
	CreateTower(*lnwire.NetAddress) (*wtdb.Tower, error)

	// LoadTower retrieves a tower by its tower ID.
	LoadTower(uint64) (*wtdb.Tower, error)

	// NextSessionKeyIndex reserves a new session key derivation index for a
	// particular tower id. The index is reserved for that tower until
	// CreateClientSession is invoked for that tower and index, at which
	// point a new index for that tower can be reserved. Multiple calls to
	// this method before CreateClientSession is invoked should return the
	// same index.
	NextSessionKeyIndex(uint64) (uint32, error)

	// CreateClientSession saves a newly negotiated client session to the
	// client's database. This enables the session to be used across
	// restarts.
	CreateClientSession(*wtdb.ClientSession) error

	// ListClientSessions returns all sessions that have not yet been
	// exhausted. This is used on startup to find any sessions which may
	// still be able to accept state updates.
	ListClientSessions() (map[wtdb.SessionID]*wtdb.ClientSession, error)

	// FetchChanPkScripts returns a map of all sweep pkscripts for
	// registered channels. This is used on startup to cache the sweep
	// pkscripts of registered channels in memory.
	FetchChanPkScripts() (map[lnwire.ChannelID][]byte, error)

	// AddChanPkScript inserts a newly generated sweep pkscript for the
	// given channel.
	AddChanPkScript(lnwire.ChannelID, []byte) error

	// MarkBackupIneligible records that the state identified by the
	// (channel id, commit height) tuple was ineligible for being backed up
	// under the current policy. This state can be retried later under a
	// different policy.
	MarkBackupIneligible(chanID lnwire.ChannelID, commitHeight uint64) error

	// CommitUpdate writes the next state update for a particular
	// session, so that we can be sure to resend it after a restart if it
	// hasn't been ACK'd by the tower. The sequence number of the update
	// should be exactly one greater than the existing entry, and less that
	// or equal to the session's MaxUpdates.
	CommitUpdate(id *wtdb.SessionID, seqNum uint16,
		update *wtdb.CommittedUpdate) (uint16, error)

	// AckUpdate records an acknowledgment from the watchtower that the
	// update identified by seqNum was received and saved. The returned
	// lastApplied will be recorded.
	AckUpdate(id *wtdb.SessionID, seqNum, lastApplied uint16) error
}

// Dial connects to an addr using the specified net and returns the connection
// object.
type Dial func(net, addr string) (net.Conn, error)

// AuthDialer connects to a remote node using an authenticated transport, such as
// brontide. The dialer argument is used to specify a resolver, which allows
// this method to be used over Tor or clear net connections.
type AuthDialer func(localPriv *btcec.PrivateKey, netAddr *lnwire.NetAddress,
	dialer func(string, string) (net.Conn, error)) (wtserver.Peer, error)

// AuthDial is the watchtower client's default method of dialing.
func AuthDial(localPriv *btcec.PrivateKey, netAddr *lnwire.NetAddress,
	dialer func(string, string) (net.Conn, error)) (wtserver.Peer, error) {

	return brontide.Dial(localPriv, netAddr, dialer)
}

// SecretKeyRing abstracts the ability to derive HD private keys given a
// description of the derivation path.
type SecretKeyRing interface {
	// DerivePrivKey derives the private key from the root seed using a
	// key descriptor specifying the key's derivation path.
	DerivePrivKey(loc keychain.KeyDescriptor) (*btcec.PrivateKey, error)
}
