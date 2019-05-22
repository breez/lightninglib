package wtmock

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/breez/lightninglib/lnwire"
	"github.com/breez/lightninglib/watchtower/wtdb"
)

type towerPK [33]byte

// ClientDB is a mock, in-memory database or testing the watchtower client
// behavior.
type ClientDB struct {
	nextTowerID uint64 // to be used atomically

	mu             sync.Mutex
	sweepPkScripts map[lnwire.ChannelID][]byte
	activeSessions map[wtdb.SessionID]*wtdb.ClientSession
	towerIndex     map[towerPK]uint64
	towers         map[uint64]*wtdb.Tower

	nextIndex uint32
	indexes   map[uint64]uint32
}

// NewClientDB initializes a new mock ClientDB.
func NewClientDB() *ClientDB {
	return &ClientDB{
		sweepPkScripts: make(map[lnwire.ChannelID][]byte),
		activeSessions: make(map[wtdb.SessionID]*wtdb.ClientSession),
		towerIndex:     make(map[towerPK]uint64),
		towers:         make(map[uint64]*wtdb.Tower),
		indexes:        make(map[uint64]uint32),
	}
}

// CreateTower initializes a database entry with the given lightning address. If
// the tower exists, the address is append to the list of all addresses used to
// that tower previously.
func (m *ClientDB) CreateTower(lnAddr *lnwire.NetAddress) (*wtdb.Tower, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var towerPubKey towerPK
	copy(towerPubKey[:], lnAddr.IdentityKey.SerializeCompressed())

	var tower *wtdb.Tower
	towerID, ok := m.towerIndex[towerPubKey]
	if ok {
		tower = m.towers[towerID]
		tower.AddAddress(lnAddr.Address)
	} else {
		towerID = atomic.AddUint64(&m.nextTowerID, 1)
		tower = &wtdb.Tower{
			ID:          towerID,
			IdentityKey: lnAddr.IdentityKey,
			Addresses:   []net.Addr{lnAddr.Address},
		}
	}

	m.towerIndex[towerPubKey] = towerID
	m.towers[towerID] = tower

	return tower, nil
}

// LoadTower retrieves a tower by its tower ID.
func (m *ClientDB) LoadTower(towerID uint64) (*wtdb.Tower, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if tower, ok := m.towers[towerID]; ok {
		return tower, nil
	}

	return nil, wtdb.ErrTowerNotFound
}

// MarkBackupIneligible records that particular commit height is ineligible for
// backup. This allows the client to track which updates it should not attempt
// to retry after startup.
func (m *ClientDB) MarkBackupIneligible(chanID lnwire.ChannelID, commitHeight uint64) error {
	return nil
}

// ListClientSessions returns the set of all client sessions known to the db.
func (m *ClientDB) ListClientSessions() (map[wtdb.SessionID]*wtdb.ClientSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sessions := make(map[wtdb.SessionID]*wtdb.ClientSession)
	for _, session := range m.activeSessions {
		sessions[session.ID] = session
	}

	return sessions, nil
}

// CreateClientSession records a newly negotiated client session in the set of
// active sessions. The session can be identified by its SessionID.
func (m *ClientDB) CreateClientSession(session *wtdb.ClientSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Ensure that a session key index has been reserved for this tower.
	keyIndex, ok := m.indexes[session.TowerID]
	if !ok {
		return wtdb.ErrNoReservedKeyIndex
	}

	// Ensure that the session's index matches the reserved index.
	if keyIndex != session.KeyIndex {
		return wtdb.ErrIncorrectKeyIndex
	}

	// Remove the key index reservation for this tower. Once committed, this
	// permits us to create another session with this tower.
	delete(m.indexes, session.TowerID)

	m.activeSessions[session.ID] = &wtdb.ClientSession{
		TowerID:          session.TowerID,
		KeyIndex:         session.KeyIndex,
		ID:               session.ID,
		Policy:           session.Policy,
		SeqNum:           session.SeqNum,
		TowerLastApplied: session.TowerLastApplied,
		RewardPkScript:   cloneBytes(session.RewardPkScript),
		CommittedUpdates: make(map[uint16]*wtdb.CommittedUpdate),
		AckedUpdates:     make(map[uint16]wtdb.BackupID),
	}

	return nil
}

// NextSessionKeyIndex reserves a new session key derivation index for a
// particular tower id. The index is reserved for that tower until
// CreateClientSession is invoked for that tower and index, at which point a new
// index for that tower can be reserved. Multiple calls to this method before
// CreateClientSession is invoked should return the same index.
func (m *ClientDB) NextSessionKeyIndex(towerID uint64) (uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if index, ok := m.indexes[towerID]; ok {
		return index, nil
	}

	index := m.nextIndex
	m.indexes[towerID] = index

	m.nextIndex++

	return index, nil
}

// CommitUpdate persists the CommittedUpdate provided in the slot for (session,
// seqNum). This allows the client to retransmit this update on startup.
func (m *ClientDB) CommitUpdate(id *wtdb.SessionID, seqNum uint16,
	update *wtdb.CommittedUpdate) (uint16, error) {

	m.mu.Lock()
	defer m.mu.Unlock()

	// Fail if session doesn't exist.
	session, ok := m.activeSessions[*id]
	if !ok {
		return 0, wtdb.ErrClientSessionNotFound
	}

	// Check if an update has already been committed for this state.
	dbUpdate, ok := session.CommittedUpdates[seqNum]
	if ok {
		// If the breach hint matches, we'll just return the last
		// applied value so the client can retransmit.
		if dbUpdate.Hint == update.Hint {
			return session.TowerLastApplied, nil
		}

		// Otherwise, fail since the breach hint doesn't match.
		return 0, wtdb.ErrUpdateAlreadyCommitted
	}

	// Sequence number must increment.
	if seqNum != session.SeqNum+1 {
		return 0, wtdb.ErrCommitUnorderedUpdate
	}

	// Save the update and increment the sequence number.
	session.CommittedUpdates[seqNum] = update
	session.SeqNum++

	return session.TowerLastApplied, nil
}

// AckUpdate persists an acknowledgment for a given (session, seqnum) pair. This
// removes the update from the set of committed updates, and validates the
// lastApplied value returned from the tower.
func (m *ClientDB) AckUpdate(id *wtdb.SessionID, seqNum, lastApplied uint16) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Fail if session doesn't exist.
	session, ok := m.activeSessions[*id]
	if !ok {
		return wtdb.ErrClientSessionNotFound
	}

	// Retrieve the committed update, failing if none is found. We should
	// only receive acks for state updates that we send.
	update, ok := session.CommittedUpdates[seqNum]
	if !ok {
		return wtdb.ErrCommittedUpdateNotFound
	}

	// Ensure the returned last applied value does not exceed the highest
	// allocated sequence number.
	if lastApplied > session.SeqNum {
		return wtdb.ErrUnallocatedLastApplied
	}

	// Ensure the last applied value isn't lower than a previous one sent by
	// the tower.
	if lastApplied < session.TowerLastApplied {
		return wtdb.ErrLastAppliedReversion
	}

	// Finally, remove the committed update from disk and mark the update as
	// acked. The tower last applied value is also recorded to send along
	// with the next update.
	delete(session.CommittedUpdates, seqNum)
	session.AckedUpdates[seqNum] = update.BackupID
	session.TowerLastApplied = lastApplied

	return nil
}

// FetchChanPkScripts returns the set of sweep pkscripts known for all channels.
// This allows the client to cache them in memory on startup.
func (m *ClientDB) FetchChanPkScripts() (map[lnwire.ChannelID][]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sweepPkScripts := make(map[lnwire.ChannelID][]byte)
	for chanID, pkScript := range m.sweepPkScripts {
		sweepPkScripts[chanID] = cloneBytes(pkScript)
	}

	return sweepPkScripts, nil
}

// AddChanPkScript sets a pkscript or sweeping funds from the channel or chanID.
func (m *ClientDB) AddChanPkScript(chanID lnwire.ChannelID, pkScript []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.sweepPkScripts[chanID]; ok {
		return fmt.Errorf("pkscript for %x already exists", pkScript)
	}

	m.sweepPkScripts[chanID] = cloneBytes(pkScript)

	return nil
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}

	bb := make([]byte, len(b))
	copy(bb, b)

	return bb
}
