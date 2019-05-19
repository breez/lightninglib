package sweep

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/breez/lightninglib/channeldb"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

// makeTestDB creates a new instance of the ChannelDB for testing purposes. A
// callback which cleans up the created temporary directories is also returned
// and intended to be executed after the test completes.
func makeTestDB() (*channeldb.DB, func(), error) {
	// First, create a temporary directory to be used for the duration of
	// this test.
	tempDirName, err := ioutil.TempDir("", "channeldb")
	if err != nil {
		return nil, nil, err
	}

	// Next, create channeldb for the first time.
	cdb, err := channeldb.Open(tempDirName)
	if err != nil {
		return nil, nil, err
	}

	cleanUp := func() {
		cdb.Close()
		os.RemoveAll(tempDirName)
	}

	return cdb, cleanUp, nil
}

// TestStore asserts that the store persists the presented data to disk and is
// able to retrieve it again.
func TestStore(t *testing.T) {
	t.Run("bolt", func(t *testing.T) {

		// Create new store.
		cdb, cleanUp, err := makeTestDB()
		if err != nil {
			t.Fatalf("unable to open channel db: %v", err)
		}
		defer cleanUp()

		if err != nil {
			t.Fatal(err)
		}

		testStore(t, func() (SweeperStore, error) {
			var chain chainhash.Hash
			return NewSweeperStore(cdb, &chain)
		})
	})
	t.Run("mock", func(t *testing.T) {
		store := NewMockSweeperStore()

		testStore(t, func() (SweeperStore, error) {
			// Return same store, because the mock has no real
			// persistence.
			return store, nil
		})
	})
}

func testStore(t *testing.T, createStore func() (SweeperStore, error)) {
	store, err := createStore()
	if err != nil {
		t.Fatal(err)
	}

	// Initially we expect the store not to have a last published tx.
	retrievedTx, err := store.GetLastPublishedTx()
	if err != nil {
		t.Fatal(err)
	}
	if retrievedTx != nil {
		t.Fatal("expected no last published tx")
	}

	// Notify publication of tx1
	tx1 := wire.MsgTx{}
	tx1.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Index: 1,
		},
	})

	err = store.NotifyPublishTx(&tx1)
	if err != nil {
		t.Fatal(err)
	}

	// Notify publication of tx2
	tx2 := wire.MsgTx{}
	tx2.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Index: 2,
		},
	})

	err = store.NotifyPublishTx(&tx2)
	if err != nil {
		t.Fatal(err)
	}

	// Recreate the sweeper store
	store, err = createStore()
	if err != nil {
		t.Fatal(err)
	}

	// Assert that last published tx2 is present.
	retrievedTx, err = store.GetLastPublishedTx()
	if err != nil {
		t.Fatal(err)
	}

	if tx2.TxHash() != retrievedTx.TxHash() {
		t.Fatal("txes do not match")
	}

	// Assert that both txes are recognized as our own.
	ours, err := store.IsOurTx(tx1.TxHash())
	if err != nil {
		t.Fatal(err)
	}
	if !ours {
		t.Fatal("expected tx to be ours")
	}

	ours, err = store.IsOurTx(tx2.TxHash())
	if err != nil {
		t.Fatal(err)
	}
	if !ours {
		t.Fatal("expected tx to be ours")
	}

	// An different hash should be reported on as not being ours.
	var unknownHash chainhash.Hash
	ours, err = store.IsOurTx(unknownHash)
	if err != nil {
		t.Fatal(err)
	}
	if ours {
		t.Fatal("expected tx to be not ours")
	}
}
