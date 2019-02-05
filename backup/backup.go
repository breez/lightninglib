package backup

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/breez/lightninglib/channeldb"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcwallet/walletdb"
	bolt "github.com/coreos/bbolt"
)

const (
	txMaxSize = 65536
)

var (
	// Namespace keys.
	waddrmgrNamespace = []byte("waddrmgr")
	syncBucketName    = []byte("sync")

	// Sync related key names (sync bucket).
	syncedToName   = []byte("syncedto")
	startBlockName = []byte("startblock")
	birthdayName   = []byte("birthday")
)

func Backup(chainParams *chaincfg.Params, channelDB *channeldb.DB, walletDB walletdb.DB) ([]string, error) {

	dir, err := ioutil.TempDir("", "backup")
	if err != nil {
		return nil, err
	}

	walletCopy, err := walletdbCopy(dir, walletDB)
	if err != nil {
		return nil, err
	}

	err = dropSyncedBlock(chainParams, walletCopy)
	if err != nil {
		return nil, err
	}

	walletdbPath := filepath.Join(dir, "wallet.db")
	err = boltCopy(walletCopy, walletdbPath, nil)
	if err != nil {
		return nil, err
	}
	err = os.Remove(walletCopy)
	if err != nil {
		return nil, err
	}

	channeldbPath := filepath.Join(dir, "channel.db")
	err = channeldbCopy(channelDB, channeldbPath)
	if err != nil {
		return nil, err
	}

	return []string{walletdbPath, channeldbPath}, nil
}

func dropSyncedBlock(chainParams *chaincfg.Params, wallet string) error {
	wdb, err := walletdb.Open("bdb", wallet)
	if err != nil {
		return err
	}
	defer wdb.Close()
	err = walletdb.Update(wdb, func(tx walletdb.ReadWriteTx) error {
		ns := tx.ReadWriteBucket(waddrmgrNamespace)
		if err != nil {
			return err
		}
		syncBucketOld := ns.NestedReadWriteBucket(syncBucketName)
		birthday := syncBucketOld.Get(birthdayName)
		ns.DeleteNestedBucket(syncBucketName)

		genesis := chainParams.GenesisHash.CloneBytes()
		startBlock := append([]byte{0, 0, 0, 0}, genesis...)
		syncedTo := append(startBlock, 0, 0, 0, 0)
		syncBucket, err := ns.CreateBucket(syncBucketName)
		if err != nil {
			return err
		}
		err = syncBucket.Put(birthdayName, birthday)
		if err != nil {
			return err
		}
		err = syncBucket.Put([]byte{0, 0, 0, 0}, genesis)
		if err != nil {
			return err
		}
		err = syncBucket.Put(startBlockName, startBlock)
		if err != nil {
			return err
		}
		err = syncBucket.Put(syncedToName, syncedTo)
		if err != nil {
			return err
		}
		return nil

	})
	if err != nil {
		return err
	}

	return nil
}

func walletdbCopy(dir string, walletDB walletdb.DB) (string, error) {
	walletCopy := filepath.Join(dir, "wallet-temp.db")
	f1, err := os.Create(walletCopy)
	if err != nil {
		return "", err
	}
	defer f1.Close()
	err = walletDB.Copy(f1)
	if err != nil {
		return "", err
	}
	return walletCopy, nil
}

func channeldbCopy(channelDB *channeldb.DB, destfile string) error {
	// Open destination database.
	dst, err := bolt.Open(destfile, 0600, nil)
	if err != nil {
		return err
	}
	defer dst.Close()

	graphBuckets := map[string]interface{}{
		"graph-edge": true,
		"graph-meta": true,
		"graph-node": true,
	}

	// Run compaction.
	err = compact(dst, channelDB.DB, func(keys [][]byte, k, v []byte) bool {
		if len(keys) == 0 && v == nil && graphBuckets[string(k)] != nil {
			return true
		}
		return false
	})
	return err
}

func boltCopy(srcfile, destfile string, skip skipFunc) error {
	// Open source database.
	src, err := bolt.Open(srcfile, 0444, nil)
	if err != nil {
		return err
	}
	defer src.Close()

	// Open destination database.
	dst, err := bolt.Open(destfile, 0600, nil)
	if err != nil {
		return err
	}
	defer dst.Close()

	// Run compaction.
	err = compact(dst, src, skip)
	return err
}

func compact(dst, src *bolt.DB, skip skipFunc) error {
	// commit regularly, or we'll run out of memory for large datasets if using one transaction.
	var size int64
	tx, err := dst.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := walk(src, func(keys [][]byte, k, v []byte, seq uint64) error {
		// On each key/value, check if we have exceeded tx size.
		sz := int64(len(k) + len(v))
		if size+sz > txMaxSize {
			// Commit previous transaction.
			if err := tx.Commit(); err != nil {
				return err
			}

			// Start new transaction.
			tx, err = dst.Begin(true)
			if err != nil {
				return err
			}
			size = 0
		}
		size += sz

		// Create bucket on the root transaction if this is the first level.
		nk := len(keys)
		if nk == 0 {
			bkt, err := tx.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Create buckets on subsequent levels, if necessary.
		b := tx.Bucket(keys[0])
		if nk > 1 {
			for _, k := range keys[1:] {
				b = b.Bucket(k)
			}
		}

		// Fill the entire page for best compaction.
		b.FillPercent = 1.0

		// If there is no value then this is a bucket call.
		if v == nil {
			bkt, err := b.CreateBucket(k)
			if err != nil {
				return err
			}
			if err := bkt.SetSequence(seq); err != nil {
				return err
			}
			return nil
		}

		// Otherwise treat it as a key/value pair.
		return b.Put(k, v)
	}, skip); err != nil {
		return err
	}

	return tx.Commit()
}

// walkFunc is the type of the function called for keys (buckets and "normal"
// values) discovered by Walk. keys is the list of keys to descend to the bucket
// owning the discovered key/value pair k/v.
type walkFunc func(keys [][]byte, k, v []byte, seq uint64) error

type skipFunc func(keys [][]byte, k, v []byte) bool

// walk walks recursively the bolt database db, calling walkFn for each key it finds.
func walk(db *bolt.DB, walkFn walkFunc, skipFn skipFunc) error {
	return db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return walkBucket(b, nil, name, nil, b.Sequence(), walkFn, skipFn)
		})
	})
}

func walkBucket(b *bolt.Bucket, keypath [][]byte, k, v []byte, seq uint64, fn walkFunc, skip skipFunc) error {

	if skip != nil && skip(keypath, k, v) {
		return nil
	}

	// Execute callback.
	if err := fn(keypath, k, v, seq); err != nil {
		return err
	}

	// If this is not a bucket then stop.
	if v != nil {
		return nil
	}

	// Iterate over each child key/value.
	keypath = append(keypath, k)
	return b.ForEach(func(k, v []byte) error {
		if v == nil {
			bkt := b.Bucket(k)
			return walkBucket(bkt, keypath, k, nil, bkt.Sequence(), fn, skip)
		}
		return walkBucket(b, keypath, k, v, b.Sequence(), fn, skip)
	})
}
