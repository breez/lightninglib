package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

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

	edgeBucket         = []byte("graph-edge")
	channelPointBucket = []byte("chan-index")
	edgeIndexBucket    = []byte("edge-index")
	nodeBucket         = []byte("graph-node")
)

type backupResult struct {
	path string
	err  error
}

func Backup(chainParams *chaincfg.Params, channelDB *channeldb.DB, walletDB walletdb.DB) ([]string, error) {

	fmt.Println("Backup started at: ", time.Now())
	dir, err := ioutil.TempDir("", "backup")
	if err != nil {
		return nil, err
	}
	channeldbPath, walletdbPath := filepath.Join(dir, "channel.db"), filepath.Join(dir, "wallet.db")
	var walletdbErr, channeldbErr error

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		channeldbErr = backupChanneldb(channelDB, channeldbPath)
	}()

	go func() {
		defer wg.Done()
		walletdbErr = backupWalletdb(chainParams, walletDB, walletdbPath)
	}()
	wg.Wait()

	fmt.Println("Backup completed at: ", time.Now())
	err = walletdbErr
	if channeldbErr != nil {
		err = channeldbErr
	}
	return []string{channeldbPath, walletdbPath}, err
}

func backupWalletdb(chainParams *chaincfg.Params, walletDB walletdb.DB, destFile string) error {
	walletCopy, err := walletdbCopy(filepath.Dir(destFile), walletDB)
	if err != nil {
		return err
	}

	err = dropSyncedBlock(chainParams, walletCopy)
	if err != nil {
		return err
	}

	err = boltCopy(walletCopy, destFile, nil)
	if err != nil {
		return err
	}

	err = os.Remove(walletCopy)
	if err != nil {
		return err
	}

	return nil
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

func backupChanneldb(channelDB *channeldb.DB, destfile string) error {
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
	if err != nil {
		return err
	}

	return copyChanIndex(dst, channelDB.DB)
}

func copyChanIndex(dst, src *bolt.DB) error {
	tx, err := dst.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	edges, err := tx.CreateBucketIfNotExists(edgeBucket)
	if err != nil {
		return err
	}
	chanIndex, err := edges.CreateBucketIfNotExists(channelPointBucket)
	if err != nil {
		return err
	}
	edgeIndex, err := edges.CreateBucketIfNotExists(edgeIndexBucket)
	if err != nil {
		return err
	}
	nodes, err := tx.CreateBucketIfNotExists(nodeBucket)
	if err != nil {
		return err
	}
	err = copyChanEdgeIndexToBucket(edges, chanIndex, edgeIndex, nodes, src)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func copyChanEdgeIndexToBucket(e, ci, ei, n *bolt.Bucket, db *bolt.DB) error {
	return db.View(func(tx *bolt.Tx) error {
		edges := tx.Bucket(edgeBucket)
		if edges == nil {
			return nil
		}
		chanIndex := edges.Bucket(channelPointBucket)
		if chanIndex == nil {
			return nil
		}
		edgeIndex := edges.Bucket(edgeIndexBucket)
		/*if edgeIndex == nil {
			return nil
		}*/
		nodes := tx.Bucket(nodeBucket)
		/*if nodes == nil {
			return nil
		}*/
		return chanIndex.ForEach(func(k, chanID []byte) error {
			if chanID != nil {
				err := ci.Put(k, chanID)
				if err != nil {
					return err
				}
				edgeInfo := edgeIndex.Get(chanID)
				if edgeInfo != nil {
					err = ei.Put(chanID, edgeInfo)
					if err != nil {
						return err
					}
					node1Pub := edgeInfo[:33]
					var edge1Key [33 + 8]byte
					copy(edge1Key[:], node1Pub)
					copy(edge1Key[33:], chanID[:])
					e.Put(edge1Key[:], edges.Get(edge1Key[:]))
					n.Put(node1Pub, nodes.Get(node1Pub))

					node2Pub := edgeInfo[33:66]
					var edge2Key [33 + 8]byte
					copy(edge2Key[:], node2Pub)
					copy(edge2Key[33:], chanID[:])
					e.Put(edge2Key[:], edges.Get(edge2Key[:]))
					n.Put(node2Pub, nodes.Get(node2Pub))
				}
			}
			return nil
		})
	})
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
