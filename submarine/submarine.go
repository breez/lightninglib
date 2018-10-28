package submarine

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/breez/lightninglib/channeldb"
	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcwallet/chain"
	"github.com/coreos/bbolt"
)

const (
	defaultLockHeight = 72
)

var (
	submarineBucket = []byte("submarineTransactions")
)

func genSubmarineSwapScript(swapperPubKey, payerPubKey, hash []byte, lockHeight int64) ([]byte, error) {
	builder := txscript.NewScriptBuilder()

	builder.AddOp(txscript.OP_HASH160)
	builder.AddData(btcutil.Hash160(hash))
	builder.AddOp(txscript.OP_EQUAL) // Leaves 0P1 (true) on the stack if preimage matches
	builder.AddOp(txscript.OP_IF)
	builder.AddData(swapperPubKey) // Path taken if preimage matches
	builder.AddOp(txscript.OP_ELSE)
	builder.AddInt64(lockHeight)
	builder.AddOp(txscript.OP_CHECKSEQUENCEVERIFY)
	builder.AddOp(txscript.OP_DROP)
	builder.AddData(payerPubKey) // Refund back to payer
	builder.AddOp(txscript.OP_ENDIF)
	builder.AddOp(txscript.OP_CHECKSIG)

	return builder.Script()
}

func saveSwapperSubmarineData(db *channeldb.DB, netID byte, hash []byte, creationHeight, lockHeight int64, swapperKey []byte, script []byte) error {

	/**
	key: swapper:<hash>
	value:
		[0]: netID
		[1:9]: creationHeight
		[9:17]: lockHeight
		[17:17+btcec.PrivKeyBytesLen]: swapperKey
		[17+btcec.PrivKeyBytesLen:]: script
	*/

	if len(swapperKey) != btcec.PrivKeyBytesLen {
		return errors.New("pubKey not valid")
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(submarineBucket)
		if err != nil {
			return err
		}

		var key bytes.Buffer
		_, err = key.WriteString("swapper:")
		if err != nil {
			return err
		}
		_, err = key.Write(hash)
		if err != nil {
			return err
		}

		var submarineData bytes.Buffer
		err = submarineData.WriteByte(netID)
		if err != nil {
			return err
		}
		b := make([]byte, 16)
		binary.BigEndian.PutUint64(b[0:], uint64(creationHeight))
		binary.BigEndian.PutUint64(b[8:], uint64(lockHeight))
		_, err = submarineData.Write(b)
		if err != nil {
			return err
		}
		_, err = submarineData.Write(swapperKey)
		if err != nil {
			return err
		}
		_, err = submarineData.Write(script)
		if err != nil {
			return err
		}

		return bucket.Put(key.Bytes(), submarineData.Bytes())
	})
}

func getSwapperSubmarineData(db *channeldb.DB, netID byte, hash []byte) (creationHeight, lockHeight int64, swapperKey, script []byte, err error) {

	err = db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(submarineBucket)
		if bucket == nil {
			return errors.New("Not found")
		}

		var key bytes.Buffer
		_, err = key.WriteString("swapper:")
		if err != nil {
			return err
		}
		_, err = key.Write(hash)
		if err != nil {
			return err
		}

		value := bucket.Get(key.Bytes())
		if value == nil {
			return errors.New("Not found")
		}

		submarineData := bytes.NewBuffer(value)
		savedNetID, err := submarineData.ReadByte()
		if err != nil {
			return err
		}
		if savedNetID != netID {
			return errors.New("Not the same network")
		}
		b := make([]byte, 16)
		_, err = submarineData.Read(b)
		if err != nil {
			return err
		}
		creationHeight = int64(binary.BigEndian.Uint64(b[0:]))
		lockHeight = int64(binary.BigEndian.Uint64(b[8:]))

		swapperKey = make([]byte, btcec.PrivKeyBytesLen)
		_, err = submarineData.Read(swapperKey)
		if err != nil {
			return err
		}

		script = make([]byte, submarineData.Len())
		_, err = submarineData.Read(script)
		if err != nil {
			return err
		}

		return nil
	})

	return
}

func AddressFromHash(net *chaincfg.Params, db *channeldb.DB, hash []byte) (address btcutil.Address, creationHeight int64, err error) {
	var script []byte
	creationHeight, _, _, script, err = getSwapperSubmarineData(db, net.ScriptHashAddrID, hash)
	if err != nil {
		return
	}
	address, err = btcutil.NewAddressScriptHash(script, net)
	return
}

func NewAddress(net *chaincfg.Params, chainClient chain.Interface, db *channeldb.DB, pubKey, hash []byte) (address btcutil.Address, script, swapperPubKey []byte, err error) {

	if len(pubKey) != btcec.PubKeyBytesLenCompressed {
		err = errors.New("pubKey not valid")
		return
	}

	if len(hash) != 32 {
		err = errors.New("hash not valid")
		return
	}

	//Need to check that the hash doesn't already exists in our db
	_, _, _, _, errGet := getSwapperSubmarineData(db, net.ScriptHashAddrID, hash)
	if errGet == nil {
		err = errors.New("Hash already exists")
		return
	}

	//Create swapperKey and swapperPubKey
	key, err := btcec.NewPrivateKey(btcec.S256())
	if err != nil {
		return
	}
	swapperKey := key.Serialize()
	swapperPubKey = key.PubKey().SerializeCompressed()

	//Create the script
	script, err = genSubmarineSwapScript(swapperPubKey, pubKey, hash, defaultLockHeight)
	if err != nil {
		return
	}

	//Generate the address
	address, err = btcutil.NewAddressScriptHash(script, net)
	if err != nil {
		return
	}

	_, currentHeight, err := chainClient.GetBestBlock()
	if err != nil {
		return
	}

	//Need to save these keyed by hash:
	err = saveSwapperSubmarineData(db, net.ScriptHashAddrID, hash, int64(currentHeight), defaultLockHeight, swapperKey, script)
	if err != nil {
		return
	}

	//Watch the new address
	err = chainClient.NotifyReceived([]btcutil.Address{address})
	if err != nil {
		return
	}

	return
}
