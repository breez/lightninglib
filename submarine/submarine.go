package submarine

import (
	"bytes"
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

func saveSubmarineData(db *channeldb.DB, netID byte, hash, swapperKey []byte, script []byte) error {
	if len(swapperKey) != btcec.PrivKeyBytesLen {
		return errors.New("pubKey not valid")
	}

	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(submarineBucket)
		if err != nil {
			return err
		}

		var submarineData bytes.Buffer
		err = submarineData.WriteByte(netID)
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

		return bucket.Put(hash, submarineData.Bytes())
	})
}

func getSubmarineData(db *channeldb.DB, netID byte, hash []byte) (swapperKey, script []byte, err error) {

	err = db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(submarineBucket)
		if bucket == nil {
			return errors.New("Not found")
		}

		value := bucket.Get(hash)
		if value == nil {
			return errors.New("Not found")
		}

		if value[0] != netID {
			return errors.New("Not the same network")
		}

		swapperKey = make([]byte, btcec.PrivKeyBytesLen)
		copy(swapperKey, value[1:btcec.PrivKeyBytesLen+1])
		script = make([]byte, len(value)-1-btcec.PrivKeyBytesLen)
		copy(script, value[btcec.PrivKeyBytesLen+1:])

		return nil
	})

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
	_, _, errGet := getSubmarineData(db, net.ScriptHashAddrID, hash)
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

	//Need to save these keyed by hash:
	err = saveSubmarineData(db, net.ScriptHashAddrID, hash, swapperKey, script)
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
