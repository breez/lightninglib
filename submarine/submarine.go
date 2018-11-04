package submarine

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/breez/lightninglib/channeldb"
	"github.com/breez/lightninglib/lnwallet"
	"github.com/breez/lightninglib/lnwallet/btcwallet"
	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcwallet/chain"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/btcsuite/btcwallet/wtxmgr"
	"github.com/coreos/bbolt"
)

const (
	defaultLockHeight = 72
)

var (
	submarineBucket    = []byte("submarineTransactions")
	wtxmgrNamespaceKey = []byte("wtxmgr")
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

func saveSubmarineData(db *channeldb.DB, netID byte, address btcutil.Address, creationHeight, lockHeight int64, preimage, key, swapperPubKey, script []byte) error {

	if len(preimage) != 32 {
		return errors.New("preimage not valid")
	}
	if len(key) != btcec.PrivKeyBytesLen {
		return errors.New("key not valid")
	}
	if len(swapperPubKey) != btcec.PubKeyBytesLenCompressed {
		return errors.New("swapperPubKey not valid")
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
		b := make([]byte, 16)
		binary.BigEndian.PutUint64(b[0:], uint64(creationHeight))
		binary.BigEndian.PutUint64(b[8:], uint64(lockHeight))
		_, err = submarineData.Write(b)
		if err != nil {
			return err
		}
		_, err = submarineData.Write(preimage)
		if err != nil {
			return err
		}
		_, err = submarineData.Write(key)
		if err != nil {
			return err
		}
		_, err = submarineData.Write(swapperPubKey)
		if err != nil {
			return err
		}
		_, err = submarineData.Write(script)
		if err != nil {
			return err
		}

		return bucket.Put([]byte("swapClient:"+address.String()), submarineData.Bytes())
	})
}

func getSubmarineData(db *channeldb.DB, netID byte, address btcutil.Address) (creationHeight, lockHeight int64, preimage, key, swapperPubKey, script []byte, err error) {
	err = db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(submarineBucket)
		if bucket == nil {
			return errors.New("Not found")
		}

		value := bucket.Get([]byte("swapClient:" + address.String()))
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

		preimage = make([]byte, 32)
		_, err = submarineData.Read(preimage)
		if err != nil {
			return err
		}
		key = make([]byte, btcec.PrivKeyBytesLen)
		_, err = submarineData.Read(key)
		if err != nil {
			return err
		}
		swapperPubKey = make([]byte, btcec.PubKeyBytesLenCompressed)
		_, err = submarineData.Read(swapperPubKey)
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
		return errors.New("swapperKey not valid")
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

func newAddressWitnessScriptHash(script []byte, net *chaincfg.Params) (btcutil.Address, error){
	witnessProg := sha256.Sum256(script)
	return btcutil.NewAddressWitnessScriptHash(witnessProg[:], net)
}


// AddressFromHash
func AddressFromHash(net *chaincfg.Params, db *channeldb.DB, hash []byte) (address btcutil.Address, creationHeight int64, err error) {
	var script []byte
	creationHeight, _, _, script, err = getSwapperSubmarineData(db, net.ScriptHashAddrID, hash)
	if err != nil {
		return
	}
	address, err = newAddressWitnessScriptHash(script, net)
	return
}

// CreationHeight
func CreationHeight(net *chaincfg.Params, db *channeldb.DB, address btcutil.Address) (creationHeight int64, err error) {
	creationHeight, _, _, _, _, _, err = getSubmarineData(db, net.ScriptHashAddrID, address)
	return
}

func SubmarineSwapInit() (preimage, hash, key, pubKey []byte, err error) {
	preimage = make([]byte, 32)
	rand.Read(preimage)
	h := sha256.Sum256(preimage)
	hash = h[:]

	k, err := btcec.NewPrivateKey(btcec.S256())
	if err != nil {
		return
	}
	key = k.Serialize()
	pubKey = k.PubKey().SerializeCompressed()
	return
}

func NewSubmarineSwap(net *chaincfg.Params, chainClient chain.Interface, db *channeldb.DB, pubKey, hash []byte) (address btcutil.Address, script, swapperPubKey []byte, lockHeight int64, err error) {

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
	address, err = newAddressWitnessScriptHash(script, net)
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

	lockHeight = defaultLockHeight

	//Watch the new address
	err = chainClient.NotifyReceived([]btcutil.Address{address})
	if err != nil {
		return
	}

	return
}

func WatchSubmarineSwap(net *chaincfg.Params, chainClient chain.Interface, db *channeldb.DB,
	preimage, key, swapperPubKey []byte, lockHeight int64) (address btcutil.Address, script []byte, err error) {

	_, currentHeight, err := chainClient.GetBestBlock()
	if err != nil {
		return
	}

	_, pu := btcec.PrivKeyFromBytes(btcec.S256(), key)
	h := sha256.Sum256(preimage)
	hash := h[:]
	//Create the script
	script, err = genSubmarineSwapScript(swapperPubKey, pu.SerializeCompressed(), hash, lockHeight)
	if err != nil {
		return
	}
	//Generate the address
	address, err = newAddressWitnessScriptHash(script, net)
	if err != nil {
		return
	}

	err = saveSubmarineData(db, net.ScriptHashAddrID, address, int64(currentHeight), lockHeight, preimage, key, swapperPubKey, script)
	if err != nil {
		return
	}

	//Watch the new address
	err = chainClient.NotifyReceived([]btcutil.Address{address})
	return
}

// GetFirstTransaction returns the amount of btc in an address from transaction mined
// from the start height. Returns also the height of the transaction and it's id
func GetFirstTransaction(db walletdb.DB, txstore *wtxmgr.Store, net *chaincfg.Params, start int32, address string) (btcutil.Amount, chainhash.Hash, uint32, int32, error) {
	var amt btcutil.Amount
	var txid chainhash.Hash
	var txIndex uint32
	firstHeight := int32(-1)
	err := walletdb.View(db, func(dbtx walletdb.ReadTx) error {
		txmgrNs := dbtx.ReadBucket(wtxmgrNamespaceKey)
		rangeFn := func(details []wtxmgr.TxDetails) (bool, error) {
			// TODO: probably should make RangeTransactions not reuse the
			// details backing array memory.
			dets := make([]wtxmgr.TxDetails, len(details))
			copy(dets, details)
			details = dets

			//txs := make([]TransactionSummary, 0, len(details))
			for _, d := range details {
				//txs = append(txs, makeTxSummary(dbtx, w, &details[i]))
				if d.Block.Height != -1 {
					for i, txout := range d.MsgTx.TxOut {
						_, addrs, _, err := txscript.ExtractPkScriptAddrs(txout.PkScript, net)
						if err == nil {
							if addrs[0].String() == address {
								if firstHeight < 0 {
									firstHeight = d.Block.Height
									amt = btcutil.Amount(txout.Value)
									txIndex = uint32(i)
									txid = d.MsgTx.TxHash()
								}
								return true, nil
							}
						}
					}
				}
			}
			return false, nil
		}

		return txstore.RangeTransactions(txmgrNs, start, int32(^uint32(0)>>1), rangeFn)
	})
	return amt, txid, txIndex, firstHeight, err
}

// Redeem
func Redeem(db *channeldb.DB, net *chaincfg.Params, wallet *lnwallet.LightningWallet, preimage []byte, redeemAddress btcutil.Address, feePerKw lnwallet.SatPerKWeight) (*wire.MsgTx, error) {

	hash := sha256.Sum256(preimage)
	creationHeight, _, swapperKey, script, err := getSwapperSubmarineData(db, net.ScriptHashAddrID, hash[:])
	if err != nil {
		return nil, err
	}
	address, err := newAddressWitnessScriptHash(script, net)
	if err != nil {
		return nil, err
	}
	amount, txid, _, err := wallet.WalletController.(*btcwallet.BtcWallet).InternalWallet().GetFirstTransaction(int32(creationHeight), address.String())

	// Type 2 supports CSV
	redeemTx := wire.NewMsgTx(2)

	// We need to reference the swap transactions outpoint
	prevOut := wire.NewOutPoint(&txid, 0)

	// Send the funds to an address
	redeemScript, err := txscript.PayToAddrScript(redeemAddress)
	if err != nil {
		return nil, err
	}

	feeSatPerKB := btcutil.Amount(feePerKw.FeePerKVByte())
	// TODO: Need to calculate the Fee using feeSatPerKB
	fee := feeSatPerKB
	txAmount := amount - fee
	txOut := wire.NewTxOut(int64(txAmount), redeemScript)
	redeemTx.AddTxOut(txOut)

	// Sign with out private key
	privateKey, _ := btcec.PrivKeyFromBytes(btcec.S256(), swapperKey)
	scriptSig, err := txscript.SignatureScript(redeemTx, 0, script, txscript.SigHashAll, privateKey, true)
	if err != nil {
		return nil, err
	}

	txIn := wire.NewTxIn(prevOut, script, [][]byte{scriptSig, preimage})
	redeemTx.AddTxIn(txIn)

	err = wallet.PublishTransaction(redeemTx)
	if err != nil {
		return nil, err
	}
	return redeemTx, nil
}
