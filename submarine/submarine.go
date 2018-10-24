package submarine

import (
	"errors"

	"github.com/breez/lightninglib/channeldb"
	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcwallet/chain"
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

func saveSubmarineData(db *channeldb.DB, hash []byte, address btcutil.AddressScriptHash, swapperKey, script []byte) error {
	//need to serialize address, swapperKey and script in order to save them
	//address size is always ripemd160.Size + 1 = 20 + 1 = 21 bytes
	//swapperKey size is btcec.PrivKeyBytesLen = 32 bytes
	return nil
}

func getSubmarineData(db *channeldb.DB, hash []byte) (address btcutil.Address, swapperKey, script []byte, err error) {
	err = errors.New("hash not found")
	return
}

func NewAddress(net *chaincfg.Params, chainClient chain.Interface, db *channeldb.DB, pubKey, hash []byte) (address btcutil.Address, script, swapperPubKey []byte, err error) {

	if len(pubKey) != btcec.PubKeyBytesLenCompressed {
		err = errors.New("pubKey not valid")
	}

	if len(hash) != 32 {
		err = errors.New("hash not valid")
	}

	//Need to check that the hash doesn't already exists in our db
	_, _, _, errGet := getSubmarineData(db, hash)
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
	err = saveSubmarineData(db, hash, address, swapperKey, script)
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
