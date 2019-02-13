package lnwire

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

var (
	testOnionHash     = []byte{}
	testAmount        = MilliSatoshi(1)
	testCtlvExpiry    = uint32(2)
	testFlags         = uint16(2)
	sig, _            = NewSigFromSignature(testSig)
	testChannelUpdate = ChannelUpdate{
		Signature:      sig,
		ShortChannelID: NewShortChanIDFromInt(1),
		Timestamp:      1,
		MessageFlags:   0,
		ChannelFlags:   1,
	}
)

var onionFailures = []FailureMessage{
	&FailInvalidRealm{},
	&FailTemporaryNodeFailure{},
	&FailPermanentNodeFailure{},
	&FailRequiredNodeFeatureMissing{},
	&FailPermanentChannelFailure{},
	&FailRequiredChannelFeatureMissing{},
	&FailUnknownNextPeer{},
	&FailIncorrectPaymentAmount{},
	&FailFinalExpiryTooSoon{},

	NewFailUnknownPaymentHash(99),
	NewInvalidOnionVersion(testOnionHash),
	NewInvalidOnionHmac(testOnionHash),
	NewInvalidOnionKey(testOnionHash),
	NewTemporaryChannelFailure(&testChannelUpdate),
	NewTemporaryChannelFailure(nil),
	NewAmountBelowMinimum(testAmount, testChannelUpdate),
	NewFeeInsufficient(testAmount, testChannelUpdate),
	NewIncorrectCltvExpiry(testCtlvExpiry, testChannelUpdate),
	NewExpiryTooSoon(testChannelUpdate),
	NewChannelDisabled(testFlags, testChannelUpdate),
	NewFinalIncorrectCltvExpiry(testCtlvExpiry),
	NewFinalIncorrectHtlcAmount(testAmount),
}

// TestEncodeDecodeCode tests the ability of onion errors to be properly encoded
// and decoded.
func TestEncodeDecodeCode(t *testing.T) {
	for _, failure1 := range onionFailures {
		var b bytes.Buffer

		if err := EncodeFailure(&b, failure1, 0); err != nil {
			t.Fatalf("unable to encode failure code(%v): %v",
				failure1.Code(), err)
		}

		failure2, err := DecodeFailure(&b, 0)
		if err != nil {
			t.Fatalf("unable to decode failure code(%v): %v",
				failure1.Code(), err)
		}

		if !reflect.DeepEqual(failure1, failure2) {
			t.Fatalf("expected %v, got %v", spew.Sdump(failure1),
				spew.Sdump(failure2))
		}
	}
}

// TestChannelUpdateCompatabilityParsing tests that we're able to properly read
// out channel update messages encoded in an onion error payload that was
// written in the legacy (type prefixed) format.
func TestChannelUpdateCompatabilityParsing(t *testing.T) {
	t.Parallel()

	// We'll start by taking out test channel update, and encoding it into
	// a set of raw bytes.
	var b bytes.Buffer
	if err := testChannelUpdate.Encode(&b, 0); err != nil {
		t.Fatalf("unable to encode chan update: %v", err)
	}

	// Now that we have the set of bytes encoded, we'll ensure that we're
	// able to decode it using our compatibility method, as it's a regular
	// encoded channel update message.
	var newChanUpdate ChannelUpdate
	err := parseChannelUpdateCompatabilityMode(
		bufio.NewReader(&b), &newChanUpdate, 0,
	)
	if err != nil {
		t.Fatalf("unable to parse channel update: %v", err)
	}

	// At this point, we'll ensure that we get the exact same failure out
	// on the other side.
	if !reflect.DeepEqual(testChannelUpdate, newChanUpdate) {
		t.Fatalf("mismatched channel updates: %v", err)
	}

	// We'll now reset then re-encoded the same channel update to try it in
	// the proper compatible mode.
	b.Reset()

	// Before we encode the update itself, we'll also write out the 2-byte
	// type in order to simulate the compat mode.
	var tByte [2]byte
	binary.BigEndian.PutUint16(tByte[:], MsgChannelUpdate)
	b.Write(tByte[:])
	if err := testChannelUpdate.Encode(&b, 0); err != nil {
		t.Fatalf("unable to encode chan update: %v", err)
	}

	// We should be able to properly parse the encoded channel update
	// message even with the extra two bytes.
	var newChanUpdate2 ChannelUpdate
	err = parseChannelUpdateCompatabilityMode(
		bufio.NewReader(&b), &newChanUpdate2, 0,
	)
	if err != nil {
		t.Fatalf("unable to parse channel update: %v", err)
	}

	if !reflect.DeepEqual(newChanUpdate2, newChanUpdate) {
		t.Fatalf("mismatched channel updates: %v", err)
	}
}

// TestWriteOnionErrorChanUpdate tests that we write an exact size for the
// channel update in order to be more compliant with the parsers of other
// implementations.
func TestWriteOnionErrorChanUpdate(t *testing.T) {
	t.Parallel()

	// First, we'll write out the raw channel update so we can obtain the
	// raw serialized length.
	var b bytes.Buffer
	update := testChannelUpdate
	if err := update.Encode(&b, 0); err != nil {
		t.Fatalf("unable to write update: %v", err)
	}
	trueUpdateLength := b.Len()

	// Next, we'll use the function to encode the update as we would in a
	// onion error message.
	var errorBuf bytes.Buffer
	err := writeOnionErrorChanUpdate(&errorBuf, &update, 0)
	if err != nil {
		t.Fatalf("unable to encode onion error: %v", err)
	}

	// Finally, read the length encoded and ensure that it matches the raw
	// length.
	var encodedLen uint16
	if err := readElement(&errorBuf, &encodedLen); err != nil {
		t.Fatalf("unable to read len: %v", err)
	}
	if uint16(trueUpdateLength) != encodedLen {
		t.Fatalf("wrong length written: expected %v, got %v",
			trueUpdateLength, encodedLen)
	}
}

// TestFailUnknownPaymentHashOptionalAmount tests that we're able to decode an
// UnknownPaymentHash error that doesn't have the optional amount. This ensures
// we're able to decode FailUnknownPaymentHash messages from older nodes.
func TestFailUnknownPaymentHashOptionalAmount(t *testing.T) {
	t.Parallel()

	// Creation an error that is a non-pointer will allow us to skip the
	// type assertion for the Serializable interface. As a result, the
	// amount body won't be written.
	onionError := FailUnknownPaymentHash{}

	var b bytes.Buffer
	if err := EncodeFailure(&b, onionError, 0); err != nil {
		t.Fatalf("unable to encode failure: %v", err)
	}

	onionError2, err := DecodeFailure(bytes.NewReader(b.Bytes()), 0)
	if err != nil {
		t.Fatalf("unable to decode error: %v", err)
	}

	if !reflect.DeepEqual(onionError, onionError) {
		t.Fatalf("expected %v, got %v", spew.Sdump(onionError),
			spew.Sdump(onionError2))
	}
}
