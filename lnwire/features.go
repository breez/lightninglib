package lnwire

import (
	"encoding/binary"
	"fmt"
	"io"
)

// FeatureBit represents a feature that can be enabled in either a local or
// global feature vector at a specific bit position. Feature bits follow the
// "it's OK to be odd" rule, where features at even bit positions must be known
// to a node receiving them from a peer while odd bits do not. In accordance,
// feature bits are usually assigned in pairs, first being assigned an odd bit
// position which may later be changed to the preceding even position once
// knowledge of the feature becomes required on the network.
type FeatureBit uint16

const (
	// DataLossProtectRequired is a feature bit that indicates that a peer
	// *requires* the other party know about the data-loss-protect optional
	// feature. If the remote peer does not know of such a feature, then
	// the sending peer SHOLUD disconnect them. The data-loss-protect
	// feature allows a peer that's lost partial data to recover their
	// settled funds of the latest commitment state.
	DataLossProtectRequired FeatureBit = 0

	// DataLossProtectOptional is an optional feature bit that indicates
	// that the sending peer knows of this new feature and can activate it
	// it. The data-loss-protect feature allows a peer that's lost partial
	// data to recover their settled funds of the latest commitment state.
	DataLossProtectOptional FeatureBit = 1

	// InitialRoutingSync is a local feature bit meaning that the receiving
	// node should send a complete dump of routing information when a new
	// connection is established.
	InitialRoutingSync FeatureBit = 3

	// GossipQueriesRequired is a feature bit that indicates that the
	// receiving peer MUST know of the set of features that allows nodes to
	// more efficiently query the network view of peers on the network for
	// reconciliation purposes.
	GossipQueriesRequired FeatureBit = 6

	// GossipQueriesOptional is an optional feature bit that signals that
	// the setting peer knows of the set of features that allows more
	// efficient network view reconciliation.
	GossipQueriesOptional FeatureBit = 7

	// maxAllowedSize is a maximum allowed size of feature vector.
	//
	// NOTE: Within the protocol, the maximum allowed message size is 65535
	// bytes. Adding the overhead from the crypto protocol (the 2-byte
	// packet length and 16-byte MAC), we arrive at 65569 bytes. Accounting
	// for the overhead within the feature message to signal the type of
	// the message, that leaves 65567 bytes for the init message itself.
	// Next, we reserve 4-bytes to encode the lengths of both the local and
	// global feature vectors, so 65563 for the global and local features.
	// Knocking off one byte for the sake of the calculation, that leads to
	// a max allowed size of 32781 bytes for each feature vector, or 131124
	// different features.
	maxAllowedSize = 32781
)

// LocalFeatures is a mapping of known connection-local feature bits to a
// descriptive name. All known local feature bits must be assigned a name in
// this mapping. Local features are those which are only sent to the peer and
// not advertised to the entire network. A full description of these feature
// bits is provided in the BOLT-09 specification.
var LocalFeatures = map[FeatureBit]string{
	DataLossProtectRequired: "data-loss-protect-required",
	DataLossProtectOptional: "data-loss-protect-optional",
	InitialRoutingSync:      "initial-routing-sync",
	GossipQueriesRequired:   "gossip-queries-required",
	GossipQueriesOptional:   "gossip-queries-optional",
}

// GlobalFeatures is a mapping of known global feature bits to a descriptive
// name. All known global feature bits must be assigned a name in this mapping.
// Global features are those which are advertised to the entire network. A full
// description of these feature bits is provided in the BOLT-09 specification.
var GlobalFeatures map[FeatureBit]string

// RawFeatureVector represents a set of feature bits as defined in BOLT-09.  A
// RawFeatureVector itself just stores a set of bit flags but can be used to
// construct a FeatureVector which binds meaning to each bit. Feature vectors
// can be serialized and deserialized to/from a byte representation that is
// transmitted in Lightning network messages.
type RawFeatureVector struct {
	features map[FeatureBit]bool
}

// NewRawFeatureVector creates a feature vector with all of the feature bits
// given as arguments enabled.
func NewRawFeatureVector(bits ...FeatureBit) *RawFeatureVector {
	fv := &RawFeatureVector{features: make(map[FeatureBit]bool)}
	for _, bit := range bits {
		fv.Set(bit)
	}
	return fv
}

// IsSet returns whether a particular feature bit is enabled in the vector.
func (fv *RawFeatureVector) IsSet(feature FeatureBit) bool {
	return fv.features[feature]
}

// Set marks a feature as enabled in the vector.
func (fv *RawFeatureVector) Set(feature FeatureBit) {
	fv.features[feature] = true
}

// Unset marks a feature as disabled in the vector.
func (fv *RawFeatureVector) Unset(feature FeatureBit) {
	delete(fv.features, feature)
}

// SerializeSize returns the number of bytes needed to represent feature vector
// in byte format.
func (fv *RawFeatureVector) SerializeSize() int {
	// Find the largest feature bit index
	max := -1
	for feature := range fv.features {
		index := int(feature)
		if index > max {
			max = index
		}
	}
	if max == -1 {
		return 0
	}

	// We calculate byte-length via the largest bit index
	return max/8 + 1
}

// Encode writes the feature vector in byte representation. Every feature
// encoded as a bit, and the bit vector is serialized using the least number of
// bytes. Since the bit vector length is variable, the first two bytes of the
// serialization represent the length.
func (fv *RawFeatureVector) Encode(w io.Writer) error {
	// Write length of feature vector.
	var l [2]byte
	length := fv.SerializeSize()
	binary.BigEndian.PutUint16(l[:], uint16(length))
	if _, err := w.Write(l[:]); err != nil {
		return err
	}

	// Generate the data and write it.
	data := make([]byte, length)
	for feature := range fv.features {
		byteIndex := int(feature / 8)
		bitIndex := feature % 8
		data[length-byteIndex-1] |= 1 << bitIndex
	}

	_, err := w.Write(data)
	return err
}

// Decode reads the feature vector from its byte representation. Every feature
// encoded as a bit, and the bit vector is serialized using the least number of
// bytes. Since the bit vector length is variable, the first two bytes of the
// serialization represent the length.
func (fv *RawFeatureVector) Decode(r io.Reader) error {
	// Read the length of the feature vector.
	var l [2]byte
	if _, err := io.ReadFull(r, l[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint16(l[:])

	// Read the feature vector data.
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}

	// Set feature bits from parsed data.
	bitsNumber := len(data) * 8
	for i := 0; i < bitsNumber; i++ {
		byteIndex := uint16(i / 8)
		bitIndex := uint(i % 8)
		if (data[length-byteIndex-1]>>bitIndex)&1 == 1 {
			fv.Set(FeatureBit(i))
		}
	}

	return nil
}

// FeatureVector represents a set of enabled features. The set stores
// information on enabled flags and metadata about the feature names. A feature
// vector is serializable to a compact byte representation that is included in
// Lightning network messages.
type FeatureVector struct {
	*RawFeatureVector
	featureNames map[FeatureBit]string
}

// NewFeatureVector constructs a new FeatureVector from a raw feature vector
// and mapping of feature definitions. If the feature vector argument is nil, a
// new one will be constructed with no enabled features.
func NewFeatureVector(featureVector *RawFeatureVector,
	featureNames map[FeatureBit]string) *FeatureVector {

	if featureVector == nil {
		featureVector = NewRawFeatureVector()
	}
	return &FeatureVector{
		RawFeatureVector: featureVector,
		featureNames:     featureNames,
	}
}

// HasFeature returns whether a particular feature is included in the set. The
// feature can be seen as set either if the bit is set directly OR the queried
// bit has the same meaning as its corresponding even/odd bit, which is set
// instead. The second case is because feature bits are generally assigned in
// pairs where both the even and odd position represent the same feature.
func (fv *FeatureVector) HasFeature(feature FeatureBit) bool {
	return fv.IsSet(feature) ||
		(fv.isFeatureBitPair(feature) && fv.IsSet(feature^1))
}

// UnknownRequiredFeatures returns a list of feature bits set in the vector
// that are unknown and in an even bit position. Feature bits with an even
// index must be known to a node receiving the feature vector in a message.
func (fv *FeatureVector) UnknownRequiredFeatures() []FeatureBit {
	var unknown []FeatureBit
	for feature := range fv.features {
		if feature%2 == 0 && !fv.IsKnown(feature) {
			unknown = append(unknown, feature)
		}
	}
	return unknown
}

// Name returns a string identifier for the feature represented by this bit. If
// the bit does not represent a known feature, this returns a string indicating
// as much.
func (fv *FeatureVector) Name(bit FeatureBit) string {
	name, known := fv.featureNames[bit]
	if !known {
		name = "unknown"
	}
	return fmt.Sprintf("%s(%d)", name, bit)
}

// IsKnown returns whether this feature bit represents a known feature.
func (fv *FeatureVector) IsKnown(bit FeatureBit) bool {
	_, known := fv.featureNames[bit]
	return known
}

// isFeatureBitPair returns whether this feature bit and its corresponding
// even/odd bit both represent the same feature. This may often be the case as
// bits are generally assigned in pairs, first being assigned an odd bit
// position then being promoted to an even bit position once the network is
// ready.
func (fv *FeatureVector) isFeatureBitPair(bit FeatureBit) bool {
	name1, known1 := fv.featureNames[bit]
	name2, known2 := fv.featureNames[bit^1]
	return known1 && known2 && name1 == name2
}
