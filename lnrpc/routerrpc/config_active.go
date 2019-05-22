// +build routerrpc

package routerrpc

import (
	"github.com/breez/lightninglib/macaroons"
	"github.com/breez/lightninglib/routing"
	"github.com/btcsuite/btcd/chaincfg"
)

// Config is the main configuration file for the router RPC server. It contains
// all the items required for the router RPC server to carry out its duties.
// The fields with struct tags are meant to be parsed as normal configuration
// options, while if able to be populated, the latter fields MUST also be
// specified.
type Config struct {
	// RouterMacPath is the path for the router macaroon. If unspecified
	// then we assume that the macaroon will be found under the network
	// directory, named DefaultRouterMacFilename.
	RouterMacPath string `long:"routermacaroonpath" description:"Path to the router macaroon"`

	// NetworkDir is the main network directory wherein the router rpc
	// server will find the macaroon named DefaultRouterMacFilename.
	NetworkDir string

	// ActiveNetParams are the network parameters of the primary network
	// that the route is operating on. This is necessary so we can ensure
	// that we receive payment requests that send to destinations on our
	// network.
	ActiveNetParams *chaincfg.Params

	// MacService is the main macaroon service that we'll use to handle
	// authentication for the Router rpc server.
	MacService *macaroons.Service

	// Router is the main channel router instance that backs this RPC
	// server.
	//
	// TODO(roasbeef): make into pkg lvl interface?
	//
	// TODO(roasbeef): assumes router handles saving payment state
	Router *routing.ChannelRouter

	// RouterBackend contains shared logic between this sub server and the
	// main rpc server.
	RouterBackend *RouterBackend
}
