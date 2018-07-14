// Copyright (c) 2013-2017 The btcsuite developers
// Copyright (c) 2015-2016 The Decred developers
// Copyright (C) 2015-2017 The Lightning Network Developers

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	macaroon "gopkg.in/macaroon.v2"

	"github.com/breez/lightninglib/lncfg"
	"github.com/breez/lightninglib/lnrpc"
	"github.com/breez/lightninglib/macaroons"
	"github.com/btcsuite/btcutil"
	"github.com/urfave/cli"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	defaultTLSCertFilename  = "tls.cert"
	defaultMacaroonFilename = "admin.macaroon"
	defaultRpcPort          = "10009"
	defaultRpcHostPort      = "localhost:" + defaultRpcPort
)

var (
	//Commit stores the current commit hash of this build. This should be
	//set using -ldflags during compilation.
	Commit string

	defaultLndDir       = btcutil.AppDataDir("lnd", false)
	defaultTLSCertPath  = filepath.Join(defaultLndDir, defaultTLSCertFilename)
	defaultMacaroonPath = filepath.Join(defaultLndDir, defaultMacaroonFilename)
)

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "[lncli] %v\n", err)
	os.Exit(1)
}

func getWalletUnlockerClient(ctx *cli.Context) (lnrpc.WalletUnlockerClient, func()) {
	conn := getClientConn(ctx, true)

	cleanUp := func() {
		conn.Close()
	}

	return lnrpc.NewWalletUnlockerClient(conn), cleanUp
}

func getClient(ctx *cli.Context) (lnrpc.LightningClient, func()) {
	conn := getClientConn(ctx, false)

	cleanUp := func() {
		conn.Close()
	}

	return lnrpc.NewLightningClient(conn), cleanUp
}

func getClientConn(ctx *cli.Context, skipMacaroons bool) *grpc.ClientConn {
	lndDir := cleanAndExpandPath(ctx.GlobalString("lnddir"))
	if lndDir != defaultLndDir {
		// If a custom lnd directory was set, we'll also check if custom
		// paths for the TLS cert and macaroon file were set as well. If
		// not, we'll override their paths so they can be found within
		// the custom lnd directory set. This allows us to set a custom
		// lnd directory, along with custom paths to the TLS cert and
		// macaroon file.
		tlsCertPath := cleanAndExpandPath(ctx.GlobalString("tlscertpath"))
		if tlsCertPath == defaultTLSCertPath {
			ctx.GlobalSet("tlscertpath",
				filepath.Join(lndDir, defaultTLSCertFilename))
		}

		macPath := cleanAndExpandPath(ctx.GlobalString("macaroonpath"))
		if macPath == defaultMacaroonPath {
			ctx.GlobalSet("macaroonpath",
				filepath.Join(lndDir, defaultMacaroonFilename))
		}
	}

	// Load the specified TLS certificate and build transport credentials
	// with it.
	tlsCertPath := cleanAndExpandPath(ctx.GlobalString("tlscertpath"))
	creds, err := credentials.NewClientTLSFromFile(tlsCertPath, "")
	if err != nil {
		fatal(err)
	}

	// Create a dial options array.
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
	}

	// Only process macaroon credentials if --no-macaroons isn't set and
	// if we're not skipping macaroon processing.
	if !ctx.GlobalBool("no-macaroons") && !skipMacaroons {
		// Load the specified macaroon file.
		macPath := cleanAndExpandPath(ctx.GlobalString("macaroonpath"))
		macBytes, err := ioutil.ReadFile(macPath)
		if err != nil {
			fatal(err)
		}
		mac := &macaroon.Macaroon{}
		if err = mac.UnmarshalBinary(macBytes); err != nil {
			fatal(err)
		}

		macConstraints := []macaroons.Constraint{
			// We add a time-based constraint to prevent replay of the
			// macaroon. It's good for 60 seconds by default to make up for
			// any discrepancy between client and server clocks, but leaking
			// the macaroon before it becomes invalid makes it possible for
			// an attacker to reuse the macaroon. In addition, the validity
			// time of the macaroon is extended by the time the server clock
			// is behind the client clock, or shortened by the time the
			// server clock is ahead of the client clock (or invalid
			// altogether if, in the latter case, this time is more than 60
			// seconds).
			// TODO(aakselrod): add better anti-replay protection.
			macaroons.TimeoutConstraint(ctx.GlobalInt64("macaroontimeout")),

			// Lock macaroon down to a specific IP address.
			macaroons.IPLockConstraint(ctx.GlobalString("macaroonip")),

			// ... Add more constraints if needed.
		}

		// Apply constraints to the macaroon.
		constrainedMac, err := macaroons.AddConstraints(mac, macConstraints...)
		if err != nil {
			fatal(err)
		}

		// Now we append the macaroon credentials to the dial options.
		cred := macaroons.NewMacaroonCredential(constrainedMac)
		opts = append(opts, grpc.WithPerRPCCredentials(cred))
	}

	// We need to use a custom dialer so we can also connect to unix sockets
	// and not just TCP addresses.
	opts = append(
		opts, grpc.WithDialer(
			lncfg.ClientAddressDialer(defaultRpcPort),
		),
	)
	conn, err := grpc.Dial(ctx.GlobalString("rpcserver"), opts...)
	if err != nil {
		fatal(err)
	}

	return conn
}

func main() {
	app := cli.NewApp()
	app.Name = "lncli"
	app.Version = fmt.Sprintf("%s commit=%s", "0.4.2", Commit)
	app.Usage = "control plane for your Lightning Network Daemon (lnd)"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "rpcserver",
			Value: defaultRpcHostPort,
			Usage: "host:port of ln daemon",
		},
		cli.StringFlag{
			Name:  "lnddir",
			Value: defaultLndDir,
			Usage: "path to lnd's base directory",
		},
		cli.StringFlag{
			Name:  "tlscertpath",
			Value: defaultTLSCertPath,
			Usage: "path to TLS certificate",
		},
		cli.BoolFlag{
			Name:  "no-macaroons",
			Usage: "disable macaroon authentication",
		},
		cli.StringFlag{
			Name:  "macaroonpath",
			Value: defaultMacaroonPath,
			Usage: "path to macaroon file",
		},
		cli.Int64Flag{
			Name:  "macaroontimeout",
			Value: 60,
			Usage: "anti-replay macaroon validity time in seconds",
		},
		cli.StringFlag{
			Name:  "macaroonip",
			Usage: "if set, lock macaroon to specific IP address",
		},
	}
	app.Commands = []cli.Command{
		createCommand,
		unlockCommand,
		changePasswordCommand,
		newAddressCommand,
		sendManyCommand,
		sendCoinsCommand,
		connectCommand,
		disconnectCommand,
		openChannelCommand,
		closeChannelCommand,
		closeAllChannelsCommand,
		listPeersCommand,
		walletBalanceCommand,
		channelBalanceCommand,
		getInfoCommand,
		pendingChannelsCommand,
		sendPaymentCommand,
		payInvoiceCommand,
		sendToRouteCommand,
		addInvoiceCommand,
		lookupInvoiceCommand,
		listInvoicesCommand,
		listChannelsCommand,
		closedChannelsCommand,
		listPaymentsCommand,
		describeGraphCommand,
		getChanInfoCommand,
		getNodeInfoCommand,
		queryRoutesCommand,
		getNetworkInfoCommand,
		debugLevelCommand,
		decodePayReqCommand,
		listChainTxnsCommand,
		stopCommand,
		signMessageCommand,
		verifyMessageCommand,
		feeReportCommand,
		updateChannelPolicyCommand,
		forwardingHistoryCommand,
	}

	if err := app.Run(os.Args); err != nil {
		fatal(err)
	}
}

// cleanAndExpandPath expands environment variables and leading ~ in the
// passed path, cleans the result, and returns it.
// This function is taken from https://github.com/btcsuite/btcd
func cleanAndExpandPath(path string) string {
	// Expand initial ~ to OS specific home directory.
	if strings.HasPrefix(path, "~") {
		var homeDir string

		user, err := user.Current()
		if err == nil {
			homeDir = user.HomeDir
		} else {
			homeDir = os.Getenv("HOME")
		}

		path = strings.Replace(path, "~", homeDir, 1)
	}

	// NOTE: The os.ExpandEnv doesn't work with Windows-style %VARIABLE%,
	// but the variables can still be expanded via POSIX-style $VARIABLE.
	return filepath.Clean(os.ExpandEnv(path))
}
