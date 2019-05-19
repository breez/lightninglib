package daemon

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/breez/lightninglib/autopilot"
	"github.com/breez/lightninglib/build"
	"github.com/breez/lightninglib/chainntnfs"
	"github.com/breez/lightninglib/chanbackup"
	"github.com/breez/lightninglib/channeldb"
	"github.com/breez/lightninglib/channelnotifier"
	"github.com/breez/lightninglib/contractcourt"
	"github.com/breez/lightninglib/discovery"
	"github.com/breez/lightninglib/htlcswitch"
	"github.com/breez/lightninglib/invoices"
	"github.com/breez/lightninglib/lnrpc/autopilotrpc"
	"github.com/breez/lightninglib/lnrpc/chainrpc"
	"github.com/breez/lightninglib/lnrpc/invoicesrpc"
	"github.com/breez/lightninglib/lnrpc/routerrpc"
	"github.com/breez/lightninglib/lnrpc/signrpc"
	"github.com/breez/lightninglib/lnrpc/walletrpc"
	"github.com/breez/lightninglib/lnwallet"
	"github.com/breez/lightninglib/netann"
	"github.com/breez/lightninglib/routing"
	"github.com/breez/lightninglib/signal"
	"github.com/breez/lightninglib/sweep"
	"github.com/breez/lightninglib/watchtower"
	"github.com/btcsuite/btcd/connmgr"
	"github.com/btcsuite/btclog"
	"github.com/jrick/logrotate/rotator"
	"github.com/lightninglabs/neutrino"
	sphinx "github.com/lightningnetwork/lightning-onion"
)

// Loggers per subsystem.  A single backend logger is created and all subsystem
// loggers created from it will write to the backend.  When adding new
// subsystems, add the subsystem logger variable here and to the
// subsystemLoggers map.
//
// Loggers can not be used before the log rotator has been initialized with a
// log file.  This must be performed early during application startup by
// calling initLogRotator.
var (
	logWriter = &build.LogWriter{}

	// backendLog is the logging backend used to create all subsystem
	// loggers.  The backend must not be used before the log rotator has
	// been initialized, or data races and/or nil pointer dereferences will
	// occur.
	backendLog = btclog.NewBackend(logWriter)

	// logRotator is one of the logging outputs.  It should be closed on
	// application shutdown.
	logRotator *rotator.Rotator

	ltndLog = build.NewSubLogger("LTND", backendLog.Logger)
	lnwlLog = build.NewSubLogger("LNWL", backendLog.Logger)
	peerLog = build.NewSubLogger("PEER", backendLog.Logger)
	discLog = build.NewSubLogger("DISC", backendLog.Logger)
	rpcsLog = build.NewSubLogger("RPCS", backendLog.Logger)
	srvrLog = build.NewSubLogger("SRVR", backendLog.Logger)
	ntfnLog = build.NewSubLogger("NTFN", backendLog.Logger)
	chdbLog = build.NewSubLogger("CHDB", backendLog.Logger)
	fndgLog = build.NewSubLogger("FNDG", backendLog.Logger)
	hswcLog = build.NewSubLogger("HSWC", backendLog.Logger)
	utxnLog = build.NewSubLogger("UTXN", backendLog.Logger)
	brarLog = build.NewSubLogger("BRAR", backendLog.Logger)
	cmgrLog = build.NewSubLogger("CMGR", backendLog.Logger)
	crtrLog = build.NewSubLogger("CRTR", backendLog.Logger)
	btcnLog = build.NewSubLogger("BTCN", backendLog.Logger)
	atplLog = build.NewSubLogger("ATPL", backendLog.Logger)
	cnctLog = build.NewSubLogger("CNCT", backendLog.Logger)
	sphxLog = build.NewSubLogger("SPHX", backendLog.Logger)
	swprLog = build.NewSubLogger("SWPR", backendLog.Logger)
	sgnrLog = build.NewSubLogger("SGNR", backendLog.Logger)
	wlktLog = build.NewSubLogger("WLKT", backendLog.Logger)
	arpcLog = build.NewSubLogger("ARPC", backendLog.Logger)
	invcLog = build.NewSubLogger("INVC", backendLog.Logger)
	nannLog = build.NewSubLogger("NANN", backendLog.Logger)
	wtwrLog = build.NewSubLogger("WTWR", backendLog.Logger)
	ntfrLog = build.NewSubLogger("NTFR", backendLog.Logger)
	irpcLog = build.NewSubLogger("IRPC", backendLog.Logger)
	chnfLog = build.NewSubLogger("CHNF", backendLog.Logger)
	chbuLog = build.NewSubLogger("CHBU", backendLog.Logger)
)

// Initialize package-global logger variables.
func init() {
	lnwallet.UseLogger(lnwlLog)
	discovery.UseLogger(discLog)
	chainntnfs.UseLogger(ntfnLog)
	channeldb.UseLogger(chdbLog)
	htlcswitch.UseLogger(hswcLog)
	connmgr.UseLogger(cmgrLog)
	routing.UseLogger(crtrLog)
	neutrino.UseLogger(btcnLog)
	autopilot.UseLogger(atplLog)
	contractcourt.UseLogger(cnctLog)
	sphinx.UseLogger(sphxLog)
	signal.UseLogger(ltndLog)
	sweep.UseLogger(swprLog)
	signrpc.UseLogger(sgnrLog)
	walletrpc.UseLogger(wlktLog)
	autopilotrpc.UseLogger(arpcLog)
	invoices.UseLogger(invcLog)
	netann.UseLogger(nannLog)
	watchtower.UseLogger(wtwrLog)
	chainrpc.UseLogger(ntfrLog)
	invoicesrpc.UseLogger(irpcLog)
	channelnotifier.UseLogger(chnfLog)
	chanbackup.UseLogger(chbuLog)

	addSubLogger(routerrpc.Subsystem, routerrpc.UseLogger)
}

// addSubLogger is a helper method to conveniently register the logger of a sub
// system.
func addSubLogger(subsystem string, useLogger func(btclog.Logger)) {
	logger := build.NewSubLogger(subsystem, backendLog.Logger)
	useLogger(logger)
	subsystemLoggers[subsystem] = logger
}

// subsystemLoggers maps each subsystem identifier to its associated logger.
var subsystemLoggers = map[string]btclog.Logger{
	"LTND": ltndLog,
	"LNWL": lnwlLog,
	"PEER": peerLog,
	"DISC": discLog,
	"RPCS": rpcsLog,
	"SRVR": srvrLog,
	"NTFN": ntfnLog,
	"CHDB": chdbLog,
	"FNDG": fndgLog,
	"HSWC": hswcLog,
	"UTXN": utxnLog,
	"BRAR": brarLog,
	"CMGR": cmgrLog,
	"CRTR": crtrLog,
	"BTCN": btcnLog,
	"ATPL": atplLog,
	"CNCT": cnctLog,
	"SPHX": sphxLog,
	"SWPR": swprLog,
	"SGNR": sgnrLog,
	"WLKT": wlktLog,
	"ARPC": arpcLog,
	"INVC": invcLog,
	"NANN": nannLog,
	"WTWR": wtwrLog,
	"NTFR": ntfnLog,
	"IRPC": irpcLog,
	"CHNF": chnfLog,
	"CHBU": chbuLog,
}

// initLogRotator initializes the logging rotator to write logs to logFile and
// create roll files in the same directory.  It must be called before the
// package-global log rotator variables are used.
func initLogRotator(logFile string, MaxLogFileSize int, MaxLogFiles int) {
	logDir, _ := filepath.Split(logFile)
	err := os.MkdirAll(logDir, 0700)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log directory: %v\n", err)
		os.Exit(1)
	}
	r, err := rotator.New(logFile, int64(MaxLogFileSize*1024), false, MaxLogFiles)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create file rotator: %v\n", err)
		os.Exit(1)
	}

	pr, pw := io.Pipe()
	go r.Run(pr)

	logWriter.RotatorPipe = pw
	logRotator = r
}

// setLogLevel sets the logging level for provided subsystem.  Invalid
// subsystems are ignored.  Uninitialized subsystems are dynamically created as
// needed.
func setLogLevel(subsystemID string, logLevel string) {
	// Ignore invalid subsystems.
	logger, ok := subsystemLoggers[subsystemID]
	if !ok {
		return
	}

	// Defaults to info if the log level is invalid.
	level, _ := btclog.LevelFromString(logLevel)
	logger.SetLevel(level)
}

// setLogLevels sets the log level for all subsystem loggers to the passed
// level. It also dynamically creates the subsystem loggers as needed, so it
// can be used to initialize the logging system.
func setLogLevels(logLevel string) {
	// Configure all sub-systems with the new logging level.  Dynamically
	// create loggers as needed.
	for subsystemID := range subsystemLoggers {
		setLogLevel(subsystemID, logLevel)
	}
}

// logClosure is used to provide a closure over expensive logging operations so
// don't have to be performed when the logging level doesn't warrant it.
type logClosure func() string

// String invokes the underlying function and returns the result.
func (c logClosure) String() string {
	return c()
}

// newLogClosure returns a new closure over a function that returns a string
// which itself provides a Stringer interface so that it can be used with the
// logging system.
func newLogClosure(c func() string) logClosure {
	return logClosure(c)
}

func BackendLog() *btclog.Backend {
	return backendLog
}
