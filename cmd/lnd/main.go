package main

import (
	"fmt"
	"os"

	"github.com/breez/lightninglib/daemon"
	flags "github.com/jessevdk/go-flags"
)

func main() {
	// Call the "real" main in a nested manner so the defers will properly
	// be executed in the case of a graceful shutdown.
	if err := daemon.LndMain(os.Args, nil); err != nil {
		if e, ok := err.(*flags.Error); ok && e.Type == flags.ErrHelp {
		} else {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}
