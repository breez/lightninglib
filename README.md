# lightninglib
`lightninglib` is a fork of [lnd](https://github.com/lightningnetwork/lnd) which aims to be usable as a go library inside any application, including mobile apps.

### Preliminaries

This project uses `dep` to manage dependencies as well as to provide *reproducible builds*.

```
go get -u github.com/golang/dep/cmd/dep
```

### Installing

```
go get -d github.com/breez/lightninglib
cd $GOPATH/src/github.com/breez/lightninglib
dep ensure -v
```
Optionally, if you want to build lnd and lncli
```
go install -v ./cmd/...
```

### Using the library
You can for instance use lightninglib in a mobile application using [gomobile](https://godoc.org/golang.org/x/mobile).

First create a file containing the following code:

```
package lightningmobile

import (
        "fmt"
        "os"

        "github.com/breez/lightninglib/daemon"
)

func Start(appDir string) {
        go func() {
                if err := daemon.LndMain([]string{"lightningmobile", "--lnddir", appDir}); err != nil {
                        fmt.Fprintln(os.Stderr, err)
                        os.Exit(1)
                }
        }()
}
```

Then run
```
gomobile bind -target=android -tags="android" -o lightningmobile.aar lightningmobile
```
You can now use the aar file in your android application.

In https://github.com/golang/go/wiki/Mobile you'll find more informations about using go in mobile apps.

### Updating

To update your version of `lightninglib` to the latest version run the following
commands:
```
cd $GOPATH/src/github.com/breez/lightninglib
git pull
dep ensure -v
```
