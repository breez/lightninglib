# lightninglib
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
If you want to build lnd and lncli
```
go install -v ./cmd/...
```

**Updating**

To update your version of `lightninglib` to the latest version run the following
commands:
```
cd $GOPATH/src/github.com/breez/lightninglib
git pull
dep ensure -v
```
You can rebuild lnd and lncli
```
go install -v ./cmd/...
```
