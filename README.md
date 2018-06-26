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
