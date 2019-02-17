module github.com/breez/lightninglib

require (
	git.schwanenlied.me/yawning/bsaes.git v0.0.0-20180720073208-c0276d75487e // indirect
	github.com/NebulousLabs/fastrand v0.0.0-20180208210444-3cf7173006a0 // indirect
	github.com/NebulousLabs/go-upnp v0.0.0-20180202185039-29b680b06c82
	github.com/Yawning/aez v0.0.0-20180114000226-4dad034d9db2
	github.com/aead/chacha20 v0.0.0-20180709150244-8b13a72661da // indirect
	github.com/btcsuite/btcd v0.0.0-20190115013929-ed77733ec07d
	github.com/btcsuite/btclog v0.0.0-20170628155309-84c8d2346e9f
	github.com/btcsuite/btcutil v0.0.0-20190112041146-bf1e1be93589
	github.com/btcsuite/btcwallet v0.0.0-20190123033236-ba03278a64bc
	github.com/btcsuite/fastsha256 v0.0.0-20160815193821-637e65642941
	github.com/btcsuite/goleveldb v1.0.0 // indirect
	github.com/coreos/bbolt v1.3.2
	github.com/davecgh/go-spew v1.1.1
	github.com/go-errors/errors v1.0.1
	github.com/golang/protobuf v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.5.1
	github.com/jackpal/gateway v1.0.4
	github.com/jackpal/go-nat-pmp v0.0.0-20170405195558-28a68d0c24ad
	github.com/jessevdk/go-flags v0.0.0-20170926144705-f88afde2fa19
	github.com/jrick/logrotate v1.0.0
	github.com/juju/errors v0.0.0-20180806074554-22422dad46e1 // indirect
	github.com/juju/loggo v0.0.0-20180524022052-584905176618 // indirect
	github.com/juju/retry v0.0.0-20160928201858-1998d01ba1c3 // indirect
	github.com/juju/testing v0.0.0-20180807044555-c84dd6ba038a // indirect
	github.com/juju/utils v0.0.0-20180808125547-9dfc6dbfb02b // indirect
	github.com/juju/version v0.0.0-20180108022336-b64dbd566305 // indirect
	github.com/kkdai/bstream v0.0.0-20181106074824-b3251f7901ec
	github.com/kr/pretty v0.1.0 // indirect
	github.com/lightninglabs/neutrino v0.0.0-20190115022559-351f5f06c6af
	github.com/lightningnetwork/lightning-onion v0.0.0-20180605012408-ac4d9da8f1d6
	github.com/ltcsuite/ltcd v0.0.0-20170901085657-5f654d5faab9
	github.com/miekg/dns v0.0.0-20171125082028-79bfde677fa8
	github.com/rogpeppe/fastuuid v0.0.0-20150106093220-6724a57986af // indirect
	github.com/tv42/zbase32 v0.0.0-20160707012821-501572607d02
	github.com/urfave/cli v1.18.0
	golang.org/x/crypto v0.0.0-20181127143415-eb0de9b17e85
	golang.org/x/net v0.0.0-20181106065722-10aee1819953
	golang.org/x/sync v0.0.0-20181108010431-42b317875d0f // indirect
	golang.org/x/time v0.0.0-20180412165947-fbb02b2291d2
	google.golang.org/genproto v0.0.0-20181127195345-31ac5d88444a
	google.golang.org/grpc v1.16.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/errgo.v1 v1.0.0-20180502123906-c17903c6b19d // indirect
	gopkg.in/macaroon-bakery.v2 v2.0.1
	gopkg.in/macaroon.v2 v2.0.0
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
	launchpad.net/gocheck v0.0.0-20140225173054-000000000087 // indirect
)

replace github.com/btcsuite/btcd v0.0.0-20190115013929-ed77733ec07d => github.com/breez/btcd v0.0.0-20190217135408-786ef411fa65

replace github.com/lightninglabs/neutrino v0.0.0-20190115022559-351f5f06c6af => github.com/breez/neutrino v0.0.0-20190217133150-08c3c9e7d6d5

replace github.com/btcsuite/btcwallet v0.0.0-20190123033236-ba03278a64bc => github.com/breez/btcwallet v0.0.0-20190217125204-7c7100b2373d
