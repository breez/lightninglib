package watchtower

import (
	"github.com/breez/lightninglib/watchtower/lookout"
	"github.com/breez/lightninglib/watchtower/wtserver"
)

// DB abstracts the persistent functionality required to run the watchtower
// daemon. It composes the database interfaces required by the lookout and
// wtserver subsystems.
type DB interface {
	lookout.DB
	wtserver.DB
}
