package routing

import (
	"testing"

	"github.com/breez/lightninglib/channeldb"
	"github.com/breez/lightninglib/lnwire"
	"github.com/breez/lightninglib/routing/route"
)

func TestRequestRoute(t *testing.T) {
	const (
		height = 10
	)

	findPath := func(g *graphParams, r *RestrictParams,
		source, target route.Vertex, amt lnwire.MilliSatoshi) (
		[]*channeldb.ChannelEdgePolicy, error) {

		// We expect find path to receive a cltv limit excluding the
		// final cltv delta.
		if *r.CltvLimit != 22 {
			t.Fatal("wrong cltv limit")
		}

		path := []*channeldb.ChannelEdgePolicy{
			{
				Node: &channeldb.LightningNode{},
			},
		}

		return path, nil
	}

	session := &paymentSession{
		mc: &missionControl{
			selfNode: &channeldb.LightningNode{},
		},
		pruneViewSnapshot: graphPruneView{},
		pathFinder:        findPath,
	}

	cltvLimit := uint32(30)
	finalCltvDelta := uint16(8)

	payment := &LightningPayment{
		CltvLimit:      &cltvLimit,
		FinalCLTVDelta: &finalCltvDelta,
	}

	route, err := session.RequestRoute(payment, height, finalCltvDelta)
	if err != nil {
		t.Fatal(err)
	}

	// We expect an absolute route lock value of height + finalCltvDelta
	if route.TotalTimeLock != 18 {
		t.Fatalf("unexpected total time lock of %v",
			route.TotalTimeLock)
	}
}
