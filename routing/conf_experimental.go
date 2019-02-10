// +build experimental

package routing

// Conf exposes the experimental command line routing configurations.
type Conf struct {
	AssumeChannelValid   bool `long:"assumechanvalid" description:"Skip checking channel spentness during graph validation. (default: false)"`
	UpdateGraphOnStartup bool `long:"updategraphonstratup" description "Update the graph on startup. (default: true)"`
}

// UseAssumeChannelValid returns true if the router should skip checking for
// spentness when processing channel updates and announcements.
func (c *Conf) UseAssumeChannelValid() bool {
	return c.AssumeChannelValid
}

// UseUpdateGraphOnStartup return false if the router should skip updating
// the graph on startup
func (c *Conf) UseUpdateGraphOnStartup() bool {
	return c.UpdateGraphOnStartup
}
