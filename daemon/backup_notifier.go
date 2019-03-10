package daemon

import (
	"sync/atomic"

	"github.com/breez/lightninglib/subscribe"
)

// BackupNotifier purpose is to send events when a specific channel
// commitment transaction has changed and backup is needed.
type BackupNotifier struct {
	started uint32
	stopped uint32

	ntfnServer *subscribe.Server
}

// BackupEvent represents a new event where a channel needs bacup.
type BackupEvent struct{}

// NewBackupNotifier creates a new BackupNotifier
func NewBackupNotifier() *BackupNotifier {
	return &BackupNotifier{
		ntfnServer: subscribe.NewServer(),
	}
}

// Start starts the ChannelNotifier and all goroutines it needs to carry out its task.
func (b *BackupNotifier) Start() error {
	if !atomic.CompareAndSwapUint32(&b.started, 0, 1) {
		return nil
	}

	ltndLog.Tracef("BackupNotifier %v starting", b)

	if err := b.ntfnServer.Start(); err != nil {
		return err
	}

	return nil
}

// Stop signals the notifier for a graceful shutdown.
func (b *BackupNotifier) Stop() error {
	if !atomic.CompareAndSwapUint32(&b.stopped, 0, 1) {
		return nil
	}

	return b.ntfnServer.Stop()
}

// SubscribeBackupEvents returns a subscribe.Client that will receive updates
// any time the Server is made aware of a new event.
func (b *BackupNotifier) SubscribeBackupEvents() (*subscribe.Client, error) {
	return b.ntfnServer.Subscribe()
}

// NotifyBackupEvent notifies of a needed backup.
func (b *BackupNotifier) NotifyBackupEvent() {
	// Send the open event to all channel event subscribers.
	event := BackupEvent{}
	if err := b.ntfnServer.SendUpdate(event); err != nil {
		ltndLog.Warnf("Unable to send backup update: %v", err)
	}
}
