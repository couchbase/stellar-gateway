package topology

type Endpoint struct {
	NodeID        string
	AdvertiseAddr string
	AdvertisePort int
	ServerGroup   string
}

type Snapshot struct {
	Endpoints []Endpoint
}

// TODO(brett19): Add context support everywhere so things are deadlineable/cancellable.
// TODO(brett19): Add cancellable watch handling (cancel after the stream is done).
// It might be acceptable to have the watch only last as long as the context does, and have there
// be no way to timeout the initial setup.  We may need to make it non-blocking for that to work though...

/*
Note that the Join/Leave calls must not be called concurrently.  It is however
safe to concurrently call Join or Leave alongside Watch/Get calls.
*/
type Provider interface {
	Join(localConfig *Endpoint) error
	Leave() error

	Watch() (chan *Snapshot, error)
	Get() (*Snapshot, error)
}
