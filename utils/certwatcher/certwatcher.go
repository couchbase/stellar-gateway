package certwatcher

import (
	"crypto/sha256"
	"maps"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// DefaultPollInterval is how often the watched files are checked for changes.
const DefaultPollInterval = 30 * time.Second

// Watcher periodically polls a set of files and invokes a callback whenever
// the contents of any of them change.
//
// Changes are detected by hashing each file's contents rather than relying
// on filesystem events or modification times. Files are read through any
// symlinks, so this works regardless of how a file is replaced (written in
// place, renamed over, or swapped via a symlink, as a Kubernetes Secret
// volume does), wherever a symlink points, and on filesystems which don't
// deliver change notifications, such as NFS. Rewriting a file with
// identical contents does not count as a change.
type Watcher struct {
	logger   *zap.Logger
	paths    []string
	interval time.Duration
	onChange func()

	hashes  map[string][sha256.Size]byte
	done    chan struct{}
	stopped sync.WaitGroup
}

// New records the current contents of paths and then polls them every
// interval, invoking onChange whenever any of their contents differ from
// the previous poll. It fails if any of the files cannot be read initially.
func New(logger *zap.Logger, paths []string, interval time.Duration, onChange func()) (*Watcher, error) {
	uniquePaths := make([]string, 0, len(paths))
	for _, path := range paths {
		if path == "" || slices.Contains(uniquePaths, path) {
			continue
		}

		uniquePaths = append(uniquePaths, path)
	}

	w := &Watcher{
		logger:   logger,
		paths:    uniquePaths,
		interval: interval,
		onChange: onChange,
		done:     make(chan struct{}),
	}

	hashes, err := w.hashFiles()
	if err != nil {
		return nil, err
	}
	w.hashes = hashes

	w.stopped.Add(1)
	go w.run()

	return w, nil
}

func (w *Watcher) hashFiles() (map[string][sha256.Size]byte, error) {
	hashes := make(map[string][sha256.Size]byte, len(w.paths))
	for _, path := range w.paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read %s", path)
		}

		hashes[path] = sha256.Sum256(data)
	}

	return hashes, nil
}

func (w *Watcher) run() {
	defer w.stopped.Done()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-w.done:
			return

		case <-ticker.C:
			hashes, err := w.hashFiles()
			if err != nil {
				// A file may be briefly missing part way through being replaced,
				// so keep the previous hashes and check again on the next poll.
				w.logger.Warn("failed to read watched file", zap.Error(err))
				continue
			}

			if maps.Equal(hashes, w.hashes) {
				continue
			}

			// The new hashes are recorded even if onChange fails to apply them.
			// When a certificate and key are replaced non-atomically, a poll may
			// observe only one of them updated; the other is then picked up as
			// a further change on a later poll.
			w.hashes = hashes

			w.logger.Debug("detected watched file change")

			w.onChange()
		}
	}
}

// Close stops the watcher, waiting for any in-progress onChange to return.
// onChange is not invoked after Close returns.
func (w *Watcher) Close() {
	close(w.done)
	w.stopped.Wait()
}
