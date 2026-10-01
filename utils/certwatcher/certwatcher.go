package certwatcher

import (
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// debounceInterval coalesces the burst of events a single certificate
// rotation produces (e.g. a Kubernetes Secret volume update swaps its
// "..data" symlink, which shows up as several fsnotify events) into one
// call to onChange.
const debounceInterval = 10 * time.Second

// Watcher watches the directories containing a set of files and invokes
// a callback whenever their contents change. It watches the containing
// directories rather than the files themselves because Kubernetes
// Secret/ConfigMap volume mounts update by atomically re-pointing a
// symlink at a new versioned directory rather than writing the watched
// files in place; a watch on the file itself would miss that.
type Watcher struct {
	logger  *zap.Logger
	watcher *fsnotify.Watcher
	done    chan struct{}
}

// New starts watching the directories containing paths and invokes onChange
// whenever anything within one of those directories changes.
func New(logger *zap.Logger, paths []string, onChange func()) (*Watcher, error) {
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create fsnotify watcher")
	}

	dirs := make(map[string]struct{})
	for _, path := range paths {
		if path == "" {
			continue
		}

		dirs[filepath.Dir(path)] = struct{}{}
	}

	for dir := range dirs {
		if err := fsWatcher.Add(dir); err != nil {
			_ = fsWatcher.Close()
			return nil, errors.Wrapf(err, "failed to watch directory %s", dir)
		}
	}

	w := &Watcher{
		logger:  logger,
		watcher: fsWatcher,
		done:    make(chan struct{}),
	}

	go w.run(onChange)

	return w, nil
}

func (w *Watcher) run(onChange func()) {
	var debounce *time.Timer

	for {
		select {
		case <-w.done:
			if debounce != nil {
				debounce.Stop()
			}

			return

		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}

			w.logger.Debug("detected tls file change",
				zap.String("path", event.Name),
				zap.String("op", event.Op.String()))

			if debounce == nil {
				debounce = time.AfterFunc(debounceInterval, onChange)
			} else {
				debounce.Reset(debounceInterval)
			}

		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}

			w.logger.Warn("error watching tls files", zap.Error(err))
		}
	}
}

// Close stops the watcher. It does not invoke a pending debounced onChange.
func (w *Watcher) Close() error {
	close(w.done)
	return w.watcher.Close()
}
