package certwatcher_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/stellar-gateway/utils/certwatcher"
	"go.uber.org/zap"
)

const testPollInterval = 20 * time.Millisecond

func writeFile(t *testing.T, path, contents string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("failed to write %s: %v", path, err)
	}
}

// startWatcher starts a watcher over paths and returns a channel which
// receives once for each onChange invocation.
func startWatcher(t *testing.T, paths ...string) chan struct{} {
	t.Helper()

	changed := make(chan struct{}, 10)
	w, err := certwatcher.New(zap.NewNop(), paths, testPollInterval, func() {
		changed <- struct{}{}
	})
	if err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}
	t.Cleanup(w.Close)

	return changed
}

func expectChange(t *testing.T, changed chan struct{}) {
	t.Helper()

	select {
	case <-changed:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not detect the change in time")
	}
}

func expectNoChange(t *testing.T, changed chan struct{}) {
	t.Helper()

	select {
	case <-changed:
		t.Fatal("watcher unexpectedly reported a change")
	case <-time.After(10 * testPollInterval):
	}
}

func TestWatcherDetectsInPlaceWrite(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	writeFile(t, certPath, "cert-1")
	writeFile(t, keyPath, "key-1")

	changed := startWatcher(t, certPath, keyPath)

	writeFile(t, certPath, "cert-2")
	expectChange(t, changed)

	writeFile(t, keyPath, "key-2")
	expectChange(t, changed)
}

func TestWatcherDetectsRenameOver(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	writeFile(t, certPath, "cert-1")

	changed := startWatcher(t, certPath)

	tmpPath := filepath.Join(dir, "tls.crt.tmp")
	writeFile(t, tmpPath, "cert-2")
	if err := os.Rename(tmpPath, certPath); err != nil {
		t.Fatalf("failed to rename over cert: %v", err)
	}

	expectChange(t, changed)
}

// writeKubernetesSecretLayout builds a directory laid out the way kubelet
// lays out a Secret volume mount: the watched files are symlinks into a
// "..data" symlink, which itself points at a versioned directory. Updating
// the mount is simulated by writing a new versioned directory and
// atomically repointing "..data" at it, exactly as kubelet does.
func writeKubernetesSecretLayout(t *testing.T, root string, version int, contents string) {
	t.Helper()

	versionDir := filepath.Join(root, "..data"+strconv.Itoa(version))
	if err := os.Mkdir(versionDir, 0o755); err != nil {
		t.Fatalf("failed to create version directory: %v", err)
	}

	writeFile(t, filepath.Join(versionDir, "tls.crt"), contents)
	writeFile(t, filepath.Join(versionDir, "tls.key"), contents)

	dataLink := filepath.Join(root, "..data")
	tmpLink := filepath.Join(root, "..data_tmp")
	if err := os.Symlink(versionDir, tmpLink); err != nil {
		t.Fatalf("failed to create ..data symlink: %v", err)
	}
	if err := os.Rename(tmpLink, dataLink); err != nil {
		t.Fatalf("failed to atomically swap ..data symlink: %v", err)
	}

	for _, name := range []string{"tls.crt", "tls.key"} {
		link := filepath.Join(root, name)
		_ = os.Remove(link)
		if err := os.Symlink(filepath.Join("..data", name), link); err != nil {
			t.Fatalf("failed to create %s symlink: %v", name, err)
		}
	}
}

func TestWatcherDetectsKubernetesSecretRotation(t *testing.T) {
	root := t.TempDir()
	writeKubernetesSecretLayout(t, root, 1, "initial")

	changed := startWatcher(t, filepath.Join(root, "tls.crt"), filepath.Join(root, "tls.key"))

	writeKubernetesSecretLayout(t, root, 2, "rotated")

	expectChange(t, changed)
}

func TestWatcherDetectsSymlinkTargetChangeInAnotherDirectory(t *testing.T) {
	targetDir := t.TempDir()
	targetPath := filepath.Join(targetDir, "tls.crt")
	writeFile(t, targetPath, "cert-1")

	linkPath := filepath.Join(t.TempDir(), "tls.crt")
	if err := os.Symlink(targetPath, linkPath); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	changed := startWatcher(t, linkPath)

	writeFile(t, targetPath, "cert-2")

	expectChange(t, changed)
}

func TestWatcherIgnoresUnchangedContents(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	writeFile(t, certPath, "cert-1")

	changed := startWatcher(t, certPath)

	writeFile(t, certPath, "cert-1")
	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(certPath, future, future); err != nil {
		t.Fatalf("failed to update modification time: %v", err)
	}

	expectNoChange(t, changed)
}

func TestWatcherIgnoresUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	writeFile(t, certPath, "cert-1")

	changed := startWatcher(t, certPath)

	writeFile(t, filepath.Join(dir, "unrelated"), "data")

	expectNoChange(t, changed)
}

func TestWatcherRecoversFromTemporarilyMissingFile(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	writeFile(t, certPath, "cert-1")

	changed := startWatcher(t, certPath)

	if err := os.Remove(certPath); err != nil {
		t.Fatalf("failed to remove cert: %v", err)
	}
	expectNoChange(t, changed)

	writeFile(t, certPath, "cert-2")
	expectChange(t, changed)
}

func TestNewFailsWhenFileMissing(t *testing.T) {
	_, err := certwatcher.New(zap.NewNop(), []string{filepath.Join(t.TempDir(), "missing")}, testPollInterval, func() {})
	if err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func TestWatcherCloseStopsWatching(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	writeFile(t, certPath, "cert-1")

	changed := make(chan struct{}, 1)
	w, err := certwatcher.New(zap.NewNop(), []string{certPath}, testPollInterval, func() {
		changed <- struct{}{}
	})
	if err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}

	w.Close()

	writeFile(t, certPath, "cert-2")

	expectNoChange(t, changed)
}
