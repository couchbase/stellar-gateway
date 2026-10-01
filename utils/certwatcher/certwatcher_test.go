package certwatcher_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/couchbase/stellar-gateway/utils/certwatcher"
	"go.uber.org/zap"
)

// writeKubernetesSecretLayout builds a directory laid out the way kubelet
// lays out a Secret volume mount: the watched files are symlinks into a
// "..data" symlink, which itself points at a versioned directory. Updating
// the mount is simulated by writing a new versioned directory and
// atomically repointing "..data" at it, exactly as kubelet does.
func writeKubernetesSecretLayout(t *testing.T, root string, version int, contents string) {
	t.Helper()

	versionDir := filepath.Join(root, "..data"+string(rune('0'+version)))
	if err := os.Mkdir(versionDir, 0o755); err != nil {
		t.Fatalf("failed to create version directory: %v", err)
	}

	if err := os.WriteFile(filepath.Join(versionDir, "tls.crt"), []byte(contents), 0o644); err != nil {
		t.Fatalf("failed to write tls.crt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(versionDir, "tls.key"), []byte(contents), 0o644); err != nil {
		t.Fatalf("failed to write tls.key: %v", err)
	}

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

	certPath := filepath.Join(root, "tls.crt")
	keyPath := filepath.Join(root, "tls.key")

	changed := make(chan struct{}, 1)
	w, err := certwatcher.New(zap.NewNop(), []string{certPath, keyPath}, func() {
		select {
		case changed <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}
	defer func() { _ = w.Close() }()

	writeKubernetesSecretLayout(t, root, 2, "rotated")

	select {
	case <-changed:
	case <-time.After(15 * time.Second):
		t.Fatal("watcher did not detect the secret rotation in time")
	}

	got, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("failed to read rotated cert: %v", err)
	}
	if string(got) != "rotated" {
		t.Fatalf("expected rotated content, got %q", string(got))
	}
}

func TestWatcherCloseStopsWatching(t *testing.T) {
	root := t.TempDir()
	writeKubernetesSecretLayout(t, root, 1, "initial")

	certPath := filepath.Join(root, "tls.crt")
	keyPath := filepath.Join(root, "tls.key")

	changed := make(chan struct{}, 1)
	w, err := certwatcher.New(zap.NewNop(), []string{certPath, keyPath}, func() {
		select {
		case changed <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("failed to close watcher: %v", err)
	}

	writeKubernetesSecretLayout(t, root, 2, "rotated")

	select {
	case <-changed:
		t.Fatal("watcher fired onChange after being closed")
	case <-time.After(2 * time.Second):
	}
}
