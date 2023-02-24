package app_config

import (
	"crypto/rand"
	"encoding/json"
	"math/big"
	"os"
	"testing"
	"time"
)

type TestConfig struct {
	Thing  string `json:"thing"`
	Other string `json:"other"`
}

func generateRandomString(n int) (string) {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-"
	ret := make([]byte, n)
	for i := 0; i < n; i++ {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return ""
		}
		ret[i] = letters[num.Int64()]
	}

	return string(ret)
}

func TestCreateGenericWatcher(t *testing.T) {
	dir := t.TempDir()
	watcher := NewConfigWatcher[TestConfig](dir + "/test1.json")

	if watcher == nil {
		t.Errorf("Watcher was nil")
	}
}

func TestWatcherReturnsTypeWhenChangesHappenToFile(t *testing.T) {
	config := TestConfig{
		Thing: generateRandomString(10),
		Other: generateRandomString(10),
	}
	dir := t.TempDir()
	watcher := NewConfigWatcher[TestConfig](dir + "/test2.json")

	configChan := make(chan TestConfig)
	configUnsub := watcher.Subscribe(configChan)
	defer configUnsub()

	changes := 0

	go func() {
		for {
			<-configChan
			changes++
		}
	}()
	bytes, _ := json.Marshal(config)
	_ = os.WriteFile(dir + "/test2.json", bytes, 0400)

	time.Sleep(500 * time.Microsecond)
	if changes == 0 {
		t.Errorf("No changes detected")
	}
}

func TestWatcherRepeatedlyReturnsTypeWhenChangesHappenToFile(t *testing.T) {
	config := TestConfig{
		Thing: generateRandomString(10),
		Other: generateRandomString(10),
	}
	dir := t.TempDir()
	watcher := NewConfigWatcher[TestConfig](dir + "/test3.json")

	configChan := make(chan TestConfig)
	configUnsub := watcher.Subscribe(configChan)
	defer configUnsub()

	changes := 0

	go func() {
		for {
			<-configChan
			changes++
		}
	}()

	for i := 0; i < 10; i++ {
		config.Thing = generateRandomString(10)
		config.Other = generateRandomString(10)
		bytes, _ := json.Marshal(config)
		_ = os.WriteFile(dir + "/test3.json", bytes, 0400)
	}

	time.Sleep(100 * time.Microsecond)
	if changes < 10 {
		t.Errorf("Not enough changes detected: %v", changes)
	}
}

func TestWatcherReturnTypeHasUpdatedValues(t *testing.T) {
	config := TestConfig{
		Thing: generateRandomString(10),
		Other: generateRandomString(10),
	}
	dir := t.TempDir()
	bytes, _ := json.Marshal(config)
	_ = os.WriteFile(dir + "/test4.json", bytes, 0400)
	watcher := NewConfigWatcher[TestConfig](dir + "/test4.json")

	configChan := make(chan TestConfig)
	configUnsub := watcher.Subscribe(configChan)
	defer configUnsub()

	config2 := TestConfig{
		Thing: generateRandomString(10),
		Other: generateRandomString(10),
	}

	go func() {
		for {
			c := <-configChan
			if c.Other == config.Other || c.Thing == config.Thing {
				t.Errorf("Expected Values: %v  Actual Values: %v", config2, c )
			}
		}
	}()

	bytes, _ = json.Marshal(config2)
	_ = os.WriteFile(dir + "/test4.json", bytes, 0400)
	time.Sleep(100 * time.Microsecond)

}