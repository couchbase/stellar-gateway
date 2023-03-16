package app_config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
)

type ConfigWatcher[T any] struct {
	configPath string
	watchers   map[uuid.UUID]chan<- T
	watch      *fsnotify.Watcher
}

func (c *ConfigWatcher[T]) Unsub(key uuid.UUID) func() {
	return func() {
		delete(c.watchers, key)
	}
}

func (c *ConfigWatcher[T]) readConfig() T {
	bytes, _ := os.ReadFile(c.configPath)
	var config T

	// we don't care about errors here.  We'll just end up returning an empty config if we can't read the file or it is misformatted.
	_ = json.Unmarshal(bytes, &config)

	return config
}

func (c *ConfigWatcher[T]) broadcastConfig(config T) {
	for _, ch := range c.watchers {
		ch <- config
	}
}

func (c *ConfigWatcher[T]) startWatcher() error {
	go func() {
		for {
			select {
			case event, ok := <-c.watch.Events:
				if !ok {
					return
				}
				if event.Has(fsnotify.Write) {
					config := c.readConfig()
					c.broadcastConfig(config)
				}
			case err, ok := <-c.watch.Errors:
				if !ok {
					return
				}
				fmt.Println("error: ", err)
			}
		}
	}()

	err := c.watch.Add(c.configPath)
	if err != nil {
		return err
	}

	return nil
}

func checkExists(file string) bool {
	 if _, err := os.Stat(file); err != nil {
		return false
	 }
	 return true
}

func NewConfigWatcher[T any](path string) *ConfigWatcher[T] {

	if !checkExists(path) {
		file, err := os.Create(path)
		if err != nil {
			return nil
		}
		file.Close()
	}

	configWatcher := &ConfigWatcher[T]{
		configPath: path,
		watchers:   map[uuid.UUID]chan<- T{},
	}

	watch, err := fsnotify.NewWatcher()
	if err != nil {
		return nil
	}

	configWatcher.watch = watch

	err = configWatcher.startWatcher()
	if err != nil {
		return nil
	}
	return configWatcher
}

func (c *ConfigWatcher[T]) Subscribe(ch chan<- T) func() {
	id := uuid.New()
	c.watchers[id] = ch
	return c.Unsub(id)
}