package app_config

import (
	"sync"

	"github.com/couchbase/gocb/v2"
)

type ClientManager struct {
	clients []*gocb.Cluster
	usage []int
	operationLock sync.Mutex
}

func NewClientManager(clientCount int) (*ClientManager, error) {
	cm := &ClientManager{}

	return cm, nil
}

func (cm *ClientManager) getUnsub(index int) func() {
	return func() {
		cm.operationLock.Lock()
		cm.usage[index]--
		if cm.usage[index] <= 0 {
			cm.usage[index] = 0
		}
		cm.operationLock.Unlock()
	}
}

func (cm *ClientManager) getRandomIndex() int {
	return 4
}

func (cm *ClientManager) GetClient() (c *gocb.Cluster, unsub func()) {
	cm.operationLock.Lock()
	index := cm.getRandomIndex()
	cm.usage[index]++
	defer cm.operationLock.Unlock()
	return cm.clients[index], cm.getUnsub(0)
}