package clientregistry

import (
	"net"
	"sync"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type ClientState struct {
	Conn         net.Conn
	ClientID     uint64
	EOFsReceived int
}

type ClientRegistry struct {
	mutex   sync.Mutex
	clients map[uint64]ClientState
}

func NewClientState(conn net.Conn, gatewayID uint16) ClientState {
	clientId := utils.GenerateClientId(gatewayID)
	return ClientState{
		Conn:     conn,
		ClientID: clientId,
	}
}

func NewClientRegistry() ClientRegistry {
	return ClientRegistry{
		mutex:   sync.Mutex{},
		clients: map[uint64]ClientState{},
	}
}

func (registry *ClientRegistry) Get(id uint64) (ClientState, bool) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	client, ok := registry.clients[id]
	return client, ok
}

func (registry *ClientRegistry) Add(client ClientState) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.clients[client.ClientID] = client
}

func (registry *ClientRegistry) Remove(id uint64) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	delete(registry.clients, id)
}

func (registry *ClientRegistry) WithLock(action func(map[uint64]ClientState)) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	action(registry.clients)
}

func (registry *ClientRegistry) AddEOF(id uint64) int {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if client, ok := registry.clients[id]; ok {
		client.EOFsReceived++
		registry.clients[id] = client
		return client.EOFsReceived
	}
	return 0
}
