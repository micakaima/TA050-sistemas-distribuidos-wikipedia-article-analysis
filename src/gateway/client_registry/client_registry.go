package clientregistry

import (
	"crypto/rand"
	"encoding/binary"
	"net"
	"sync"
)

type ClientState struct {
	Conn     net.Conn
	ClientID uint64
}

type ClientRegistry struct {
	mutex   sync.Mutex
	clients map[uint64]ClientState
}

func NewClientState(conn net.Conn) ClientState {
	clientId := generateClientId()
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

func generateClientId() uint64 {
	var b [8]byte
	rand.Read(b[:])
	return binary.BigEndian.Uint64(b[:])
}
