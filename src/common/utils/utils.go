package utils

import (
	"fmt"
	"hash/fnv"
	"sync"
)

const FIXED_SEED = "42"

func SpawnWithWG(wg *sync.WaitGroup, fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		fn()
	}()
}

func WithLock(lock *sync.Mutex, fn func()) {
	lock.Lock()
	defer lock.Unlock()
	fn()
}

func WithLockAndReturn[T any](lock *sync.Mutex, fn func() T) T {
	lock.Lock()
	defer lock.Unlock()
	return fn()
}

func GenerateRoutingKey(prefix string, id int) string {
	return fmt.Sprintf("%s_%d", prefix, id)
}

func GetTargetNode(clientId uint64, amountNodes int) int {
	h := fnv.New64a()
	h.Write([]byte(FIXED_SEED))
	fmt.Fprintf(h, "-%d", clientId)

	hashValue := h.Sum64()
	return int(hashValue % uint64(amountNodes))
}

// GetTargetNodeByWord mapea una palabra a un nodo 0..amount-1 de forma determinista.
// Se usa para shardear los candidatos de Q3 por hash de palabra.
func GetTargetNodeByWord(word string, amount int) int {
	h := fnv.New64a()
	h.Write([]byte(FIXED_SEED))
	fmt.Fprintf(h, "-%s", word)

	hashValue := h.Sum64()
	return int(hashValue % uint64(amount))
}
