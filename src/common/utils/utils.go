package utils

import (
	"sync"
)

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
