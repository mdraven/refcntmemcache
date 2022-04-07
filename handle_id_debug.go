//go:build debug

package refcntmemcache

import (
	"runtime/debug"
	"sync"
)

type handleId uint64

var callstackMutex = sync.Mutex{}
var currentHandleId = uint64(0)
var handleIdLocations map[handleId]string = make(map[handleId]string)

func getDefaultHandleId() handleId {
	return handleId(0)
}

func registrateNewHandleId() handleId {
	callstackMutex.Lock()
	defer callstackMutex.Unlock()

	currentHandleId++
	handleId := handleId(currentHandleId)

	handleIdLocations[handleId] = string(debug.Stack())

	return handleId
}

func unregistrateHandleId(id handleId) {
	callstackMutex.Lock()
	defer callstackMutex.Unlock()

	delete(handleIdLocations, id)
}

func getHandleIdLocations() []string {
	callstackMutex.Lock()
	defer callstackMutex.Unlock()

	result := make([]string, 0, len(handleIdLocations))

	for _, value := range handleIdLocations {
		result = append(result, value)
	}

	return result
}
