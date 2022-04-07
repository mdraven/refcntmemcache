//go:build !debug

package refcntmemcache

type handleId struct{}

func getDefaultHandleId() handleId {
	return handleId{}
}

func registrateNewHandleId() handleId {
	return handleId{}
}

func unregistrateHandleId(handleId) {}

func getHandleIdLocations() []string {
	return nil
}
