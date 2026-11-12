//go:build !testhooks

package delivery

// Release builds contain no checkpoint behavior and do not inspect hook env.
func testCheckpoint(string, ClaimedDelivery) {}
