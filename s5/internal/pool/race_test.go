//go:build race

package pool

// sync.Pool 在竞态检测器下会随机丢弃 Put 的条目（sync/pool.go 的 race.Enabled
// 分支），因此「必然复用 / 0 allocs」类断言在 -race 下不成立。
const raceEnabled = true
