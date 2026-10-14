package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestNormalize：零值即全默认；心跳与读超时配反时必须被纠正。
//
// 这条约束值得单独测：PongWait ≤ PingInterval 时，空闲的健康连接会在发出第一个 ping
// 之前就被读超时判死（症状是「连接莫名其妙断」），靠代码里一行注释拦不住。
func TestNormalize(t *testing.T) {
	t.Run("零值取全默认", func(t *testing.T) {
		got := Normalize(Config{})
		assert.Equal(t, DefaultMaxConnections, got.MaxConnections)
		assert.Equal(t, int64(DefaultReadLimit), got.ReadLimit)
		assert.Equal(t, DefaultMessageBuffer, got.MessageBuffer)
		assert.Equal(t, DefaultReadBuffer, got.ReadBufferSize)
		assert.Equal(t, DefaultWriteBuffer, got.WriteBufferSize)
		assert.Equal(t, DefaultPingInterval, got.PingInterval)
		assert.Equal(t, DefaultPongWait, got.PongWait)
		assert.Equal(t, DefaultWriteWait, got.WriteWait)
	})

	t.Run("负数与零一律取默认", func(t *testing.T) {
		got := Normalize(Config{
			MaxConnections:  -1,
			ReadLimit:       -1,
			MessageBuffer:   -1,
			ReadBufferSize:  -1,
			WriteBufferSize: -1,
			PingInterval:    -time.Second,
			PongWait:        0,
			WriteWait:       -time.Second,
		})
		assert.Equal(t, DefaultMaxConnections, got.MaxConnections)
		assert.Equal(t, int64(DefaultReadLimit), got.ReadLimit)
		assert.Equal(t, DefaultMessageBuffer, got.MessageBuffer)
		assert.Equal(t, DefaultPingInterval, got.PingInterval)
		assert.Equal(t, DefaultPongWait, got.PongWait)
		assert.Equal(t, DefaultWriteWait, got.WriteWait)
	})

	t.Run("读超时不大于心跳时抬到两倍心跳", func(t *testing.T) {
		got := Normalize(Config{PingInterval: 5 * time.Second, PongWait: 5 * time.Second})
		assert.Equal(t, 10*time.Second, got.PongWait)

		got = Normalize(Config{PingInterval: 5 * time.Second, PongWait: time.Second})
		assert.Equal(t, 10*time.Second, got.PongWait)
	})

	t.Run("显式配置原样保留", func(t *testing.T) {
		got := Normalize(Config{
			MaxConnections: 3,
			ReadLimit:      1024,
			MessageBuffer:  2,
			PingInterval:   50 * time.Millisecond,
			PongWait:       time.Second,
			WriteWait:      200 * time.Millisecond,
		})
		assert.Equal(t, 3, got.MaxConnections)
		assert.Equal(t, int64(1024), got.ReadLimit)
		assert.Equal(t, 2, got.MessageBuffer)
		assert.Equal(t, 50*time.Millisecond, got.PingInterval)
		assert.Equal(t, time.Second, got.PongWait)
		assert.Equal(t, 200*time.Millisecond, got.WriteWait)
	})
}
