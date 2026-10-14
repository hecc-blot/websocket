package config

import (
	"net/http"
	"time"
)

// 默认参数。
const (
	DefaultMaxConnections = 1000             // 最大并发连接数
	DefaultReadLimit      = 512 * 1024       // 单条消息大小上限（512KB）
	DefaultMessageBuffer  = 16               // 消息通道缓冲长度
	DefaultReadBuffer     = 4096             // 底层读缓冲（字节）
	DefaultWriteBuffer    = 4096             // 底层写缓冲（字节）
	DefaultPingInterval   = 30 * time.Second // 服务端 ping 间隔
	DefaultPongWait       = 60 * time.Second // 等待对端响应的超时
	DefaultWriteWait      = 10 * time.Second // 单次写超时
)

// Config WebSocket 服务配置。零值即全默认，构造时由 Normalize 补全。
//
// 时长字段用 time.Duration 而不是仓库其他 config 的「秒」：本配置以编程传入为主，
// 且毫秒级间隔能让测试直接把心跳压短（sse 是靠包级 var heartbeatInterval 打补丁）。
// 需要从 yaml 绑定时 viper 的默认解码 hook 认 "30s" 这类字符串。
type Config struct {
	// MaxConnections 并发连接上限，<=0 用默认 1000；超出时握手返回 HTTP 503。
	MaxConnections int `mapstructure:"max_connections"`
	// ReadLimit 单条消息大小上限（字节），<=0 用默认 512KB；超限时关闭帧码为 1009。
	ReadLimit int64 `mapstructure:"read_limit"`
	// MessageBuffer 消息通道缓冲长度，<=0 用默认 16。
	// 业务消费不及时会让读协程阻塞在投递上（背压，不丢消息），不要靠调大它掩盖慢消费。
	MessageBuffer int `mapstructure:"message_buffer"`
	// ReadBufferSize / WriteBufferSize 底层读写缓冲（字节），<=0 用默认 4096。
	ReadBufferSize  int `mapstructure:"read_buffer_size"`
	WriteBufferSize int `mapstructure:"write_buffer_size"`
	// PingInterval 服务端 ping 间隔，<=0 用默认 30s。
	PingInterval time.Duration `mapstructure:"ping_interval"`
	// PongWait 等待对端响应的超时（pong 或任何入站流量都算），<=0 用默认 60s。
	// 必须大于 PingInterval，否则空闲的健康连接会被自己判死（见 Normalize）。
	PongWait time.Duration `mapstructure:"pong_wait"`
	// WriteWait 单次写的超时，<=0 用默认 10s。
	WriteWait time.Duration `mapstructure:"write_wait"`
	// Subprotocols 服务端支持的子协议，按优先级排列。为空则不协商，
	// Conn.Subprotocol() 恒为空串。
	Subprotocols []string `mapstructure:"subprotocols"`
	// AllowOrigins 允许的 Origin 白名单（如 "https://app.example.com"），大小写不敏感；
	// 含 "*" 表示放行任意来源（仅供开发）。为空则只放行同源。
	//
	// 注意：Origin 只在浏览器里可信，白名单防的是跨站请求伪造，挡不住非浏览器客户端 ——
	// 鉴权该走 Conn.Query / Conn.Header。
	AllowOrigins []string `mapstructure:"allow_origins"`
	// CheckOrigin 自定义 Origin 校验，非 nil 时优先于 AllowOrigins。
	CheckOrigin func(r *http.Request) bool `mapstructure:"-"`
}

// Normalize 补全默认值，供构造服务时调用。
func Normalize(cfg Config) Config {
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = DefaultMaxConnections
	}
	if cfg.ReadLimit <= 0 {
		cfg.ReadLimit = DefaultReadLimit
	}
	if cfg.MessageBuffer <= 0 {
		cfg.MessageBuffer = DefaultMessageBuffer
	}
	if cfg.ReadBufferSize <= 0 {
		cfg.ReadBufferSize = DefaultReadBuffer
	}
	if cfg.WriteBufferSize <= 0 {
		cfg.WriteBufferSize = DefaultWriteBuffer
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = DefaultPingInterval
	}
	if cfg.PongWait <= 0 {
		cfg.PongWait = DefaultPongWait
	}
	if cfg.WriteWait <= 0 {
		cfg.WriteWait = DefaultWriteWait
	}
	// 心跳必须早于读超时：ping 晚于读 deadline 才发出去时，空闲的健康连接会被判死。
	// 用户配反了不该让连接莫名断开，这里直接抬到两倍心跳。
	if cfg.PongWait <= cfg.PingInterval {
		cfg.PongWait = 2 * cfg.PingInterval
	}
	return cfg
}
