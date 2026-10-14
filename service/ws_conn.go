package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"github.com/hecc-blot/core/util"
	wsConfig "github.com/hecc-blot/websocket/config"
	wsContract "github.com/hecc-blot/websocket/contract"
)

// maxCloseReason 关闭帧 reason 的字节上限：控制帧载荷 ≤125 字节，减去 2 字节状态码。
const maxCloseReason = 123

// wsConn 单条连接的读写实现，框架侧唯一持有底层连接的地方。
type wsConn struct {
	conn     *websocket.Conn
	ctx      context.Context
	cancel   context.CancelFunc
	messages chan wsContract.Message
	cfg      wsConfig.Config

	// 握手请求快照。升级之后 *http.Request 已脱离 net/http 生命周期，
	// 且 Body 被框架的 bodySizeLimit 包成 MaxBytesReader —— 快照掉比持有请求更省心。
	headers    http.Header
	query      url.Values
	remoteAddr string

	// writeMu 串行化业务写：底层库把并发写当作致命错误（panic），不是返回 error。
	// 控制帧走 WriteControl，底层有独立互斥，不参与这把锁。
	writeMu sync.Mutex
	// closeOnce 收敛三条关闭路径（业务主动关 / 读协程退出 / 停机钩子），保证关闭帧只发一次。
	closeOnce sync.Once
}

// newWsConn 构造连接对象并启动读协程与心跳协程。
func newWsConn(raw *websocket.Conn, r *http.Request, cfg wsConfig.Config) *wsConn {
	ctx, cancel := context.WithCancel(r.Context())
	c := &wsConn{
		conn:       raw,
		ctx:        ctx,
		cancel:     cancel,
		messages:   make(chan wsContract.Message, cfg.MessageBuffer),
		cfg:        cfg,
		headers:    r.Header.Clone(),
		query:      r.URL.Query(),
		remoteAddr: r.RemoteAddr,
	}

	// 显式清一次读写 deadline。net/http 在 hijack 时已经 SetDeadline(零值) 清过
	// （server.go 的 hijackLocked），这里重述意图：本连接不受服务端 read/write timeout
	// 约束，之后每次读/写前都用「相对当前时间」重设，绝不算一次绝对时间点。
	_ = raw.SetReadDeadline(time.Time{})
	_ = raw.SetWriteDeadline(time.Time{})

	raw.SetReadLimit(cfg.ReadLimit)
	// pong 处理器：顺延读 deadline，心跳才能真正维持空闲连接。
	raw.SetPongHandler(func(string) error {
		return raw.SetReadDeadline(time.Now().Add(cfg.PongWait))
	})
	// 不改 ping / close 处理器，用底层库默认行为：
	//   - 默认 ping 处理器自动回 pong（WriteControl，与业务写并发安全）；
	//   - 默认 close 处理器原样回一个关闭帧（回显礼仪），随后 ReadMessage 返回 *CloseError。

	// 这两个协程不在 gin.Recovery 的覆盖范围内（框架的 recover 只管 handler 所在的协程），
	// 裸 go 里的 panic 会直接带走整个进程 —— 走 core/util.Go 让它被捕获并按 ctx 归因 traceId。
	util.Go(ctx, c.readPump)
	util.Go(ctx, c.pingLoop)

	return c
}

// readPump 唯一的读者：持续读帧、顺延读 deadline、把业务消息投递给业务。
//
// defer 顺序承重（LIFO，注册晚的先执行）：先关连接（取消 ctx、断开 TCP，
// 解除可能阻塞的写与渠道），再关消息通道让业务退出循环。
func (c *wsConn) readPump() {
	code, reason := wsContract.CloseNormalClosure, ""
	defer close(c.messages)
	defer func() { _ = c.Close(code, reason) }()

	for {
		// 每次读前重设读 deadline：pong 处理器里也会顺延，
		// 于是「心跳正常」与「有业务流量」都能维持连接。
		_ = c.conn.SetReadDeadline(time.Now().Add(c.cfg.PongWait))
		mt, data, err := c.conn.ReadMessage()
		if err != nil {
			code, reason = readCloseCode(err)
			return
		}
		select {
		case c.messages <- wsContract.Message{Type: wsContract.MessageType(mt), Data: data}:
		case <-c.ctx.Done():
			return
		}
	}
}

// readCloseCode 把读错误翻译成关闭码与原因，用于收尾时告知客户端。
func readCloseCode(err error) (wsContract.CloseCode, string) {
	var closeErr *websocket.CloseError
	switch {
	case errors.As(err, &closeErr):
		// 客户端主动关闭（或 TCP 断开被包装成 1006）：属正常断开，
		// 底层默认 close 处理器已回显过关闭帧。
		return wsContract.CloseNormalClosure, ""
	case errors.Is(err, websocket.ErrReadLimit):
		// 底层库已按 1009 发过关闭帧，这里只是补上我方上下文。
		return wsContract.CloseMessageTooBig, "message too big"
	case isTimeout(err):
		return wsContract.CloseGoingAway, "heartbeat timeout"
	default:
		return wsContract.CloseProtocolError, ""
	}
}

// isTimeout 判断是否为读超时（读 deadline 到期）。
func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// pingLoop 定期发 ping 维持连接。
//
// 走 WriteControl 而非 WriteMessage：控制帧允许插在消息分片之间，底层也有独立互斥，
// 因此不必抢 writeMu，也不会被业务的长写阻塞。
func (c *wsConn) pingLoop() {
	ticker := time.NewTicker(c.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			// 写失败不作致命处理（可能是瞬时超时），跳过本次心跳即可；
			// 真正断开由读侧的超时兜底。
			_ = c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(c.cfg.WriteWait))
		}
	}
}

// Close 幂等关闭：发关闭帧、取消 ctx、断开底层连接。
func (c *wsConn) Close(code wsContract.CloseCode, reason string) error {
	var err error
	c.closeOnce.Do(func() {
		// 保留码不得出现在关闭帧里，否则对端会判协议错误
		switch code {
		case wsContract.CloseNoStatusReceived, wsContract.CloseAbnormalClosure, wsContract.CloseTLSHandshake:
			code = wsContract.CloseNormalClosure
		}
		err = c.conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(int(code), truncateCloseReason(reason)),
			time.Now().Add(c.cfg.WriteWait),
		)
		// 对端已经发过关闭帧时底层返回 ErrCloseSent —— 关闭礼仪里「只发一个关闭帧」
		// 才是对的，不算失败。
		if errors.Is(err, websocket.ErrCloseSent) {
			err = nil
		}
		c.cancel()
		_ = c.conn.Close() // 断开 TCP：解除仍在阻塞的读（读协程）与写（业务）
	})
	// err 是本次调用的局部变量：只有首次调用（真正发帧的那次）会拿到结果，
	// 重复调用返回 nil —— 幂等语义下「已经关过了」不是一个可行动的失败。
	return err
}

// Write 发送一条消息，并发安全。
func (c *wsConn) Write(msgType wsContract.MessageType, data []byte) error {
	mt, err := mapMessageType(msgType)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// 每次写前重设写 deadline（相对时间）：一次卡住的写最多占用 WriteWait，
	// 不会拖住整个关闭流程。
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteWait))
	return c.conn.WriteMessage(mt, data)
}

// WriteJSON 把 v 序列化为 JSON 并以文本消息发送。
//
// 不用底层库的 WriteJSON：它内部不持 writeMu，且用 json.Encoder 会多出一个换行。
func (c *wsConn) WriteJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Write(wsContract.TextMessage, data)
}

// Messages 返回消息接收通道。
func (c *wsConn) Messages() <-chan wsContract.Message {
	return c.messages
}

// RemoteAddr 返回客户端地址。
func (c *wsConn) RemoteAddr() string {
	return c.remoteAddr
}

// Query 取握手请求的查询参数。
func (c *wsConn) Query(key string) string {
	return c.query.Get(key)
}

// Header 取握手请求头。
func (c *wsConn) Header(key string) string {
	return c.headers.Get(key)
}

// Subprotocol 返回协商出的子协议。
func (c *wsConn) Subprotocol() string {
	return c.conn.Subprotocol()
}

// shutdown 停机时的关闭路径：1001 going away，客户端可据此重连。
func (c *wsConn) shutdown() {
	_ = c.Close(wsContract.CloseGoingAway, "server is shutting down")
}

// mapMessageType 把契约消息类型映射到底层帧类型。
func mapMessageType(msgType wsContract.MessageType) (int, error) {
	switch msgType {
	case wsContract.TextMessage:
		return websocket.TextMessage, nil
	case wsContract.BinaryMessage:
		return websocket.BinaryMessage, nil
	default:
		return 0, fmt.Errorf("websocket: unsupported message type %d", msgType)
	}
}

// truncateCloseReason 把关闭原因截到控制帧允许的长度（≤123 字节）。
//
// 底层库不会截断，超长会直接拒绝整个关闭帧（连关闭码也发不出去）；
// 而按字节硬截可能劈开多字节字符 —— 对端收到非法 UTF-8 的关闭原因会判协议错误（1007）。
func truncateCloseReason(reason string) string {
	if len(reason) <= maxCloseReason {
		return reason
	}
	b := []byte(reason)[:maxCloseReason]
	for !utf8.Valid(b) {
		b = b[:len(b)-1]
	}
	return string(b)
}
