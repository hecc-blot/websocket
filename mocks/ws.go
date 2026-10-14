package mocks

import (
	"context"
	"encoding/json"
	"sync"

	ws "github.com/hecc-blot/websocket/contract"
)

// MockConn 是 Conn 接口的 mock 实现，记录写入的消息与关闭调用。
//
// 带锁：WebSocket 用例天然并发（框架的读协程与心跳协程一直在跑），
// 无锁版本在 -race 下会踩竞态。
type MockConn struct {
	mu sync.Mutex

	// WriteFn 定制 Write 行为；为 nil 时记录到 Sent 并返回 nil。
	WriteFn func(msgType ws.MessageType, data []byte) error
	// CloseFn 定制 Close 行为；为 nil 时只做记录并返回 nil。
	CloseFn func(code ws.CloseCode, reason string) error
	// MessagesFn 定制 Messages() 返回的通道；为 nil 时返回一个已关闭的空通道。
	MessagesFn func() <-chan ws.Message

	// Addr / QueryValues / HeaderValue / SubprotocolValue 供握手信息类断言使用。
	Addr             string
	QueryValues      map[string]string
	HeaderValue      map[string]string
	SubprotocolValue string

	sent        []ws.Message
	sentJSON    [][]byte
	closeCount  int
	closeCode   ws.CloseCode
	closeReason string
	done        chan ws.Message
}

// Messages 返回消息通道。
func (m *MockConn) Messages() <-chan ws.Message {
	if m.MessagesFn != nil {
		return m.MessagesFn()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done == nil {
		m.done = make(chan ws.Message)
		close(m.done)
	}
	return m.done
}

// Write 记录一条消息。
func (m *MockConn) Write(msgType ws.MessageType, data []byte) error {
	if m.WriteFn != nil {
		return m.WriteFn(msgType, data)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, ws.Message{Type: msgType, Data: data})
	return nil
}

// WriteJSON 记录一条 JSON 文本消息。
func (m *MockConn) WriteJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.sentJSON = append(m.sentJSON, data)
	m.mu.Unlock()
	return m.Write(ws.TextMessage, data)
}

// Close 记录一次关闭调用。
func (m *MockConn) Close(code ws.CloseCode, reason string) error {
	m.mu.Lock()
	m.closeCount++
	m.closeCode = code
	m.closeReason = reason
	m.mu.Unlock()
	if m.CloseFn != nil {
		return m.CloseFn(code, reason)
	}
	return nil
}

// RemoteAddr 返回预设地址。
func (m *MockConn) RemoteAddr() string { return m.Addr }

// Query 返回预设的查询参数。
func (m *MockConn) Query(key string) string { return m.QueryValues[key] }

// Header 返回预设的请求头。
func (m *MockConn) Header(key string) string { return m.HeaderValue[key] }

// Subprotocol 返回预设的子协议。
func (m *MockConn) Subprotocol() string { return m.SubprotocolValue }

// Sent 返回已记录的消息副本。
func (m *MockConn) Sent() []ws.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]ws.Message(nil), m.sent...)
}

// SentJSON 返回已记录的 JSON 载荷副本。
func (m *MockConn) SentJSON() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][]byte(nil), m.sentJSON...)
}

// CloseCount 返回 Close 被调用的次数。
func (m *MockConn) CloseCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCount
}

// ClosedWith 返回最后一次 Close 的关闭码与原因。
func (m *MockConn) ClosedWith() (ws.CloseCode, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCode, m.closeReason
}

// MockWs 是 IWs 接口的 mock 实现，可通过 ServeFn 定制行为。
type MockWs struct {
	ServeFn func(ctx context.Context, conn ws.Conn) error
}

// Serve 执行预设行为。
func (m *MockWs) Serve(ctx context.Context, conn ws.Conn) error {
	if m.ServeFn != nil {
		return m.ServeFn(ctx, conn)
	}
	return nil
}
