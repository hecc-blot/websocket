package ws

import "context"

// IWs WebSocket 业务接口，业务实现 Serve 编写一条连接的收发逻辑。
//
// ctx 为带取消能力的上下文：客户端断开、心跳超时、服务停机时都会被取消，
// 业务应在循环里 select ctx.Done() 退出。
// conn 为框架提供的读写抽象，封装了握手信息、并发安全写入、关闭礼仪与心跳，
// 业务无需直接操作底层库。
type IWs interface {
	Serve(ctx context.Context, conn Conn) error
}

// MessageType 消息类型。取值与 RFC 6455 的帧类型一致，业务无需感知底层库。
type MessageType int

const (
	// TextMessage 文本消息（UTF-8），JSON 走这里。
	TextMessage MessageType = 1
	// BinaryMessage 二进制消息。
	BinaryMessage MessageType = 2
)

// CloseCode 关闭码，取值见 RFC 6455 §7.4.1。
//
// 1005 / 1006 / 1015 是保留码，**不得出现在关闭帧里**：框架会把它们归一成 1000。
type CloseCode int

const (
	CloseNormalClosure           CloseCode = 1000 // 正常关闭
	CloseGoingAway               CloseCode = 1001 // 服务停机，或心跳超时
	CloseProtocolError           CloseCode = 1002 // 协议错误
	CloseUnsupportedData         CloseCode = 1003
	CloseNoStatusReceived        CloseCode = 1005 // 保留码
	CloseAbnormalClosure         CloseCode = 1006 // 保留码
	CloseInvalidFramePayloadData CloseCode = 1007
	ClosePolicyViolation         CloseCode = 1008 // 业务拒绝（如鉴权不通过）
	CloseMessageTooBig           CloseCode = 1009 // 超过读上限
	CloseInternalServerErr       CloseCode = 1011 // 业务 Serve 返回 error
	CloseServiceRestart          CloseCode = 1012
	CloseTryAgainLater           CloseCode = 1013
	CloseTLSHandshake            CloseCode = 1015 // 保留码
)

// Message 一条完整消息。分片已由框架聚合，控制帧不会出现在这里。
type Message struct {
	Type MessageType
	Data []byte
}

// Conn 单条连接的读写抽象，由框架实现并注入给业务。
//
// 读侧：框架在内部读协程里持续读帧，把业务消息投递到 Messages()；客户端断开时该通道被关闭。
// 写侧：Write / WriteJSON / Close 并发安全 —— 底层库把并发写视为致命错误，
// 框架内部已串行化。
type Conn interface {
	// Messages 返回消息接收通道，客户端断开时被关闭（业务不得关闭它）。
	// 既可 range，也可与其他 channel 一起 select —— 后者是「一边收、一边定时推」的常见写法：
	//
	//	for {
	//	    select {
	//	    case <-ctx.Done():
	//	        return nil
	//	    case msg, ok := <-conn.Messages():
	//	        if !ok {
	//	            return nil // 客户端断开
	//	        }
	//	        _ = conn.Write(ws.TextMessage, msg.Data)
	//	    case <-ticker.C:
	//	        _ = conn.WriteJSON(event)
	//	    }
	//	}
	//
	// 业务消费不及时会让内部读协程阻塞在投递上（背压，不丢消息）。
	Messages() <-chan Message

	// Write 发送一条消息。
	Write(msgType MessageType, data []byte) error

	// WriteJSON 把 v 序列化为 JSON 并以文本消息发送（不带换行）。
	WriteJSON(v any) error

	// Close 主动关闭连接：发送关闭帧、取消 ctx、断开底层连接。
	// 幂等 —— 重复调用是空操作（关闭帧只发一次），错误只回给首次调用者。
	Close(code CloseCode, reason string) error

	// RemoteAddr 客户端地址（"ip:port"），日志与审计用。
	RemoteAddr() string

	// Query 取握手请求的查询参数。浏览器不能给 WebSocket 设自定义头，
	// token 通常放在这里（如 ws://host/ws?token=xxx）。
	Query(key string) string

	// Header 取握手请求头（Cookie / Authorization）。
	Header(key string) string

	// Subprotocol 返回协商出的子协议，未协商时为空串。
	Subprotocol() string
}
