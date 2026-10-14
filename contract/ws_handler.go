package ws

import iCoreApi "github.com/hecc-blot/framework/contract/api"

// IWsHandle WebSocket 服务句柄，复用框架 IApiHandle 注册路由与中间件。
type IWsHandle interface {
	// Get 注册 WebSocket 端点。
	//
	// 只有 Get 没有 Post：握手恒为 GET（RFC 6455 §4.1），底层库对非 GET 直接回 405。
	// SSE 的 Post 是为 fetch + ReadableStream 准备的，WebSocket 没有对应场景。
	Get(apiPath string, ws IWs)
	Middleware(middlewares ...iCoreApi.IMiddleware) IWsHandle
	Group(relativePath string, middlewares ...iCoreApi.IMiddleware) IWsHandle
	Stats() Stats
	// Shutdown 通知所有活跃连接优雅关闭（发 1001 关闭帧并取消连接）。幂等。
	Shutdown()
}

// Stats WebSocket 连接统计指标。分组与根共享同一份。
type Stats struct {
	Active int64 // 当前活跃连接数
	Total  int64 // 历史连接总数（只统计握手成功的连接）
	Closed int64 // 已关闭连接数
}
