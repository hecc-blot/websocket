package service

import (
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/hecc-blot/core/contract/ioc"
	iCoreApi "github.com/hecc-blot/framework/contract/api"
	wsConfig "github.com/hecc-blot/websocket/config"
	wsContract "github.com/hecc-blot/websocket/contract"
)

// WsHandle WebSocket 服务处理器，复用框架 IApiHandle 注册路由，不感知具体 HTTP 内核。
type WsHandle struct {
	handle    iCoreApi.IApiHandle
	container ioc.IContainer
	cfg       wsConfig.Config
	upgrader  *websocket.Upgrader
	semaphore chan struct{} // 连接信号量，限制并发连接数
	stats     *wsStats
	conns     *connTable
}

// wsStats 连接统计，分组与根共享同一实例。
type wsStats struct {
	active atomic.Int64
	total  atomic.Int64
	closed atomic.Int64
}

// connTable 活跃连接表，分组与根共享，用于优雅关闭通知。
type connTable struct {
	mu    sync.Mutex
	conns map[*wsConn]struct{}
}

// NewWsSvc 创建 WebSocket 服务处理器，复用框架 IApiHandle 注册路由。
// cfg 传 nil 表示全默认。
func NewWsSvc(handle iCoreApi.IApiHandle, container ioc.IContainer, cfg *wsConfig.Config) wsContract.IWsHandle {
	if container == nil {
		panic("ioc: container must not be nil")
	}
	if handle == nil {
		panic("websocket: api handle must not be nil")
	}

	normalized := wsConfig.Normalize(wsConfig.Config{})
	if cfg != nil {
		normalized = wsConfig.Normalize(*cfg)
	}

	f := &WsHandle{
		handle:    handle,
		container: container,
		cfg:       normalized,
		upgrader:  newUpgrader(normalized),
		stats:     &wsStats{},
		conns:     &connTable{conns: make(map[*wsConn]struct{})},
		semaphore: make(chan struct{}, normalized.MaxConnections),
	}

	// 构造时自注册关闭钩子（理由同 sse）：conns 表由分组共享，一次注册即可；
	// 若在每个 Get 里注册，多路由就会注册多份（框架无法对钩子去重）。
	//
	// 与 SSE 的差别值得记一笔：WS 连接在升级时就被 net/http 摘出了 activeConn
	// （hijackLocked → setState(StateHijacked) → trackConn(false)），
	// 所以 srv.Shutdown **不会**为 WS 连接干等 —— 钩子在这里不是为了防停机超时，
	// 而是要给客户端一个 1001 关闭帧，并让业务的 Serve 正常返回（它自己的 defer 才能跑）。
	// 没有钩子不会卡停机，但会静默降级成客户端裸断连。
	handle.OnShutdown(f.Shutdown)

	return f
}

// Middleware 注册 WebSocket 分组中间件，由框架 IApiHandle 完成依赖注入与挂载。
func (f *WsHandle) Middleware(middlewares ...iCoreApi.IMiddleware) wsContract.IWsHandle {
	f.handle.Middleware(middlewares...)
	return f
}

// Group 创建 WebSocket 路由分组，分组内的中间件仅作用于该分组。
func (f *WsHandle) Group(relativePath string, middlewares ...iCoreApi.IMiddleware) wsContract.IWsHandle {
	group := &WsHandle{
		handle:    f.handle.Group(relativePath),
		container: f.container,
		cfg:       f.cfg,
		upgrader:  f.upgrader,
		semaphore: f.semaphore,
		stats:     f.stats,
		conns:     f.conns,
	}
	group.Middleware(middlewares...)
	return group
}

// Stats 返回 WebSocket 连接统计指标。
func (f *WsHandle) Stats() wsContract.Stats {
	return wsContract.Stats{
		Active: f.stats.active.Load(),
		Total:  f.stats.total.Load(),
		Closed: f.stats.closed.Load(),
	}
}

// Shutdown 通知所有活跃连接优雅关闭：发 1001 关闭帧、取消 ctx、断开底层连接。
//
// 幂等：连接表快照可能为空，每条连接自身的 Close 也只执行一次。
func (f *WsHandle) Shutdown() {
	f.conns.mu.Lock()
	conns := make([]*wsConn, 0, len(f.conns.conns))
	for c := range f.conns.conns {
		conns = append(conns, c)
	}
	f.conns.mu.Unlock()

	for _, c := range conns {
		c.shutdown()
	}
}

// Get 注册 WebSocket 端点。握手恒为 GET（RFC 6455 §4.1），故只有这一个动词。
func (f *WsHandle) Get(apiPath string, wsInstance wsContract.IWs) {
	f.register(apiPath, wsInstance)
}

// register 注册 WebSocket 路由。每个连接会创建独立的业务实例，避免并发共享连接态。
func (f *WsHandle) register(apiPath string, wsInstance wsContract.IWs) {
	// 注入模板实例，仅用于反射获取具体类型（顺带在启动期暴露缺失依赖）
	f.container.Inject(wsInstance)
	wsType := reflect.TypeOf(wsInstance)
	if wsType == nil || wsType.Kind() != reflect.Pointer {
		panic(`websocket: handler must be registered as a pointer, e.g. Get("ws/echo", &EchoWs{})`)
	}
	wsElem := wsType.Elem()

	handler := func(w http.ResponseWriter, r *http.Request) {
		// 1. 连接数上限：升级前判定，客户端还能收到明确的 HTTP 503
		select {
		case f.semaphore <- struct{}{}:
			defer func() { <-f.semaphore }()
		default:
			http.Error(w, "too many connections", http.StatusServiceUnavailable)
			return
		}

		// 2. 握手升级。失败时底层库已按原因写好 HTTP 响应（400 / 403 / 405 / 500），
		//    这里既不能再写响应，也不计入统计 —— Total 只统计真正升级成功的连接。
		//
		//    注意：升级之前不能有任何 body 写入 —— gin 的 responseWriter.Hijack
		//    在 size > 0 时会拒绝劫持。往 WS 分组挂「成功路径上写响应」的中间件会踩这个坑。
		raw, err := f.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		// 3. 每连接创建独立业务实例并注入依赖
		newInstance := reflect.New(wsElem).Interface()
		f.container.Inject(newInstance)
		ws := newInstance.(wsContract.IWs)

		// 4. 连接对象：内部建连接级 ctx，并启动读协程与心跳协程
		conn := newWsConn(raw, r, f.cfg)
		// 兜底关闭：业务正常返回也要断开（已关闭时是空操作）
		defer func() { _ = conn.Close(wsContract.CloseNormalClosure, "") }()

		// 5. 登记到活跃连接表，供优雅关闭通知
		//
		// 顺序有讲究：**先登记、后统计**。反过来（先统计）会留下一个窗口 ——
		// 外部看到 Active 已经 +1，连接却还没进表，此时停机 Shutdown 快照就会漏掉它，
		// 客户端只能裸断连。先登记后统计能保证「统计可见」蕴含「已登记」。
		f.conns.mu.Lock()
		f.conns.conns[conn] = struct{}{}
		f.conns.mu.Unlock()
		defer func() {
			f.conns.mu.Lock()
			delete(f.conns.conns, conn)
			f.conns.mu.Unlock()
		}()

		f.stats.total.Add(1)
		f.stats.active.Add(1)
		defer func() {
			f.stats.active.Add(-1)
			f.stats.closed.Add(1)
		}()

		// 6. 业务逻辑，返回即结束会话
		if err := ws.Serve(conn.ctx, conn); err != nil {
			// 只回固定的英文短语，不把内部错误文本透给客户端
			//（归因由业务自己在返回前用注入的 ILog 记录）。
			_ = conn.Close(wsContract.CloseInternalServerErr, "internal error")
		}
	}

	f.handle.Handle(http.MethodGet, apiPath, http.HandlerFunc(handler))
}

// newUpgrader 构造握手升级器。几个参数是刻意保留默认的：
//   - HandshakeTimeout 不设：握手是纯内存操作，没有外部依赖可等；
//   - EnableCompression 不开：v0.1 不做压缩（RFC 7692 要额外依赖与逐连接状态）。
func newUpgrader(cfg wsConfig.Config) *websocket.Upgrader {
	checkOrigin := cfg.CheckOrigin
	if checkOrigin == nil {
		checkOrigin = func(r *http.Request) bool { return originAllowed(cfg.AllowOrigins, r) }
	}
	return &websocket.Upgrader{
		ReadBufferSize:  cfg.ReadBufferSize,
		WriteBufferSize: cfg.WriteBufferSize,
		Subprotocols:    cfg.Subprotocols,
		CheckOrigin:     checkOrigin,
	}
}

// originAllowed 默认同源：Origin 缺失（非浏览器客户端，如 CLI / 移动端）或与 Host 一致才放行；
// AllowOrigins 非空时改为白名单精确匹配（大小写不敏感），含 "*" 则放行任意来源。
//
// 注意 Origin 只在浏览器里可信：这个校验防的是跨站请求伪造，挡不住非浏览器客户端，
// 鉴权该走 Conn.Query / Conn.Header。
func originAllowed(allowed []string, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	if len(allowed) == 0 {
		return sameOrigin(origin, r.Host)
	}
	for _, a := range allowed {
		if a == "*" || strings.EqualFold(strings.TrimSpace(a), origin) {
			return true
		}
	}
	return false
}

// sameOrigin 判断 Origin 是否与请求 Host 同源（与底层库默认策略一致，只比 Host 不比 scheme）。
func sameOrigin(origin, host string) bool {
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Host, host)
}
