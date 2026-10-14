# hecc-blot-websocket

WebSocket 模块：与 API 共享端口，框架封装握手校验、Origin 校验、心跳、读上限、连接上限与优雅关闭。

## 安装

```bash
go get github.com/hecc-blot/websocket
```

## 快速开始

```go
import (
	"context"

	ioc "github.com/hecc-blot/core/service/ioc"
	httpSvc "github.com/hecc-blot/framework/service/http"
	wsContract "github.com/hecc-blot/websocket/contract"
	ws "github.com/hecc-blot/websocket/service"
)

type EchoWs struct {
	LogSvc logContract.ILog `inject:""` // 注入字段在前
}

func (e *EchoWs) Serve(ctx context.Context, conn wsContract.Conn) error {
	for {
		select {
		case <-ctx.Done(): // 客户端断开、心跳超时或服务停机
			return nil
		case msg, ok := <-conn.Messages():
			if !ok {
				return nil // 客户端断开，消息通道被关闭
			}
			if err := conn.Write(msg.Type, msg.Data); err != nil {
				return err
			}
		}
	}
}

func main() {
	// 1. HTTP 内核（WS 复用它，不另起端口）
	container := ioc.New()
	container.Set(new(logContract.ILog), logSvc)
	// 只要这个服务不注册 API 路由（Get/Post），就不必注入响应渲染 ——
	// WS 只挂原生 handler。同一个内核上还要跑 API 时才加：
	//   apiHandle.SetResponseSvc(httpSvc.NewResponseSvc())
	apiHandle := httpSvc.NewApiSvc(&config.Server, container)

	// 2. WS 句柄，cfg 传 nil 即全默认；跨域前端在这里配 AllowOrigins
	wsHandle := ws.NewWsSvc(apiHandle, container, nil)

	// 3. 注册端点
	wsHandle.Get("ws/echo", &EchoWs{})

	// 4. 启动（阻塞）。WS 已在 NewWsSvc 里把自己注册进停机流程，无需手工传钩子
	apiHandle.Listen()
}
```

完整的应用组装顺序见[组织主页](https://github.com/hecc-blot)。

## 端口

WS **不定义自己的端口**：它复用框架 HTTP 内核的监听，也就是 `framework/config/http` 里那个 `port`（由 `apiHandle.Listen()` 绑成 `":" + port`）。原因是 WS 的握手本身就是一次普通 HTTP GET + `Upgrade`（RFC 6455 §4.1），`ws://` / `wss://` 的默认端口按规范就是 80 / 443 —— **这里的协议边界是 `Upgrade` 头，不是端口**。所以与 API、SSE 同端口是标准形态，不是将就：客户端连 `ws://host:<port>/<你注册的 path>` 即可。

想把 WS **隔离到独立端口**（连接风暴的爆炸半径、LB 与健康检查语义、与 API 独立扩缩容、给 WS 单独配读/闲超时与 body limit），做法是再建一个内核 = 第二个端口，模块侧无需任何改动：

```go
// 端口就在传给第二个 NewApiSvc 的那份 serverConfig.Config 里
wsAPI := httpSvc.NewApiSvc(&config.WsServer, container) // config.WsServer.Port = "8081"

wsHandle := ws.NewWsSvc(wsAPI, container, nil) // ★ 换成第二个内核
wsHandle.Get("ws/echo", &EchoWs{})

go wsAPI.Listen()  // Listen() 阻塞且各自接管 SIGINT/SIGTERM，所以要 go
apiHandle.Listen() // :8080 —— API / SSE
```

**纯 WS 的内核连 `SetResponseSvc` 都不用调** —— 响应渲染（`{code, message, data}`）只服务 `Get`/`Post` 注册的 API 路由，而 WS 走 `Handle()` 挂原生 handler，升级之后更没有 HTTP 信封可包：WS 的「响应格式」就是帧与关闭码（`contract.CloseCode`）。所以一个只跑 WS 的进程不需要任何 API 侧的构造。

两个内核各自持有 engine、中间件链、超时、连接上限与停机钩子；一次 Ctrl-C 会同时触发两边（Go 给每个已注册的 channel 各送一份信号副本），各自跑完钩子再 `Shutdown`，无需额外协调。

拆开的代价要一并算：防火墙与安全组要开第二个口，前端 CSP 的 `connect-src` 要写两份，LB 要多一个 listener 与健康检查，而且不少企业代理只放行 80/443 上的 WS（`wss://host:8443` 会被拦）。

> 别指望 ws 模块自己持有端口：`NewWsSvc` 收的是 `IApiHandle` 而不是地址，正是为了拿到 `inject:""` 依赖注入、`Group` 中间件、`gin.Recovery` 与统一的停机顺序；模块自带 listener 就得复制一整套内核，那才是倒退。端口属于部署形态，由业务在 `main` 里定。

> 第二个内核的 `responseSvc` 参数不存在了（2026-10-14 起）：响应渲染改为 `SetResponseSvc` 按需注入，因为它只服务 `Get`/`Post`；纯 WS / SSE 内核因此不必构造它。若哪天有人往这个内核注册了 `Get`/`Post` 却忘了注入，会在**注册期** panic 而不是请求期 nil 崩。

## 接口定义

```go
import (
	"context"

	iCoreApi "github.com/hecc-blot/framework/contract/api"
	wsContract "github.com/hecc-blot/websocket/contract"
)

// 业务实现 Serve 编写一条连接的收发逻辑
type IWs interface {
	// ctx 带取消能力：客户端断开 / 心跳超时 / 服务停机时被取消
	Serve(ctx context.Context, conn Conn) error
}

// 框架注入给业务的连接抽象
type Conn interface {
	Messages() <-chan Message                   // 客户端断开时被关闭
	Write(msgType MessageType, data []byte) error
	WriteJSON(v any) error                      // 序列化为 JSON 并作为文本消息发送
	Close(code CloseCode, reason string) error  // 幂等
	RemoteAddr() string
	Query(key string) string                    // 握手 URL 参数（浏览器不能自定义头，token 常放这里）
	Header(key string) string                   // 握手请求头
	Subprotocol() string
}

type Message struct {
	Type MessageType // TextMessage(1) / BinaryMessage(2)
	Data []byte
}

type IWsHandle interface {
	Get(apiPath string, ws IWs)
	Middleware(middlewares ...iCoreApi.IMiddleware) IWsHandle
	Group(relativePath string, middlewares ...iCoreApi.IMiddleware) IWsHandle
	Stats() Stats
	Shutdown()
}

type Stats struct {
	Active int64 // 当前活跃连接数
	Total  int64 // 历史连接总数（只统计握手成功的连接）
	Closed int64 // 已关闭连接数
}
```

## 使用

### 定义 WS 端点

```go
type ChatWs struct {
	LogSvc logContract.ILog `inject:""` // 注入字段在前
}

func (c *ChatWs) Serve(ctx context.Context, conn wsContract.Conn) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-conn.Messages(): // 收
			if !ok {
				return nil
			}
			if err := conn.Write(msg.Type, msg.Data); err != nil {
				return err
			}
		case <-ticker.C: // 推
			if err := conn.WriteJSON(map[string]any{"type": "tick", "at": time.Now()}); err != nil {
				return err
			}
		}
	}
}
```

收与推放在同一个 `select` 里，一个协程就能写完一条会话 —— 这是 `Messages()` 返回通道而不是阻塞式 `Read()` 的原因。

### 注册路由

```go
wsHandle.Get("ws/chat", &ChatWs{})
```

> **只有 `Get`，没有 `Post`**：握手恒为 GET（RFC 6455 §4.1），底层库对非 GET 直接回 405。SSE 的 `Post` 是为 `fetch` + `ReadableStream` 准备的，WebSocket 没有对应场景。
>
> 需要挂原生 handler 请用 `apiHandle.Handle`，`wsHandle` 不提供。

### 分组与中间件

```go
// 分组：中间件只作用于本组
wsGroup := wsHandle.Group("admin", &demo.WsAuthMiddleware{})
wsGroup.Get("ws/order", &OrderWs{})
```

> ⚠️ **`wsHandle.Middleware(...)` 是全局中间件**。它内部直接挂到 HTTP 内核的**根 group** 上，会作用于**全部 API 路由**，而不只是 WS 路由。只想作用于 WS 就用 `Group()`。
>
> ⚠️ **升级之前不能有任何 body 写入**：底层在写响应体后拒绝劫持连接。所以别给 WS 分组挂「成功路径上写响应」的中间件（写 401/403 的鉴权中间件没问题 —— 它直接中断，不会走到升级）。

### 鉴权

浏览器不能给 WebSocket 设自定义头，两条路：

```go
// 1) 中间件里校验，失败就按普通 HTTP 拒绝（客户端拿到 401，不会建立连接）
func (m *WsAuthMiddleware) Middleware() iCoreApi.MiddlewareFunc {
	return func(ctx iCoreApi.IContext) {
		if ctx.Query("token") == "" {
			ctx.AbortWithStatus(http.StatusUnauthorized)
			return
		}
	}
}

// 2) 在 Serve 里用握手信息校验，失败按 WS 语义关闭（客户端拿到 1008）
func (c *ChatWs) Serve(ctx context.Context, conn wsContract.Conn) error {
	if !c.auth(conn.Query("token"), conn.Header("Cookie")) {
		_ = conn.Close(wsContract.ClosePolicyViolation, "unauthorized")
		return nil
	}
	...
}
```

### 服务端主动推送与广播

框架**不提供广播/房间**：连接是每请求一个业务实例，跨连接投递属于业务编排（谁在哪个房间、慢消费者怎么丢弃、多副本怎么同步），塞进框架只会长成一个半成品 hub。业务侧自建连接注册表即可：

```go
// 注入一个单例，Serve 开始时登记、结束时注销
type Hub struct {
	mu    sync.RWMutex
	conns map[string]wsContract.Conn
}

func (h *Hub) Join(id string, conn wsContract.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.conns[id] = conn
}

func (h *Hub) Broadcast(data []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, conn := range h.conns {
		_ = conn.Write(wsContract.TextMessage, data) // Write 本身并发安全
	}
}
```

### 请求处理流程

```
GET /ws/chat（带 Upgrade 头）
  ├─ 连接数限流（超过上限 → HTTP 503，还没升级）
  ├─ 握手升级（非 WS 请求 → 400；跨源 → 403）
  ├─ 登记到连接表 + Stats 计数
  ├─ reflect.New 创建独立实例并 Inject 依赖
  ├─ 启动读协程（读帧、顺延读 deadline、投递到 Messages()）
  ├─ 启动心跳协程（每 30s 一个 ping）
  └─ chat.Serve(ctx, conn)
```

### 框架自动处理的能力

| 能力 | 说明 |
|------|------|
| 握手校验 | 缺 `Upgrade` / `Sec-WebSocket-Version` 等头时返回 **400**，非 GET 返回 405 |
| Origin 校验 | 默认同源；`AllowOrigins` 配白名单或 `*`；拒绝返回 **403** |
| 心跳 | 每 **30s** 一个 ping，**60s** 收不到 pong（或任何入站流量）即按心跳超时断开（1001） |
| 读上限 | 单条消息上限 **512KB**（`ReadLimit`），超限关闭码 **1009** |
| 连接限流 | 并发连接上限 **1000**（`MaxConnections`），超出握手返回 **503** |
| 优雅关闭 | `Shutdown()` 向所有活跃连接发 **1001** 关闭帧并取消连接 |
| 错误处理 | `Serve` 返回 error 时发 **1011** 关闭帧（原因固定为 `internal error`，不透内部文本） |
| 并发安全 | 框架串行化所有写（底层库把并发写当致命错误），业务可在多个协程里并发 `Write` |

## 关闭码

| 码 | 什么时候出现 |
|------|------|
| 1000 正常关闭 | 业务 `Serve` 正常返回 |
| 1001 going away | 服务停机（`server is shutting down`），或心跳超时（`heartbeat timeout`） |
| 1008 policy violation | 业务主动拒绝，如鉴权不通过 —— 由业务自己 `Close` |
| 1009 message too big | 超过 `ReadLimit`，由底层库发出 |
| 1011 internal error | 业务 `Serve` 返回了 error |

关闭礼仪由框架收口：**一个连接只会发一个关闭帧**（`Close` 幂等），对端先发关闭帧时框架原样回显、不再补发，也**不等待**对端关闭握手就断开 TCP。

> 想要 1008 这种「可区分的拒绝」，请在 `Serve` 里自己 `conn.Close(1008, "unauthorized")` 后 `return nil`；直接 `return err` 一律是 1011。

## 优雅关闭

```go
apiHandle.Listen() // 无需传参：NewWsSvc 已在构造时注册好关闭钩子
```

`NewWsSvc` 会调用 `apiHandle.OnShutdown(wsHandle.Shutdown)`，把自己托进框架的停机流程：`Listen` 收到退出信号后**先执行关闭钩子**（给所有连接发 1001 关闭帧、取消 ctx、断开连接），再以 5s 超时关闭 HTTP 服务。

> **与 SSE 不同，原因也值得记一笔**：WS 在升级时劫持了连接，`net/http` 随即把它移出活跃连接表，因此 `srv.Shutdown` **不会**为 WS 连接干等 —— 钩子在这里不是为了防停机超时，而是（1）给客户端一个 1001 关帧而不是裸 TCP 断开，（2）让业务的 `Serve` 正常返回，使它自己的 `defer` 能跑完。没有钩子不会卡停机，但会静默降级。
>
> 之所以仍然自注册，是因为旧写法 `Listen(wsHandle.Shutdown)` 一旦忘了传就**静默**降级、不报任何错。

## 语义与约定

- **每个连接一个实例**：框架对每个连接 `reflect.New` 出独立的 `IWs` 实例并重新注入依赖，因此**不要在注入字段上保存连接态数据**。
- **`NewWsSvc` 的 `container` 为 nil 会 panic**；`handle` 也不能为 nil；`Get` 只接受指针（`&EchoWs{}`），否则 panic。
- **`Write` / `WriteJSON` / `Close` 并发安全**，但同一连接上的写是**串行**的（框架内部加锁）—— 不要在业务里再叠一层大缓冲去掩盖慢写，那只会把延迟堆到写完为止。
- **有背压、无丢弃**：业务消费 `Messages()` 不及时会让内部读协程阻塞在投递上，此时连接不再读入（TCP 缓冲区积压），消息不会丢。丢弃/降速策略见 `feature.md` 的待办。
- **`Messages()` 的通道由框架关闭，业务不得关闭它**；客户端断开、心跳超时、服务停机都会关闭它并取消 `ctx`。
- **`Serve` 里 panic 不会发 1011**：它被框架的 `gin.Recovery` 兜底（进程不死、连接与槽位照常回收），客户端拿到的是兜底的**正常关闭（1000）**—— 与「业务正常收工」在客户端看来一模一样。想让客户端拿到 1011 / 1008，请 `return err` 或自己 `Close`，不要靠 panic（用例 `TestWsPanicInServe` 钉住这一点）。
- **不受 `write_timeout` 约束**：`net/http` 在读完请求头时设下的写 deadline 是绝对时间点，但 WS 升级会劫持连接、`net/http` 随即把连接 deadline 清零；框架每次写前还会用相对时间重设写 deadline。所以 WS **不需要**像 SSE 那样在 handler 里 `SetWriteDeadline(time.Time{})`（回归用例 `TestWsSurvivesWriteTimeout` 钉住这一点）。
- **不打印日志**：框架侧失败只体现在关闭码与统计上，请求归因请用注入的 `ILog` 在业务里记。
- **v0.1 不做**：压缩（RFC 7692）、按路由的子协议（只有配置级 `Subprotocols`）、广播/房间、背压丢弃策略。
- **无释放语义**：`NewWsSvc` 不返回 cleanup，连接随进程退出回收。

## 配置

```go
wsHandle := ws.NewWsSvc(apiHandle, container, &wsConfig.Config{
	MaxConnections: 2000,
	ReadLimit:      1 << 20,     // 1MB
	PingInterval:   20 * time.Second,
	PongWait:       45 * time.Second,
	AllowOrigins:   []string{"https://app.example.com"},
})
```

零值即全默认（也可以直接传 `nil`）。字段全部带 `mapstructure` tag，可从 yaml 绑定；时长字段认 `"30s"` 这类字符串。`PongWait` 配得比 `PingInterval` 还小时会被自动抬到两倍心跳，避免空闲的健康连接被自己判死。

## 测试与 mock

```go
import "github.com/hecc-blot/websocket/mocks"

mockConn := &mocks.MockConn{}  // 记录 Sent / CloseCode，带锁，可安全并发使用
mockWs := &mocks.MockWs{}      // ServeFn 定制行为
```

## 相关模块

| 模块 | 说明 |
|------|------|
| [core](https://github.com/hecc-blot/core) | `ioc.IContainer` 依赖注入、`log.ILog` 日志契约、`util.Go` 协程 panic 归因 |
| [framework](https://github.com/hecc-blot/framework) | HTTP 内核与 `IMiddleware`、`OnShutdown`；WS 复用同一端口 |
| [sse](https://github.com/hecc-blot/sse) | 同一端口上的另一条长连接（服务端单向推送） |
| [trace](https://github.com/hecc-blot/trace) | `NewHttpMiddleware` 可挂在 WS 路由上；暂无 WS 专用中间件 |
| [metrics](https://github.com/hecc-blot/metrics) | 连接数等指标的来源（`Stats()`） |
