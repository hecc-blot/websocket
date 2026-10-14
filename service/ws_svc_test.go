package service

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mocks "github.com/hecc-blot/core/mocks/ioc"
	serverConfig "github.com/hecc-blot/framework/config/http"
	iCoreApi "github.com/hecc-blot/framework/contract/api"
	envEnum "github.com/hecc-blot/framework/enum/env"
	httpSvc "github.com/hecc-blot/framework/service/http"
	wsConfig "github.com/hecc-blot/websocket/config"
	wsContract "github.com/hecc-blot/websocket/contract"
)

// hookRecorder 只关心 OnShutdown 的假 handle：其余方法由嵌入的接口兜底 ——
// NewWsSvc 的构造流程只会调 OnShutdown，所以嵌入 nil 接口是安全的。
type hookRecorder struct {
	iCoreApi.IApiHandle
	hooks []func()
}

func (h *hookRecorder) OnShutdown(fn func()) { h.hooks = append(h.hooks, fn) }

// TestNewWsSvcRegistersShutdownHook：WebSocket 在**构造时**把自己托进框架停机流程，且只注册一次。
//
// 与 SSE 的理由不同（WS 连接在升级时已被 net/http 摘出 activeConn，Shutdown 不会为它干等）：
// 钩子的价值是停机瞬间给客户端一个 1001 关闭帧，并让业务的 Serve 正常返回 ——
// 没有钩子不会卡停机，但会静默降级成客户端裸断连，属于「忘掉就出事且不报错」的约定。
func TestNewWsSvcRegistersShutdownHook(t *testing.T) {
	rec := &hookRecorder{}

	handle := NewWsSvc(rec, &mocks.MockContainer{}, nil)

	assert.Len(t, rec.hooks, 1, "构造时应恰好注册一个关闭钩子")

	// 钩子就是 Shutdown：连接表为空时调用应安全无副作用（钩子须幂等）
	rec.hooks[0]()
	rec.hooks[0]()
	assert.Equal(t, int64(0), handle.Stats().Active)
}

// TestNewWsSvcEndToEnd 走真实构造路径：NewWsSvc + 真实 ApiHandle + 真实握手。
// 其余用例直接构造 WsHandle，只有这条覆盖构造处的接线（配置归一化 → 升级器 / 信号量）。
//
// 这里**不注入 responseSvc**：一个只跑 WS 的服务不需要 API 的响应渲染，
// 于是也就不必构造它（顺带钉住这件事）。
func TestNewWsSvcEndToEnd(t *testing.T) {
	apiHandle := httpSvc.NewApiSvc(
		&serverConfig.Config{Env: envEnum.TestMode},
		&mocks.MockContainer{},
	)
	handle := NewWsSvc(apiHandle, &mocks.MockContainer{}, &wsConfig.Config{MaxConnections: 2})
	handle.Get("/ws", &echoWs{})

	server := httptest.NewServer(apiHandle.(*httpSvc.ApiHandle).Engine())
	defer server.Close()

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.WriteMessage(websocket.TextMessage, []byte("hi")))
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, data, err := c.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, "hi", string(data))
	assert.Equal(t, int64(1), handle.Stats().Total)

	handle.Shutdown()
}

// TestNewWsSvcPanicsOnMissingDependency：容器与 handle 都是硬依赖，缺了立刻 panic，
// 而不是等到第一个连接进来才 nil 崩。
func TestNewWsSvcPanicsOnMissingDependency(t *testing.T) {
	assert.Panics(t, func() { NewWsSvc(&hookRecorder{}, nil, nil) }, "container 为 nil 应 panic")
	assert.Panics(t, func() { NewWsSvc(nil, &mocks.MockContainer{}, nil) }, "handle 为 nil 应 panic")
}

// blockingWs 阻塞直到连接被取消，用于模拟长连接。
type blockingWs struct{}

func (blockingWs) Serve(ctx context.Context, conn wsContract.Conn) error {
	<-ctx.Done()
	return nil
}

// errorWs 立即返回错误，用于验证 1011 关闭帧。
type errorWs struct{}

func (errorWs) Serve(ctx context.Context, conn wsContract.Conn) error {
	return errors.New("test error")
}

// echoWs 把收到的每条消息原样回发（类型一并回传），是 README 里那条消费循环的范式。
type echoWs struct{}

func (echoWs) Serve(ctx context.Context, conn wsContract.Conn) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-conn.Messages():
			if !ok {
				return nil
			}
			if err := conn.Write(msg.Type, msg.Data); err != nil {
				return err
			}
		}
	}
}

// closeTwiceWs 连续两次主动关闭，用于验证关闭的幂等性。
type closeTwiceWs struct{}

func (closeTwiceWs) Serve(ctx context.Context, conn wsContract.Conn) error {
	_ = conn.Close(wsContract.ClosePolicyViolation, "bye")
	_ = conn.Close(wsContract.ClosePolicyViolation, "bye")
	return nil
}

// panicWs 在 Serve 里 panic，用于钉住「panic 走兜底关闭、不是 1011」这一行为。
type panicWs struct{}

func (panicWs) Serve(ctx context.Context, conn wsContract.Conn) error {
	panic("boom")
}

// watcherWs 等消息通道关闭后把 ctx 状态回传，用于验证「客户端断开」的收尾顺序。
// 每连接一个实例，无法在字段上带状态，故用包级通道。
type watcherWs struct{}

var channelClosedCh = make(chan error, 1)

func (watcherWs) Serve(ctx context.Context, conn wsContract.Conn) error {
	for range conn.Messages() {
	}
	// 框架在读协程退出时先取消 ctx、再关消息通道，两者业务都能观察到
	channelClosedCh <- ctx.Err()
	return nil
}

// handshakeWs 记录握手信息后挂住，供断言 Conn 的握手访问器。同样用包级通道收集。
type handshakeWs struct{}

var handshakeCh = make(chan [4]string, 1)

func (handshakeWs) Serve(ctx context.Context, conn wsContract.Conn) error {
	handshakeCh <- [4]string{conn.Query("token"), conn.Header("X-Test"), conn.Subprotocol(), conn.RemoteAddr()}
	<-ctx.Done()
	return nil
}

// newTestHandle 直接构造 WsHandle，便于压小连接上限与心跳间隔（配置里是 Duration，测试可给毫秒）。
// 底层引擎复用框架 ApiHandle，供 httptest.NewServer 挂载。
//
// 注意这里**不注入 responseSvc**：WS 只挂原生 handler，内核不需要 API 的响应渲染。
func newTestHandle(cfg wsConfig.Config) (*WsHandle, http.Handler) {
	apiHandle := httpSvc.NewApiSvc(
		&serverConfig.Config{Env: envEnum.TestMode},
		&mocks.MockContainer{},
	)
	normalized := wsConfig.Normalize(cfg)
	handle := &WsHandle{
		handle:    apiHandle,
		container: &mocks.MockContainer{},
		cfg:       normalized,
		upgrader:  newUpgrader(normalized),
		stats:     &wsStats{},
		conns:     &connTable{conns: make(map[*wsConn]struct{})},
		semaphore: make(chan struct{}, normalized.MaxConnections),
	}
	return handle, apiHandle.(*httpSvc.ApiHandle).Engine()
}

// wsURL 把 httptest 的 http URL 换成 ws URL。
func wsURL(s *httptest.Server, path string) string {
	return "ws" + strings.TrimPrefix(s.URL, "http") + path
}

// waitActive 等待活跃连接数达到期望值。
func waitActive(handle *WsHandle, want int64) {
	deadline := time.Now().Add(2 * time.Second)
	for handle.Stats().Active < want && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}

// waitInactive 等待活跃连接数降为 0。
func waitInactive(handle *WsHandle) {
	deadline := time.Now().Add(2 * time.Second)
	for handle.Stats().Active > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}

// waitClose 读到连接结束，断言对端发来关闭帧并返回关闭码与原因。
func waitClose(t *testing.T, c *websocket.Conn, timeout time.Duration) (wsContract.CloseCode, string) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(timeout))
	for {
		_, _, err := c.ReadMessage()
		if err == nil {
			continue // 关闭帧之前可能还有数据帧在路上
		}
		var closeErr *websocket.CloseError
		if errors.As(err, &closeErr) {
			return wsContract.CloseCode(closeErr.Code), closeErr.Text
		}
		t.Fatalf("连接未以关闭帧结束：%v", err)
		return 0, ""
	}
}

func TestWsEcho(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{})
	handle.Get("/ws", &echoWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.WriteMessage(websocket.TextMessage, []byte("hello")))
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, data, err := c.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, websocket.TextMessage, mt)
	assert.Equal(t, "hello", string(data))

	// 消息类型由业务原样回传，二进制不被改写
	require.NoError(t, c.WriteMessage(websocket.BinaryMessage, []byte{1, 2, 3}))
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, data, err = c.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, websocket.BinaryMessage, mt)
	assert.Equal(t, []byte{1, 2, 3}, data)
}

func TestWsStats(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{})
	handle.Get("/ws", &blockingWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	defer c.Close()

	waitActive(handle, 1)
	assert.Equal(t, int64(1), handle.Stats().Active)
	assert.Equal(t, int64(1), handle.Stats().Total)

	handle.Shutdown()
	waitInactive(handle)
	assert.Equal(t, int64(0), handle.Stats().Active)
	assert.Equal(t, int64(1), handle.Stats().Closed)
}

func TestWsConnectionLimit(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{MaxConnections: 1})
	handle.Get("/ws", &blockingWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	defer c.Close()
	waitActive(handle, 1)

	// 超过上限：升级之前就被拒，客户端拿到的是明确的 HTTP 状态码
	_, resp, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	assert.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	resp.Body.Close()

	handle.Shutdown()
}

func TestWsShutdown(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{})
	handle.Get("/ws", &blockingWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	defer c.Close()
	waitActive(handle, 1)

	handle.Shutdown()

	code, reason := waitClose(t, c, 2*time.Second)
	assert.Equal(t, wsContract.CloseGoingAway, code, "停机应发 1001 going away")
	assert.Equal(t, "server is shutting down", reason)

	waitInactive(handle)
	assert.Equal(t, int64(0), handle.Stats().Active)
}

func TestWsCloseOnServeError(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{})
	handle.Get("/ws", &errorWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	defer c.Close()

	code, reason := waitClose(t, c, 2*time.Second)
	assert.Equal(t, wsContract.CloseInternalServerErr, code)
	assert.Equal(t, "internal error", reason, "不应把内部错误文本透给客户端")
	waitInactive(handle)
}

func TestWsCloseTwiceIdempotent(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{})
	handle.Get("/ws", &closeTwiceWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	defer c.Close()

	code, reason := waitClose(t, c, 2*time.Second)
	assert.Equal(t, wsContract.ClosePolicyViolation, code)
	assert.Equal(t, "bye", reason, "重复 Close 必须只发一次关闭帧，且保留首次的关闭码")

	waitInactive(handle)
}

func TestWsPanicInServe(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{})
	handle.Get("/ws", &panicWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	defer c.Close()

	// panic 不走 「Serve 返回 error → 1011」那条路：它被框架的 gin.Recovery 兜底，
	// 连接退化成兜底的正常关闭（1000）。客户端分不出「业务是崩了还是正常收工」，
	// 所以想让客户端拿到 1011 / 1008，得 return err 或自己 Close。
	code, _ := waitClose(t, c, 2*time.Second)
	assert.Equal(t, wsContract.CloseNormalClosure, code)

	// 进程没被 panic 带走，统计照常回收、槽位照常释放
	waitInactive(handle)
	handle.Shutdown()
	assert.Equal(t, int64(0), handle.Stats().Active)
}

func TestWsRejectsNonWebSocket(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{})
	handle.Get("/ws", &blockingWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	resp, err := http.Get(server.URL + "/ws")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "非 WS 请求应由握手校验拒掉")
	assert.Equal(t, "13", resp.Header.Get("Sec-Websocket-Version"))
	assert.Equal(t, int64(0), handle.Stats().Total, "握手失败不计入连接统计")
}

func TestWsHeartbeat(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{
		PingInterval: 50 * time.Millisecond,
		PongWait:     300 * time.Millisecond,
	})
	handle.Get("/ws", &blockingWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	defer c.Close()

	// 客户端必须在读循环里处理 ping：默认 ping 处理器会自动回 pong，
	// 这里覆盖它以便同时收信号，回 pong 的动作照做。
	pingCh := make(chan struct{}, 1)
	c.SetPingHandler(func(data string) error {
		select {
		case pingCh <- struct{}{}:
		default:
		}
		return c.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
	})
	go func() {
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}()

	select {
	case <-pingCh:
	case <-time.After(2 * time.Second):
		t.Fatal("未在超时内收到服务端心跳 ping")
	}

	// 跨过 PongWait：pong 顺延了读 deadline，空闲连接不该被自己判死
	time.Sleep(400 * time.Millisecond)
	assert.Equal(t, int64(1), handle.Stats().Active, "心跳正常的空闲连接被读超时判死了")

	handle.Shutdown()
	waitInactive(handle)
}

func TestWsPongTimeout(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{
		PingInterval: 50 * time.Millisecond,
		PongWait:     200 * time.Millisecond,
	})
	handle.Get("/ws", &blockingWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	defer c.Close()

	// 客户端收到 ping 但故意不回 pong：服务端应在 PongWait 后按心跳超时断开。
	// 不另起读协程 —— 让 waitClose 的读来消费 ping 帧，避免同连接并发读。
	c.SetPingHandler(func(string) error { return nil })

	code, reason := waitClose(t, c, 3*time.Second)
	assert.Equal(t, wsContract.CloseGoingAway, code)
	assert.Equal(t, "heartbeat timeout", reason)
	waitInactive(handle)
}

func TestWsReadLimit(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{ReadLimit: 64})
	handle.Get("/ws", &echoWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.WriteMessage(websocket.TextMessage, bytes.Repeat([]byte("a"), 200)))

	code, _ := waitClose(t, c, 2*time.Second)
	assert.Equal(t, wsContract.CloseMessageTooBig, code)
	waitInactive(handle)
}

func TestWsMessagesChannelClosesOnClientDisconnect(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{})
	handle.Get("/ws", &watcherWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	waitActive(handle, 1)

	_ = c.Close() // 客户端主动断开

	select {
	case ctxErr := <-channelClosedCh:
		assert.Error(t, ctxErr, "消息通道关闭时 ctx 应已取消")
	case <-time.After(2 * time.Second):
		t.Fatal("客户端断开后消息通道未关闭")
	}
	waitInactive(handle)
}

func TestWsHandshakeAccess(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{Subprotocols: []string{"chat", "superchat"}})
	handle.Get("/ws", &handshakeWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	header := http.Header{}
	header.Set("X-Test", "v")
	header.Set("Sec-WebSocket-Protocol", "superchat")

	c, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws?token=abc"), header)
	require.NoError(t, err)
	defer c.Close()

	select {
	case got := <-handshakeCh:
		assert.Equal(t, "abc", got[0], "Query 应能读到握手 URL 参数")
		assert.Equal(t, "v", got[1], "Header 应能读到握手请求头")
		assert.Equal(t, "superchat", got[2], "应按客户端优先级协商出子协议")
		assert.NotEmpty(t, got[3], "RemoteAddr 不该为空")
	case <-time.After(2 * time.Second):
		t.Fatal("未在超时内取到握手信息")
	}

	handle.Shutdown()
}

// TestWsGroupSharesStats：分组与根共享统计 / 信号量 / 连接表，
// 因此根上的一次 Shutdown 能关掉所有分组里的连接。
func TestWsGroupSharesStats(t *testing.T) {
	handle, engine := newTestHandle(wsConfig.Config{})
	group := handle.Group("admin")
	group.Get("/ws", &blockingWs{})
	handle.Get("/ws", &blockingWs{})
	server := httptest.NewServer(engine)
	defer server.Close()

	c1, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/admin/ws"), nil)
	require.NoError(t, err)
	defer c1.Close()
	c2, _, err := websocket.DefaultDialer.Dial(wsURL(server, "/ws"), nil)
	require.NoError(t, err)
	defer c2.Close()

	waitActive(handle, 2)
	assert.Equal(t, int64(2), handle.Stats().Total)

	handle.Shutdown()
	code, _ := waitClose(t, c1, 2*time.Second)
	assert.Equal(t, wsContract.CloseGoingAway, code, "根上的 Shutdown 应通知到分组内的连接")
	waitClose(t, c2, 2*time.Second)
}

// TestWsSurvivesWriteTimeout 是「WebSocket 被服务端 write_timeout 切断」的回归用例。
//
// 机制：net/http 在读完请求头时把连接的写 deadline 一次性设成 now+WriteTimeout（绝对时间点），
// 但 WS 在升级时会 hijack 该连接，而 hijackLocked 里已 SetDeadline(零值) 清掉它；
// 加上框架每次写前都用相对时间重设写 deadline，WS 天然不受 write_timeout 约束
// —— 与 SSE 需要 handler 自己清 deadline 的情形不同，别照搬那套写法。
//
// 用例形状与 SSE 的 TestStreamSurvivesWriteTimeout 对齐：空闲跨过 write_timeout 后仍必须能收发。
func TestWsSurvivesWriteTimeout(t *testing.T) {
	// 心跳与读超时都拉长，保证空闲窗口内没有任何帧往来
	handle, engine := newTestHandle(wsConfig.Config{
		PingInterval: time.Hour,
		PongWait:     2 * time.Hour,
	})
	handle.Get("/ws", &echoWs{})
	defer handle.Shutdown()

	const writeTimeout = 500 * time.Millisecond
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{Handler: engine, WriteTimeout: writeTimeout}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	c, _, err := websocket.DefaultDialer.Dial("ws://"+ln.Addr().String()+"/ws", nil)
	require.NoError(t, err)
	defer c.Close()

	// 空闲跨过 write_timeout
	time.Sleep(writeTimeout + 300*time.Millisecond)

	require.NoError(t, c.WriteMessage(websocket.TextMessage, []byte("still alive")))
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, data, err := c.ReadMessage()
	require.NoError(t, err, "跨过 WriteTimeout 后连接应仍可用")
	assert.Equal(t, "still alive", string(data))
}

// TestOriginAllowed 覆盖默认同源与白名单两条路径。
func TestOriginAllowed(t *testing.T) {
	newReq := func(origin string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/ws", nil)
		r.Host = "api.example.com"
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		return r
	}

	tests := []struct {
		name    string
		allowed []string
		origin  string
		want    bool
	}{
		{"无 Origin（非浏览器客户端）放行", nil, "", true},
		{"同源放行", nil, "https://api.example.com", true},
		{"同源大小写不敏感", nil, "https://API.Example.COM", true},
		{"跨源拒绝", nil, "https://evil.example.com", false},
		{"非法 Origin 拒绝", nil, "://bad", false},
		{"白名单命中", []string{"https://app.example.com"}, "https://app.example.com", true},
		{"白名单未命中", []string{"https://app.example.com"}, "https://evil.example.com", false},
		{"通配放行任意来源", []string{"*"}, "https://evil.example.com", true},
		{"白名单下无 Origin 仍放行", []string{"https://app.example.com"}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, originAllowed(tt.allowed, newReq(tt.origin)))
		})
	}
}

// TestTruncateCloseReason：关闭原因必须落在控制帧载荷内，且不能劈开多字节字符。
func TestTruncateCloseReason(t *testing.T) {
	assert.Equal(t, "bye", truncateCloseReason("bye"), "短原因应原样返回")

	assert.Len(t, truncateCloseReason(strings.Repeat("a", 200)), maxCloseReason)

	cn := strings.Repeat("服务停机", 60) // 三字节一个汉字，远超上限
	got := truncateCloseReason(cn)
	assert.LessOrEqual(t, len(got), maxCloseReason)
	assert.True(t, utf8.ValidString(got), "截断后必须仍是合法 UTF-8")
	assert.True(t, strings.HasPrefix(cn, got), "应只在尾部截断")

	assert.Equal(t, "", truncateCloseReason(""))
}

// TestMapMessageType：非法消息类型要报错而不是静默丢弃。
func TestMapMessageType(t *testing.T) {
	mt, err := mapMessageType(wsContract.TextMessage)
	assert.NoError(t, err)
	assert.Equal(t, websocket.TextMessage, mt)

	_, err = mapMessageType(wsContract.MessageType(99))
	assert.Error(t, err)
}
