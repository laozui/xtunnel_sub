package tunnel

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/xtaci/smux"
)

// ======================== 全局配置 ========================

type GlobalConfig struct {
	DialTimeout        time.Duration
	WSHandshakeTimeout time.Duration
	ReconnectDelay     time.Duration
	RTTProbeTimeout    time.Duration
	ReadBuf            int
}

var cfg = GlobalConfig{
	DialTimeout:        3 * time.Second,
	WSHandshakeTimeout: 5 * time.Second,
	ReconnectDelay:     1 * time.Second,
	RTTProbeTimeout:    3 * time.Second, // 3秒探测周期
	ReadBuf:            64 * 1024,
}

var bufPool = sync.Pool{New: func() any { b := make([]byte, 64*1024); return &b }}

// getSmuxConfig 返回调优后的高性能 smux 配置。
// 针对移动蜂窝网络（如中国移动激进的 25s NAT 老化）与 YouTube 长音频缓冲进行深度加固：
// 1. MaxStreamBuffer 扩充至 512KB，打破 100ms 延迟下 5Mbps 吞吐上限；
// 2. MaxReceiveBuffer 扩充至 16MB，多流并发不再互相锁死；
// 3. KeepAliveInterval 收紧至 8s，KeepAliveTimeout 紧缩至 20s：
//    在移动基站防火墙 25s 超时掐断前，每 8s 发送一次活跃心跳强力冲刷 NAT 表项；
//    若遇基站无通知静默丢包，最多 20s 彻底宣告死亡就地重建，彻底终结长达 45s 的僵死期。
func getSmuxConfig() *smux.Config {
	sc := smux.DefaultConfig()
	sc.Version = 1 // 保持 version 1 以严格兼容现有节点
	sc.MaxFrameSize = 32768
	sc.MaxReceiveBuffer = 16 * 1024 * 1024
	sc.MaxStreamBuffer = 512 * 1024
	sc.KeepAliveInterval = 8 * time.Second
	sc.KeepAliveTimeout = 20 * time.Second
	return sc
}

// ======================== 全局参数 ========================

var (
	serviceMu     sync.Mutex
	proxyListener net.Listener

	token         string
	connectionNum int
	insecure      bool

	dnsServer string
	echDomain string
	fallback  bool

	echListMu sync.RWMutex
	echList   []byte
	refreshMu sync.Mutex

	echPool *ECHPool

	clientID      string
	udpBlockPorts map[int]struct{}

	ipStrategy byte
)

// ======================== 运行诊断（供 Android 层显示/上报） ========================

var (
	diagMu        sync.Mutex
	lastTunnelErr string
	diagLog       []string
	diagLogMax    = 300
)

type diagWriter struct{}

func (diagWriter) Write(p []byte) (int, error) {
	diagMu.Lock()
	line := strings.TrimRight(string(p), "\r\n")
	if line != "" {
		diagLog = append(diagLog, line)
		if len(diagLog) > diagLogMax {
			diagLog = diagLog[len(diagLog)-diagLogMax:]
		}
	}
	diagMu.Unlock()
	return os.Stderr.Write(p)
}

func init() {
	log.SetOutput(diagWriter{})
	log.SetFlags(log.Ltime)
	go diagFileFlusher()
}

// diagStatusPath 是 App 私有目录下的诊断状态文件，由 Go 侧定时刷新。
// Java 侧直接读取该文件展示给用户，无需依赖 gomobile 对 GetTunnelStatus 生成的
// Java 方法(实测 gomobile 未为部分导出函数生成 Java 绑定，会导致编译失败)。
const diagStatusPath = "/data/data/com.x.tunnel/files/xtunnel_status.log"

// diagHealthPath 是给看门狗读的极简健康快照（一行纯数字）。
// 不走 gomobile 绑定，原因同上 —— 文件通道最稳，且 Java 侧读文件零依赖。
const diagHealthPath = "/data/data/com.x.tunnel/files/xtunnel_health.txt"

// diagFileFlusher 每 5 秒把隧道实时状态写入诊断文件，供 App 界面/长按诊断读取。
//
// 顺带刷新健康快照：这是「自愈看门狗」唯一的判断依据 ——
// 文件里的时间戳本身也是证据，超过 90 秒没刷新就说明这个 Go 协程已经停了。
func diagFileFlusher() {
	for {
		time.Sleep(5 * time.Second)
		_ = os.WriteFile(diagStatusPath, []byte(GetTunnelStatus()), 0600)
		_ = os.WriteFile(diagHealthPath,
			[]byte(fmt.Sprintf("%d|%s", time.Now().UnixMilli(), GetHealthSnapshot())), 0600)
	}
}

func setTunnelErr(msg string) {
	diagMu.Lock()
	lastTunnelErr = msg
	diagMu.Unlock()
}

// GetTunnelStatus 返回隧道实时状态 + 最近日志，便于在 App 界面直接定位故障。
func GetTunnelStatus() string {
	var sb strings.Builder

	serviceMu.Lock()
	p := echPool
	serviceMu.Unlock()

	ready, total := 0, 0
	rttInfo := ""
	if p != nil {
		p.wsConnsMu.RLock()
		total = len(p.smuxConns)
		parts := make([]string, 0, len(p.smuxConns))
		for i, sess := range p.smuxConns {
			if sess != nil && !sess.IsClosed() {
				ready++
				r := atomic.LoadInt64(&p.channelRTT[i])
				if r > 0 {
					parts = append(parts, fmt.Sprintf("%d:%dms", i+1, r/1e6))
				}
			}
		}
		p.wsConnsMu.RUnlock()
		rttInfo = strings.Join(parts, " ")
	}

	diagMu.Lock()
	le := lastTunnelErr
	logs := append([]string(nil), diagLog...)
	diagMu.Unlock()

	sb.WriteString(fmt.Sprintf("通道就绪: %d/%d", ready, total))
	if rttInfo != "" {
		sb.WriteString("   [" + rttInfo + "]")
	}
	sb.WriteString("\n最后错误: ")
	if le == "" {
		sb.WriteString("(无)")
	} else {
		sb.WriteString(le)
	}
	sb.WriteString("\n--- 最近日志 ---\n")
	start := 0
	if len(logs) > 60 {
		start = len(logs) - 60
	}
	for _, l := range logs[start:] {
		sb.WriteString(l)
		sb.WriteString("\n")
	}
	return sb.String()
}

// GetRecentLogs 仅返回最近日志文本。
func GetRecentLogs() string {
	diagMu.Lock()
	defer diagMu.Unlock()
	start := 0
	if len(diagLog) > 200 {
		start = len(diagLog) - 200
	}
	return strings.Join(diagLog[start:], "\n")
}

// GetHealthSnapshot 返回极简健康快照，供 Android 层看门狗判定「是否需要深度重连」。
// 刻意只回一行纯数字，解析成本最低：
//
//	<通道就绪数>/<通道总数>|<池子是否存在(1/0)>|<SOCKS5监听是否存在(1/0)>
//
// 为什么需要它：VPN 卡死时界面仍显示"已连接"，App 自身无从判断。
// 有了这个快照，看门狗就能区分"真的通"和"只是看起来连着"。
// 注意「池子是否存在=0」是比心跳时间戳更硬的信号 —— 说明 echPool 已被置 nil
// 且没有重建，内核实际已经不在服务了。
func GetHealthSnapshot() string {
	serviceMu.Lock()
	p := echPool
	listen := proxyListener
	serviceMu.Unlock()

	ready, total := 0, 0
	if p != nil {
		p.wsConnsMu.RLock()
		total = len(p.smuxConns)
		for _, sess := range p.smuxConns {
			if sess != nil && !sess.IsClosed() {
				ready++
			}
		}
		p.wsConnsMu.RUnlock()
	}
	hasPool := 0
	if p != nil {
		hasPool = 1
	}
	hasListen := 0
	if listen != nil {
		hasListen = 1
	}
	return fmt.Sprintf("%d/%d|%d|%d", ready, total, hasPool, hasListen)
}

const (
	IPStrategyDefault  byte = 0
	IPStrategyIPv4Only byte = 1
	IPStrategyIPv6Only byte = 2
	IPStrategyPv4Pv6   byte = 3
	IPStrategyPv6Pv4   byte = 4
)

func StartSocksProxy(host, wsServer string, n int, udpBlockPortsStr string, dns, ech, ip string, tkn string, fb bool, ipsPref string, insecureFlag bool) error {
	if wsServer == "" {
		setTunnelErr("缺少 wss 服务地址（节点未配置服务地址）")
		return fmt.Errorf("缺少 wss 服务地址")
	}
	if host == "" {
		setTunnelErr("缺少本地监听地址")
		return fmt.Errorf("缺少本地监听地址")
	}
	u, err := url.Parse(wsServer)
	if err != nil {
		setTunnelErr(fmt.Sprintf("无效的服务地址: %v", err))
		return fmt.Errorf("无效的服务地址: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "wss" && scheme != "ws" {
		setTunnelErr(fmt.Sprintf("仅支持 ws:// 或 wss:// 协议（当前: %s，地址: %s）", scheme, wsServer))
		return fmt.Errorf("仅支持 ws:// 或 wss:// 协议")
	}
	if u.Hostname() == "" {
		setTunnelErr(fmt.Sprintf("服务地址缺少主机名: %q", wsServer))
		return fmt.Errorf("服务地址缺少主机名: %q", wsServer)
	}

	serviceMu.Lock()
	defer serviceMu.Unlock()
	if proxyListener != nil || echPool != nil {
		return fmt.Errorf("服务已启动")
	}

	connectionNum = n
	if connectionNum <= 0 {
		connectionNum = 1
	}
	dnsServer = dns
	if dnsServer == "" {
		dnsServer = "https://doh.pub/dns-query"
	}
	echDomain = ech
	if echDomain == "" {
		echDomain = "cloudflare-ech.com"
	}
	token = tkn
	fallback = fb
	insecure = insecureFlag
	if insecure && !fallback {
		fallback = true
	}
	ipStrategy = parseIPStrategy(ipsPref)

	var targetIPs []string
	if ip != "" {
		parts := strings.Split(ip, ",")
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				targetIPs = append(targetIPs, trimmed)
			}
		}
	}

	udpBlockPorts = make(map[int]struct{})
	if strings.TrimSpace(udpBlockPortsStr) != "" {
		parts := strings.Split(udpBlockPortsStr, ",")
		for _, p := range parts {
			pp := strings.TrimSpace(p)
			if pp == "" {
				continue
			}
			var port int
			_, _ = fmt.Sscanf(pp, "%d", &port)
			if port > 0 && port < 65536 {
				udpBlockPorts[port] = struct{}{}
			}
		}
	}

	if scheme == "wss" && !fallback {
		// prepareECH 内部为无限重试，这里加超时保护，避免 SOCKS5 监听被永久阻塞。
		echDone := make(chan error, 1)
		go func() { echDone <- prepareECH() }()
		select {
		case err := <-echDone:
			if err != nil {
				setTunnelErr(fmt.Sprintf("获取 ECH 公钥失败: %v", err))
				return fmt.Errorf("获取 ECH 公钥失败: %w", err)
			}
		case <-time.After(15 * time.Second):
			log.Printf("[客户端] ECH 公钥获取超时(15s)，先行启动（后台继续重试）")
			setTunnelErr("ECH 公钥获取超时(15s)，该节点可能不支持 ECH，建议开启『禁用ECH』")
		}
	}

	clientID = uuid.NewString()
	echPool = NewECHPool(wsServer, connectionNum, targetIPs, clientID)
	echPool.Start()
	// 启动「入口吞吐优选」：实测各入口真实速率，让选路在延迟够快的前提下优先挑跑得动的入口
	echPool.StartThroughputProbe()

	l, err := net.Listen("tcp", host)
	if err != nil {
		echPool.Close()
		echPool = nil
		setTunnelErr(fmt.Sprintf("SOCKS5 监听失败: %v", err))
		return fmt.Errorf("SOCKS5 监听失败: %w", err)
	}
	proxyListener = l

	go func() {
		cfgp := &ProxyConfig{Host: host}
		for {
			c, e := l.Accept()
			if e != nil {
				if errors.Is(e, net.ErrClosed) {
					return
				}
				continue
			}
			go handleSOCKS5(c, cfgp)
		}
	}()
	return nil
}

func WaitSocksProxyReady(timeoutMs int) bool {
	serviceMu.Lock()
	p := echPool
	serviceMu.Unlock()
	if p == nil {
		return false
	}
	if timeoutMs <= 0 {
		timeoutMs = 3000
	}
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	for {
		select {
		case <-p.ctx.Done():
			return false
		default:
		}
		p.wsConnsMu.RLock()
		ready := false
		for _, sess := range p.smuxConns {
			if sess != nil && !sess.IsClosed() {
				ready = true
				break
			}
		}
		p.wsConnsMu.RUnlock()
		if ready {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func StopSocksProxy() {
	serviceMu.Lock()
	l := proxyListener
	p := echPool
	proxyListener = nil
	echPool = nil
	if l != nil {
		_ = l.Close()
	}
	if p != nil {
		p.Close()
	}
	serviceMu.Unlock()
}

func parseIPStrategy(s string) byte {
	s = strings.ReplaceAll(strings.TrimSpace(s), " ", "")
	switch s {
	case "4":
		return IPStrategyIPv4Only
	case "6":
		return IPStrategyIPv6Only
	case "4,6":
		return IPStrategyPv4Pv6
	case "6,4":
		return IPStrategyPv6Pv4
	default:
		return IPStrategyDefault
	}
}

// ======================== WebSocket 封装为 net.Conn ========================

type wsNetConn struct {
	ws       *websocket.Conn
	readMu   sync.Mutex
	writeMu  sync.Mutex
	reader   io.Reader
	deadCh   chan struct{}
	deadMu   sync.Mutex
	deadErr  error
	deadOnce sync.Once
}

func newWSNetConn(ws *websocket.Conn) *wsNetConn {
	return &wsNetConn{ws: ws, deadCh: make(chan struct{})}
}

func (c *wsNetConn) signalDead(err error) {
	if err == nil {
		return
	}
	c.deadMu.Lock()
	if c.deadErr == nil {
		c.deadErr = err
	}
	c.deadMu.Unlock()
	c.deadOnce.Do(func() {
		close(c.deadCh)
	})
}

func (c *wsNetConn) Dead() <-chan struct{} { return c.deadCh }

func (c *wsNetConn) DeadErr() error {
	c.deadMu.Lock()
	defer c.deadMu.Unlock()
	return c.deadErr
}

func (c *wsNetConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for {
		if c.reader == nil {
			mt, r, err := c.ws.NextReader()
			if err != nil {
				c.signalDead(err)
				return 0, err
			}
			if mt != websocket.BinaryMessage {
				continue
			}
			c.reader = r
		}
		n, err := c.reader.Read(p)
		if errors.Is(err, io.EOF) {
			c.reader = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		if err != nil {
			c.signalDead(err)
		}
		return n, err
	}
}

func (c *wsNetConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	w, err := c.ws.NextWriter(websocket.BinaryMessage)
	if err != nil {
		c.signalDead(err)
		return 0, err
	}
	n, writeErr := w.Write(p)
	closeErr := w.Close()
	if writeErr != nil {
		c.signalDead(writeErr)
		return n, writeErr
	}
	if closeErr != nil {
		c.signalDead(closeErr)
		return n, closeErr
	}
	return n, nil
}

func (c *wsNetConn) Close() error {
	err := c.ws.Close()
	if err != nil {
		c.signalDead(err)
	} else {
		c.signalDead(io.EOF)
	}
	return err
}

func (c *wsNetConn) LocalAddr() net.Addr {
	if nc := c.ws.UnderlyingConn(); nc != nil {
		return nc.LocalAddr()
	}
	return nil
}

func (c *wsNetConn) RemoteAddr() net.Addr {
	if nc := c.ws.UnderlyingConn(); nc != nil {
		return nc.RemoteAddr()
	}
	return nil
}

func (c *wsNetConn) SetDeadline(t time.Time) error {
	if err := c.ws.SetReadDeadline(t); err != nil {
		return err
	}
	return c.ws.SetWriteDeadline(t)
}
func (c *wsNetConn) SetReadDeadline(t time.Time) error  { return c.ws.SetReadDeadline(t) }
func (c *wsNetConn) SetWriteDeadline(t time.Time) error { return c.ws.SetWriteDeadline(t) }

// ======================== 流类型常量 ========================

const (
	streamKindTCP  byte = 1
	streamKindUDP  byte = 2
	streamKindPing byte = 3
)

// ======================== ECH 相关 ========================

const typeHTTPS = 65

func prepareECH() error {
	for {
		log.Printf("[客户端] DNS查询 ECH: %s -> %s", dnsServer, echDomain)
		echBase64, err := queryHTTPSRecord(echDomain, dnsServer)
		if err != nil {
			log.Printf("[客户端] DNS 查询失败: %v，重试...", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if echBase64 == "" {
			log.Printf("[客户端] 未找到 ECH 参数，重试...")
			time.Sleep(2 * time.Second)
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(echBase64)
		if err != nil {
			log.Printf("[客户端] ECH Base64 解码失败: %v，重试...", err)
			time.Sleep(2 * time.Second)
			continue
		}
		echListMu.Lock()
		echList = raw
		echListMu.Unlock()
		log.Printf("[客户端] ECHConfigList 长度: %d 字节", len(raw))
		return nil
	}
}

func refreshECH() error {
	if fallback {
		return nil
	}
	refreshMu.Lock()
	defer refreshMu.Unlock()
	log.Printf("[客户端] 刷新 ECH 配置...")
	return prepareECH()
}

func getECHList() ([]byte, error) {
	if fallback {
		return nil, nil
	}
	echListMu.RLock()
	defer echListMu.RUnlock()
	if len(echList) == 0 {
		return nil, errors.New("ECH 配置尚未加载")
	}
	return echList, nil
}

func buildTLSConfigWithECH(serverName string, echList []byte) (*tls.Config, error) {
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:                     tls.VersionTLS13,
		ServerName:                     serverName,
		EncryptedClientHelloConfigList: echList,
		EncryptedClientHelloRejectionVerify: func(cs tls.ConnectionState) error {
			return errors.New("服务器拒绝 ECH")
		},
		RootCAs: roots,
	}, nil
}

func buildStandardTLSConfig(serverName string) (*tls.Config, error) {
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		ServerName:         serverName,
		RootCAs:            roots,
		InsecureSkipVerify: insecure,
	}, nil
}

func buildUnifiedTLSConfig(serverName string) (*tls.Config, error) {
	if fallback {
		return buildStandardTLSConfig(serverName)
	}
	ech, e := getECHList()
	if e != nil {
		return nil, e
	}
	cfgTLS, err := buildTLSConfigWithECH(serverName, ech)
	if err != nil {
		return nil, err
	}
	cfgTLS.InsecureSkipVerify = insecure
	return cfgTLS, nil
}

func queryHTTPSRecord(domain, dnsServer string) (string, error) {
	if strings.HasPrefix(dnsServer, "http://") || strings.HasPrefix(dnsServer, "https://") {
		return queryDoH(domain, dnsServer)
	}
	return queryDNSUDP(domain, dnsServer)
}

func queryDNSUDP(domain, dnsServer string) (string, error) {
	if !strings.Contains(dnsServer, ":") {
		dnsServer = dnsServer + ":53"
	}
	query := buildDNSQuery(domain, typeHTTPS)
	conn, err := net.Dial("udp", dnsServer)
	if err != nil {
		return "", fmt.Errorf("连接 DNS 服务器失败: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err = conn.Write(query); err != nil {
		return "", fmt.Errorf("发送查询失败: %v", err)
	}
	response := make([]byte, 4096)
	n, err := conn.Read(response)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return "", fmt.Errorf("DNS 查询超时")
		}
		return "", fmt.Errorf("读取 DNS 响应失败: %v", err)
	}
	return parseDNSResponse(response[:n])
}

func queryDoH(domain, dohURL string) (string, error) {
	u, err := url.Parse(dohURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	dnsQuery := buildDNSQuery(domain, typeHTTPS)
	dnsBase64 := base64.RawURLEncoding.EncodeToString(dnsQuery)
	q.Set("dns", dnsBase64)
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/dns-message")
	req.Header.Set("Content-Type", "application/dns-message")
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("DoH 状态码: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return parseDNSResponse(body)
}

func buildDNSQuery(domain string, qtype uint16) []byte {
	query := make([]byte, 0, 512)
	query = append(query, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
	for _, label := range strings.Split(domain, ".") {
		query = append(query, byte(len(label)))
		query = append(query, []byte(label)...)
	}
	query = append(query, 0x00)
	query = append(query, byte(qtype>>8), byte(qtype), 0x00, 0x01)
	return query
}

func parseDNSResponse(response []byte) (string, error) {
	if len(response) < 12 {
		return "", fmt.Errorf("响应过短")
	}
	ancount := binary.BigEndian.Uint16(response[6:8])
	if ancount == 0 {
		return "", fmt.Errorf("无答案记录")
	}
	offset := 12
	for offset < len(response) && response[offset] != 0 {
		offset += int(response[offset]) + 1
	}
	offset += 5
	for i := 0; i < int(ancount); i++ {
		if offset >= len(response) {
			break
		}
		if response[offset]&0xC0 == 0xC0 {
			offset += 2
		} else {
			for offset < len(response) && response[offset] != 0 {
				offset += int(response[offset]) + 1
			}
			offset++
		}
		if offset+10 > len(response) {
			break
		}
		rrType := binary.BigEndian.Uint16(response[offset : offset+2])
		offset += 8
		dataLen := binary.BigEndian.Uint16(response[offset : offset+2])
		offset += 2
		if offset+int(dataLen) > len(response) {
			break
		}
		data := response[offset : offset+int(dataLen)]
		offset += int(dataLen)
		if rrType == typeHTTPS {
			if ech := parseHTTPSRecord(data); ech != "" {
				return ech, nil
			}
		}
	}
	return "", nil
}

func parseHTTPSRecord(data []byte) string {
	if len(data) < 2 {
		return ""
	}
	offset := 2
	if offset < len(data) && data[offset] == 0 {
		offset++
	} else {
		for offset < len(data) && data[offset] != 0 {
			offset += int(data[offset]) + 1
		}
		offset++
	}
	for offset+4 <= len(data) {
		key := binary.BigEndian.Uint16(data[offset : offset+2])
		length := binary.BigEndian.Uint16(data[offset+2 : offset+4])
		offset += 4
		if offset+int(length) > len(data) {
			break
		}
		value := data[offset : offset+int(length)]
		offset += int(length)
		if key == 5 {
			return base64.StdEncoding.EncodeToString(value)
		}
	}
	return ""
}

// ======================== 多通道客户端池 ========================

type ECHPool struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once

	wsServerAddr  string
	connectionNum int
	targetIPs     []string
	clientID      string

	wsConnsMu       sync.RWMutex
	smuxConns       []*smux.Session
	channelRTT      []int64
	channelErrCount []int32
	channelDown     []int64 // 实测下行吞吐（字节/秒），0=未测到
	channelUp       []int64 // 实测上行吞吐（字节/秒）
	selectCounter   uint64
	probeOnce       sync.Once
	probeRounds     int64
}

func NewECHPool(addr string, n int, ips []string, clientID string) *ECHPool {
	total := n
	if len(ips) > 0 {
		total = len(ips) * n
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ECHPool{
		ctx:             ctx,
		cancel:          cancel,
		wsServerAddr:    addr,
		connectionNum:   n,
		targetIPs:       ips,
		clientID:        clientID,
		smuxConns:       make([]*smux.Session, total),
		channelRTT:      make([]int64, total),
		channelErrCount: make([]int32, total),
		channelDown:     make([]int64, total),
		channelUp:       make([]int64, total),
	}
}

func (p *ECHPool) Start() {
	for i := 0; i < len(p.smuxConns); i++ {
		ip := ""
		if len(p.targetIPs) > 0 {
			if idx := i / p.connectionNum; idx < len(p.targetIPs) {
				ip = p.targetIPs[idx]
			}
		}
		go p.dialAndServe(i, ip)
	}
}

func (p *ECHPool) Close() {
	p.closeOnce.Do(func() {
		p.cancel()
		p.wsConnsMu.Lock()
		for i := range p.smuxConns {
			if p.smuxConns[i] != nil {
				_ = p.smuxConns[i].Close()
				p.smuxConns[i] = nil
			}
			p.channelRTT[i] = 0
		}
		p.wsConnsMu.Unlock()
	})
}

func (p *ECHPool) dialAndServe(idx int, ip string) {
	chID := idx + 1
	ipLabel := ip
	if strings.TrimSpace(ipLabel) == "" {
		ipLabel = "自动解析"
	}
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		wsConn, err := dialWebSocketWithECH(p.wsServerAddr, 3, ip, p.clientID, chID)
		if err != nil {
			log.Printf("[客户端] 通道 %d (IP:%s) 连接失败: %v", chID, ipLabel, err)
			setTunnelErr(fmt.Sprintf("通道 %d (IP:%s) 连接失败: %v", chID, ipLabel, err))
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
			continue
		}
		wsNet := newWSNetConn(wsConn)
		sess, err := smux.Client(wsNet, getSmuxConfig())
		if err != nil {
			_ = wsConn.Close()
			log.Printf("[客户端] 通道 %d (IP:%s) smux 初始化失败: %v", chID, ipLabel, err)
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(cfg.ReconnectDelay):
			}
			continue
		}
		p.wsConnsMu.Lock()
		p.smuxConns[idx] = sess
		p.channelRTT[idx] = 0
		p.channelErrCount[idx] = 0
		p.wsConnsMu.Unlock()
		log.Printf("[客户端] 通道 %d (IP:%s) 就绪 (smux)", chID, ipLabel)
		if rtt, err := p.probeChannelRTTOnce(sess, cfg.RTTProbeTimeout); err == nil {
			atomic.StoreInt64(&p.channelRTT[idx], rtt)
		}

		done := make(chan error, 1)
		go p.probeChannelRTT(sess, idx, done)
		var probeErr error
		select {
		case <-p.ctx.Done():
			_ = sess.Close()
			_ = wsConn.Close()
			<-done
			return
		case probeErr = <-done:
		case <-wsNet.Dead():
			_ = sess.Close()
			<-done
			probeErr = wsNet.DeadErr()
			if probeErr == nil {
				probeErr = io.EOF
			}
		}

		_ = sess.Close()
		_ = wsConn.Close()

		p.wsConnsMu.Lock()
		p.smuxConns[idx] = nil
		p.channelRTT[idx] = 0
		p.channelErrCount[idx] = 0
		p.wsConnsMu.Unlock()
		if probeErr != nil {
			log.Printf("[客户端] 通道 %d 断开原因: %v", chID, probeErr)
		}
		log.Printf("[客户端] 通道 %d 断开，重连中...", chID)
		select {
		case <-p.ctx.Done():
			return
		case <-time.After(cfg.ReconnectDelay):
		}
	}
}

func (p *ECHPool) probeChannelRTT(sess *smux.Session, idx int, done chan error) {
	var exitErr error
	defer func() {
		done <- exitErr
		close(done)
	}()
	ticker := time.NewTicker(cfg.RTTProbeTimeout)
	defer ticker.Stop()
	for {
		rtt, err := p.probeChannelRTTOnce(sess, cfg.RTTProbeTimeout)
		if err != nil {
			errCnt := atomic.AddInt32(&p.channelErrCount[idx], 1)
			atomic.StoreInt64(&p.channelRTT[idx], int64(cfg.RTTProbeTimeout.Nanoseconds()))

			// ⚡ 移动网络“僵尸连接快速熔断”机制：
			// 如果该通道连续 2 次探针严重超时（说明连接已经被移动基站黑洞丢弃），
			// 绝不在池子里苟延残喘害人，直接主动关闭当前 session 触发立即就地重连！
			if errCnt >= 2 || sess.IsClosed() {
				log.Printf("[客户端] 通道 %d 连续探测失败/僵死，主动熔断重连", idx+1)
				_ = sess.Close()
				exitErr = err
				return
			}
			<-ticker.C
			continue
		}
		// EWMA 平滑 RTT (60% 历史 + 40% 瞬时)
		oldRTT := atomic.LoadInt64(&p.channelRTT[idx])
		if oldRTT <= 0 {
			atomic.StoreInt64(&p.channelRTT[idx], rtt)
		} else {
			smoothed := int64(float64(oldRTT)*0.6 + float64(rtt)*0.4)
			atomic.StoreInt64(&p.channelRTT[idx], smoothed)
		}
		if idx >= 0 && idx < len(p.channelErrCount) {
			atomic.StoreInt32(&p.channelErrCount[idx], 0)
		}
		<-ticker.C
	}
}

func (p *ECHPool) probeChannelRTTOnce(sess *smux.Session, timeout time.Duration) (int64, error) {
	start := time.Now()
	s, err := sess.OpenStream()
	if err != nil {
		return 0, err
	}
	defer s.Close()
	_ = s.SetDeadline(time.Now().Add(timeout))
	if err := writeSmuxOpenHeader(s, streamKindPing, 0, ""); err != nil {
		return 0, err
	}
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, uint64(start.UnixNano()))
	if _, err := s.Write(payload); err != nil {
		return 0, err
	}
	ack := make([]byte, 8)
	if _, err := io.ReadFull(s, ack); err != nil {
		return 0, err
	}
	if !bytes.Equal(ack, payload) {
		return 0, fmt.Errorf("ping ack mismatch")
	}
	return time.Since(start).Nanoseconds(), nil
}

func (p *ECHPool) penalizeChannel(idx int) {
	if idx >= 0 && idx < len(p.channelErrCount) {
		atomic.AddInt32(&p.channelErrCount[idx], 1)
	}
}

func (p *ECHPool) openBestStream() (*smux.Stream, int, int, error) {
	p.wsConnsMu.RLock()
	type candidate struct {
		idx   int
		tier  int
		tp    int64
		rtt   int64
		score int64
	}
	cands := make([]candidate, 0, len(p.smuxConns))
	hasThroughput := false
	for i, sess := range p.smuxConns {
		if sess == nil || sess.IsClosed() {
			continue
		}
		rtt := atomic.LoadInt64(&p.channelRTT[i])
		if rtt <= 0 {
			rtt = int64(cfg.RTTProbeTimeout.Nanoseconds())
		}
		errCnt := atomic.LoadInt32(&p.channelErrCount[i])
		tp := atomic.LoadInt64(&p.channelDown[i])
		if tp > 0 {
			hasThroughput = true
		}
		// 延迟决定"点开网页等多久"，先分档保证体感；吞吐决定"下载跑多快"，档内再比。
		tier := rttTier(time.Duration(rtt))
		if errCnt > 0 { // 故障惩罚：累计 N 次故障等效降 N 档
			tier += int(errCnt)
			if tier > 3 {
				tier = 3
			}
		}
		cands = append(cands, candidate{
			idx: i, tier: tier, tp: tp, rtt: rtt,
			score: rtt + int64(errCnt)*int64(350*time.Millisecond),
		})
	}
	p.wsConnsMu.RUnlock()
	if len(cands) == 0 {
		return nil, 0, 0, fmt.Errorf("无可用 smux 通道")
	}

	bestTier := cands[0].tier
	for _, c := range cands[1:] {
		if c.tier < bestTier {
			bestTier = c.tier
		}
	}
	group := make([]candidate, 0, len(cands))
	for _, c := range cands {
		if c.tier == bestTier {
			group = append(group, c)
		}
	}

	if hasThroughput {
		var maxTP int64
		for _, c := range group {
			if c.tp > maxTP {
				maxTP = c.tp
			}
		}
		if maxTP > 0 {
			thr := int64(float64(maxTP) * 0.7)
			if thr < 64*1024 {
				thr = 64 * 1024
			}
			fast := make([]candidate, 0, len(group))
			for _, c := range group {
				if c.tp >= thr {
					fast = append(fast, c)
				}
			}
			if len(fast) > 0 {
				group = fast
			}
		}
	} else {
		// 无吞吐数据（老服务端或探测失败）：退回原来的纯延迟 + 容差窗口行为
		minScore := group[0].score
		for _, c := range group[1:] {
			if c.score < minScore {
				minScore = c.score
			}
		}
		tieWindow := int64(float64(minScore) * 0.35)
		if tieWindow < int64(25*time.Millisecond) {
			tieWindow = int64(25 * time.Millisecond)
		}
		near := make([]candidate, 0, len(group))
		for _, c := range group {
			if c.score <= minScore+tieWindow {
				near = append(near, c)
			}
		}
		if len(near) > 0 {
			group = near
		}
	}

	pick := int(atomic.AddUint64(&p.selectCounter, 1)-1) % len(group)
	// 逐条尝试：开流失败立刻换下一条（提升鲁棒性）
	for attempt := 0; attempt < len(group); attempt++ {
		c := group[(pick+attempt)%len(group)]
		p.wsConnsMu.RLock()
		sess := p.smuxConns[c.idx]
		p.wsConnsMu.RUnlock()
		if sess == nil || sess.IsClosed() {
			p.penalizeChannel(c.idx)
			continue
		}
		s, err := sess.OpenStream()
		if err != nil {
			p.penalizeChannel(c.idx)
			continue
		}
		return s, c.idx + 1, c.idx + 1, nil
	}
	return nil, 0, 0, fmt.Errorf("通道不可用")
}

func writeSmuxOpenHeader(w io.Writer, kind byte, strategy byte, target string) error {
	if len(target) > 65535 {
		return fmt.Errorf("目标地址过长")
	}
	head := make([]byte, 4)
	head[0] = kind
	head[1] = strategy
	binary.BigEndian.PutUint16(head[2:4], uint16(len(target)))
	if _, err := w.Write(head); err != nil {
		return err
	}
	if len(target) == 0 {
		return nil
	}
	_, err := w.Write([]byte(target))
	return err
}

func (p *ECHPool) openTCPStream(target string) (*smux.Stream, int, int, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		s, chID, decision, err := p.openBestStream()
		if err != nil {
			lastErr = err
			break
		}
		if err := writeSmuxOpenHeader(s, streamKindTCP, ipStrategy, target); err != nil {
			_ = s.Close()
			p.penalizeChannel(decision - 1)
			lastErr = err
			continue
		}
		return s, chID, decision, nil
	}
	return nil, 0, 0, lastErr
}

func (p *ECHPool) openUDPStream(target string) (*smux.Stream, int, int, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		s, chID, decision, err := p.openBestStream()
		if err != nil {
			lastErr = err
			break
		}
		if err := writeSmuxOpenHeader(s, streamKindUDP, ipStrategy, target); err != nil {
			_ = s.Close()
			p.penalizeChannel(decision - 1)
			lastErr = err
			continue
		}
		return s, chID, decision, nil
	}
	return nil, 0, 0, lastErr
}

// ======================== 分块读写 ========================

func writeChunk(w io.Writer, b []byte) error {
	if len(b) > 65535 {
		return fmt.Errorf("数据块过大")
	}
	h := make([]byte, 2)
	binary.BigEndian.PutUint16(h, uint16(len(b)))
	if _, err := w.Write(h); err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	_, err := w.Write(b)
	return err
}

func readChunk(r io.Reader) ([]byte, error) {
	h := make([]byte, 2)
	if _, err := io.ReadFull(r, h); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(h))
	if n == 0 {
		return nil, nil
	}
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}

func writeUDPReply(w io.Writer, addr string, payload []byte) error {
	if len(addr) > 65535 {
		return fmt.Errorf("地址过长")
	}
	head := make([]byte, 4)
	binary.BigEndian.PutUint16(head[0:2], uint16(len(addr)))
	binary.BigEndian.PutUint16(head[2:4], uint16(len(payload)))
	if _, err := w.Write(head); err != nil {
		return err
	}
	if len(addr) > 0 {
		if _, err := w.Write([]byte(addr)); err != nil {
			return err
		}
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func readUDPReply(r io.Reader) (string, []byte, error) {
	head := make([]byte, 4)
	if _, err := io.ReadFull(r, head); err != nil {
		return "", nil, err
	}
	addrLen := int(binary.BigEndian.Uint16(head[0:2]))
	dataLen := int(binary.BigEndian.Uint16(head[2:4]))
	addrRaw := make([]byte, addrLen)
	if addrLen > 0 {
		if _, err := io.ReadFull(r, addrRaw); err != nil {
			return "", nil, err
		}
	}
	data := make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := io.ReadFull(r, data); err != nil {
			return "", nil, err
		}
	}
	return string(addrRaw), data, nil
}

// ======================== WebSocket 拨号 ========================

func dialWebSocketWithECH(addr string, retries int, ip string, clientID string, channelID int) (*websocket.Conn, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "wss" && scheme != "ws" {
		return nil, fmt.Errorf("仅支持 ws:// 或 wss:// (当前: %s)", u.Scheme)
	}

	dialURL := *u
	q := dialURL.Query()
	if clientID != "" {
		q.Set("client_id", clientID)
	}
	if channelID > 0 {
		q.Set("channel_id", strconv.Itoa(channelID))
	}
	dialURL.RawQuery = q.Encode()
	dialAddr := dialURL.String()

	newDialer := func() websocket.Dialer {
		dialer := websocket.Dialer{
			HandshakeTimeout: cfg.WSHandshakeTimeout,
			ReadBufferSize:   cfg.ReadBuf,
			WriteBufferSize:  cfg.ReadBuf,
		}
		if token != "" {
			dialer.Subprotocols = []string{token}
		}
		if ip != "" {
			dialer.NetDial = func(network, address string) (net.Conn, error) {
				_, port, _ := net.SplitHostPort(address)
				var conn net.Conn
				var err error
				if host, p, e := net.SplitHostPort(ip); e == nil {
					conn, err = net.DialTimeout(network, net.JoinHostPort(host, p), cfg.DialTimeout)
				} else {
					conn, err = net.DialTimeout(network, net.JoinHostPort(ip, port), cfg.DialTimeout)
				}
				if err == nil {
					if tc, ok := conn.(*net.TCPConn); ok {
						_ = tc.SetNoDelay(true)
						_ = tc.SetKeepAlive(true)
						_ = tc.SetKeepAlivePeriod(10 * time.Second) // 10秒底层 TCP 心跳，保死 NAT 映射
					}
				}
				return conn, err
			}
		} else {
			dialer.NetDial = func(network, address string) (net.Conn, error) {
				conn, err := net.DialTimeout(network, address, cfg.DialTimeout)
				if err == nil {
					if tc, ok := conn.(*net.TCPConn); ok {
						_ = tc.SetNoDelay(true)
						_ = tc.SetKeepAlive(true)
						_ = tc.SetKeepAlivePeriod(10 * time.Second)
					}
				}
				return conn, err
			}
		}
		return dialer
	}

	if scheme == "ws" {
		dialer := newDialer()
		conn, resp, err := dialer.Dial(dialAddr, nil)
		if err != nil {
			if resp != nil && resp.StatusCode == http.StatusUnauthorized {
				return nil, fmt.Errorf("认证失败：Token 不匹配或未提供")
			}
			return nil, err
		}
		return conn, nil
	}

	serverName := u.Hostname()
	for i := 1; i <= retries; i++ {
		tlsCfg, e := buildUnifiedTLSConfig(serverName)
		if e != nil {
			if i < retries {
				_ = refreshECH()
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, e
		}

		dialer := newDialer()
		dialer.TLSClientConfig = tlsCfg

		conn, resp, err := dialer.Dial(dialAddr, nil)
		if err != nil {
			if resp != nil && resp.StatusCode == http.StatusUnauthorized {
				return nil, fmt.Errorf("认证失败：Token 不匹配或未提供")
			}
			if !fallback && (strings.Contains(err.Error(), "ECH") || strings.Contains(err.Error(), "ech")) && i < retries {
				_ = refreshECH()
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, err
		}
		return conn, nil
	}
	return nil, fmt.Errorf("连接失败")
}

// ======================== 辅助函数 ========================

func shortID(id string) string {
	if len(id) >= 8 {
		return id[:8]
	}
	return id
}

func clientSourceAddr(c net.Conn) string {
	if ra := c.RemoteAddr(); ra != nil {
		return ra.String()
	}
	return "-"
}

func logClientConnEvent(c net.Conn, reqType, target string, chID int, opened bool) {
	arrow := "关闭"
	if opened {
		arrow = "打开"
	}
	log.Printf("[客户端] %s %s %s %s 通道 %d", clientSourceAddr(c), reqType, arrow, target, chID)
}

type closeWriter interface {
	CloseWrite() error
}

func proxyConnStream(c net.Conn, stream *smux.Stream) {
	done := make(chan struct{}, 2)
	uploadDone := make(chan struct{}, 1)

	// 方向 1: c -> stream (客户端上传请求)
	go func() {
		bufPtr := bufPool.Get().(*[]byte)
		defer bufPool.Put(bufPtr)
		_, _ = io.CopyBuffer(stream, c, *bufPtr)
		// 客户端请求上传完成，优雅半关闭流写方向（发送 FIN 帧），保留对端回传通道
		_ = stream.CloseWrite()
		select {
		case uploadDone <- struct{}{}:
		default:
		}
		done <- struct{}{}
	}()

	// 方向 2: stream -> c (服务端回传响应，加入首字节看门狗保护)
	go func() {
		bufPtr := bufPool.Get().(*[]byte)
		defer bufPool.Put(bufPtr)

		// ⚡ 首字节超时熔断器（First-Byte Watchdog）：
		// 针对 YouTube 分片下载特性：请求发出后，正常情况下几百毫秒内远端必回传首个数据块；
		// 若因移动基站丢弃了该连接导致陷入黑洞，看门狗在 7 秒后主动熔断，避免死等 30 秒！
		stopWatchdog := make(chan struct{}, 1)
		go func() {
			select {
			case <-uploadDone:
				select {
				case <-time.After(7 * time.Second):
					// 请求已发完但 7 秒仍无任何下行响应，强制掐断本流促使浏览器秒级重试
					_ = stream.Close()
					_ = c.Close()
				case <-stopWatchdog:
				}
			case <-stopWatchdog:
			}
		}()

		// 读取第一个数据包，成功收到则立刻停止看门狗
		n, err := stream.Read(*bufPtr)
		select {
		case stopWatchdog <- struct{}{}:
		default:
		}

		if err == nil && n > 0 {
			if _, werr := c.Write((*bufPtr)[:n]); werr == nil {
				// 后续正常大流量全速中继
				_, _ = io.CopyBuffer(c, stream, *bufPtr)
			}
		}

		if cw, ok := c.(closeWriter); ok {
			_ = cw.CloseWrite()
		}
		done <- struct{}{}
	}()

	<-done

	// 给接收侧最多 3 秒宽限期（若服务端还在回传响应数据，绝不立刻暴力腰斩）
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}

	_ = stream.Close()
	_ = c.Close()
}

// ======================== SOCKS5 客户端代理 ========================

type ProxyConfig struct {
	Username, Password, Host string
}

type UDPAssociation struct {
	tcpConn       net.Conn
	udpListener   *net.UDPConn
	clientUDPAddr *net.UDPAddr
	pool          *ECHPool

	mu        sync.Mutex
	closed    bool
	receiving bool
	channelID int
	target    string
	stream    *smux.Stream
}

func parseAuthAndAddr(full string) (string, string, string, error) {
	u, p, h := "", "", full
	if strings.Contains(full, "@") {
		parts := strings.SplitN(full, "@", 2)
		if len(parts) != 2 {
			return "", "", "", fmt.Errorf("格式错误")
		}
		auth := parts[0]
		if strings.Contains(auth, ":") {
			ap := strings.SplitN(auth, ":", 2)
			u, p = ap[0], ap[1]
		}
		h = parts[1]
	}
	return h, u, p, nil
}

func runSOCKS5Listener(addr string) {
	h, u, p, err := parseAuthAndAddr(strings.TrimPrefix(addr, "socks5://"))
	if err != nil {
		log.Fatalf("[客户端] SOCKS5地址解析失败: %v", err)
	}
	l, err := net.Listen("tcp", h)
	if err != nil {
		log.Fatalf("[客户端] SOCKS5监听失败: %v", err)
	}
	log.Printf("[客户端] SOCKS5 代理: %s", h)
	cfgp := &ProxyConfig{u, p, h}
	for {
		c, err := l.Accept()
		if err != nil {
			continue
		}
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		go handleSOCKS5(c, cfgp)
	}
}

func handleSOCKS5(c net.Conn, cfgp *ProxyConfig) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 2)
	if _, err := io.ReadFull(c, buf); err != nil || buf[0] != 0x05 {
		return
	}
	methods := make([]byte, buf[1])
	_, _ = io.ReadFull(c, methods)
	if cfgp.Username != "" {
		_, _ = c.Write([]byte{0x05, 0x02})
		if err := handleSOCKS5UserPassAuth(c, cfgp); err != nil {
			return
		}
	} else {
		_, _ = c.Write([]byte{0x05, 0x00})
	}

	head := make([]byte, 4)
	if _, err := io.ReadFull(c, head); err != nil {
		return
	}
	var target string
	switch head[3] {
	case 0x01:
		b := make([]byte, 4)
		_, _ = io.ReadFull(c, b)
		target = net.IP(b).String()
	case 0x03:
		b := make([]byte, 1)
		_, _ = io.ReadFull(c, b)
		addr := make([]byte, b[0])
		_, _ = io.ReadFull(c, addr)
		target = string(addr)
	case 0x04:
		b := make([]byte, 16)
		_, _ = io.ReadFull(c, b)
		target = net.IP(b).String()
	}
	pb := make([]byte, 2)
	_, _ = io.ReadFull(c, pb)
	port := int(pb[0])<<8 | int(pb[1])
	target = net.JoinHostPort(target, fmt.Sprintf("%d", port))

	host, _, _ := net.SplitHostPort(target)
	ip := net.ParseIP(host)

	if head[1] == 0x01 {
		if ipStrategy == IPStrategyIPv4Only {
			if head[3] == 0x04 || (ip != nil && ip.To4() == nil) {
				_, _ = c.Write([]byte{0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
				return
			}
		}
		if ipStrategy == IPStrategyIPv6Only {
			if head[3] == 0x01 || (ip != nil && ip.To4() != nil) {
				_, _ = c.Write([]byte{0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
				return
			}
		}
	}

	_ = c.SetDeadline(time.Time{})

	switch head[1] {
	case 0x01:
		handleSOCKS5Connect(c, target)
	case 0x03:
		handleSOCKS5UDP(c, cfgp)
	}
}

func handleSOCKS5UserPassAuth(c net.Conn, cfgp *ProxyConfig) error {
	b := make([]byte, 2)
	_, _ = io.ReadFull(c, b)
	u := make([]byte, b[1])
	_, _ = io.ReadFull(c, u)
	_, _ = io.ReadFull(c, b[:1])
	p := make([]byte, b[0])
	_, _ = io.ReadFull(c, p)
	if string(u) == cfgp.Username && string(p) == cfgp.Password {
		_, _ = c.Write([]byte{0x01, 0x00})
		return nil
	}
	_, _ = c.Write([]byte{0x01, 0x01})
	return errors.New("认证失败")
}

func handleSOCKS5Connect(c net.Conn, target string) {
	_, err := c.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	if err != nil {
		_ = c.Close()
		return
	}
	/* ★ 必须先在锁内取到池子引用，绝不能在锁外直接读全局 echPool。
	   热切换时 StopSocksProxy 会先把 echPool 置 nil、100ms 后才建新的；
	   而自动优选每 15 分钟一轮、IP 一变就热重启，长期跑下来这个窗口必然被撞上。
	   撞上就是 nil 解引用 panic —— Go 侧 panic 会终结整个进程，
	   表现就是"整个 App 突然不干活了"。 */
	serviceMu.Lock()
	pool := echPool
	serviceMu.Unlock()
	if pool == nil {
		_ = c.Close()
		return
	}
	stream, _, decision, err := pool.openTCPStream(target)
	if err != nil {
		_ = c.Close()
		return
	}
	logClientConnEvent(c, "SOCKS5", target, decision, true)
	defer logClientConnEvent(c, "SOCKS5", target, decision, false)
	proxyConnStream(c, stream)
}

func handleSOCKS5UDP(c net.Conn, cfgp *ProxyConfig) {
	host, _, _ := net.SplitHostPort(cfgp.Host)
	uAddr, _ := net.ResolveUDPAddr("udp", net.JoinHostPort(host, "0"))
	ul, err := net.ListenUDP("udp", uAddr)
	if err != nil {
		_ = c.Close()
		return
	}
	defer ul.Close()

	actual, ok := ul.LocalAddr().(*net.UDPAddr)
	if !ok || actual == nil {
		_ = c.Close()
		return
	}
	resp := []byte{0x05, 0x00, 0x00}
	if ip4 := actual.IP.To4(); ip4 != nil {
		resp = append(resp, 0x01)
		resp = append(resp, ip4...)
	} else {
		resp = append(resp, 0x04)
		resp = append(resp, actual.IP...)
	}
	resp = append(resp, byte(actual.Port>>8), byte(actual.Port))
	if _, err := c.Write(resp); err != nil {
		_ = c.Close()
		return
	}

	/* 同 handleSOCKS5Connect：池子引用必须在锁内取，避免热切换窗口内拿到 nil。 */
	serviceMu.Lock()
	pool := echPool
	serviceMu.Unlock()
	if pool == nil {
		_ = ul.Close()
		_ = c.Close()
		return
	}

	assoc := &UDPAssociation{
		tcpConn:     c,
		udpListener: ul,
		pool:        pool,
		channelID:   -1,
	}

	go assoc.loop()
	b := make([]byte, 1)
	for {
		if _, err := c.Read(b); err != nil {
			assoc.Close()
			return
		}
	}
}

func (a *UDPAssociation) loop() {
	bufPtr := bufPool.Get().(*[]byte)
	buf := *bufPtr
	defer bufPool.Put(bufPtr)

	for {
		n, addr, err := a.udpListener.ReadFromUDP(buf)
		if err != nil {
			return
		}
		a.mu.Lock()
		if a.clientUDPAddr == nil {
			a.clientUDPAddr = addr
		} else if a.clientUDPAddr.String() != addr.String() {
			a.mu.Unlock()
			continue
		}
		a.mu.Unlock()

		tgt, data, err := parseSOCKS5UDPPacket(buf[:n])
		if err == nil {
			h, ps, _ := net.SplitHostPort(tgt)
			if ip := net.ParseIP(h); ip != nil {
				if ipStrategy == IPStrategyIPv4Only && ip.To4() == nil {
					continue
				}
				if ipStrategy == IPStrategyIPv6Only && ip.To4() != nil {
					continue
				}
			}
			var prt int
			_, _ = fmt.Sscanf(ps, "%d", &prt)
			if _, ok := udpBlockPorts[prt]; ok {
				continue
			}
			a.send(tgt, data)
		}
	}
}

func (a *UDPAssociation) send(target string, data []byte) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	needStart := !a.receiving
	if needStart {
		a.receiving = true
		a.target = target
	}
	stream := a.stream
	a.mu.Unlock()

	if needStart {
		s, id, decision, err := a.pool.openUDPStream(target)
		if err != nil {
			a.Close()
			return
		}
		a.mu.Lock()
		a.stream = s
		a.channelID = id
		stream = s
		a.mu.Unlock()
		logClientConnEvent(a.tcpConn, "SOCKS5-UDP", target, decision, true)
		go func() {
			for {
				addrStr, payload, e := readUDPReply(s)
				if e != nil {
					a.Close()
					return
				}
				a.handleUDPResponse(addrStr, payload)
			}
		}()
	} else {
		if target != "" && target != a.target {
			a.mu.Lock()
			a.target = target
			a.mu.Unlock()
		}
	}
	if stream == nil {
		a.Close()
		return
	}
	if err := writeChunk(stream, data); err != nil {
		a.Close()
	}
}

func (a *UDPAssociation) handleUDPResponse(addrStr string, data []byte) {
	host, portStr, _ := net.SplitHostPort(addrStr)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)
	pkt, _ := buildSOCKS5UDPPacket(host, port, data)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.clientUDPAddr != nil {
		_, _ = a.udpListener.WriteToUDP(pkt, a.clientUDPAddr)
	}
}

func (a *UDPAssociation) Close() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	stream := a.stream
	target := a.target
	chID := a.channelID
	a.closed = true
	a.stream = nil
	a.mu.Unlock()
	if stream != nil {
		_ = stream.Close()
	}
	if chID > 0 && target != "" {
		logClientConnEvent(a.tcpConn, "SOCKS5-UDP", target, chID, false)
	}
	_ = a.udpListener.Close()
	if a.tcpConn != nil {
		_ = a.tcpConn.Close()
	}
}

func parseSOCKS5UDPPacket(b []byte) (string, []byte, error) {
	if len(b) < 10 || b[2] != 0 {
		return "", nil, errors.New("数据不合法")
	}
	off := 4
	var h string
	switch b[3] {
	case 0x01:
		if off+4 > len(b) {
			return "", nil, errors.New("IPv4地址长度过短")
		}
		h = net.IP(b[off : off+4]).String()
		off += 4
	case 0x03:
		if off+1 > len(b) {
			return "", nil, errors.New("域名长度不足")
		}
		l := int(b[off])
		off++
		if off+l > len(b) {
			return "", nil, errors.New("域名长度不足")
		}
		h = string(b[off : off+l])
		off += l
	case 0x04:
		if off+16 > len(b) {
			return "", nil, errors.New("IPv6地址长度过短")
		}
		h = net.IP(b[off : off+16]).String()
		off += 16
	default:
		return "", nil, errors.New("地址类型无效")
	}
	if off+2 > len(b) {
		return "", nil, errors.New("端口字段过短")
	}
	p := int(b[off])<<8 | int(b[off+1])
	off += 2
	t := fmt.Sprintf("%s:%d", h, p)
	if b[3] == 0x04 {
		t = fmt.Sprintf("[%s]:%d", h, p)
	}
	return t, b[off:], nil
}

func buildSOCKS5UDPPacket(h string, p int, d []byte) ([]byte, error) {
	buf := []byte{0, 0, 0}
	ip := net.ParseIP(h)
	if ip4 := ip.To4(); ip4 != nil {
		buf = append(buf, 0x01)
		buf = append(buf, ip4...)
	} else if ip != nil {
		buf = append(buf, 0x04)
		buf = append(buf, ip...)
	} else {
		buf = append(buf, 0x03, byte(len(h)))
		buf = append(buf, h...)
	}
	buf = append(buf, byte(p>>8), byte(p))
	buf = append(buf, d...)
	return buf, nil
}
