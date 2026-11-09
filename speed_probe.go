package tunnel

// ============================================================
// 通道吞吐探测与「入口优选」（Android 版，2026-11-09）
//
// 与 Windows/macOS 端共用同一套思路：旧的优选只看 TCP 握手延迟，
// 而实测同一个节点、同一分钟内不同 Cloudflare 入口 IP 的单流下行
// 可以相差 27 倍——"握手快但回程烂"的入口会被错误地排到前面。
//
// 这里的策略：
//   1. 选路先按 RTT 分档（延迟决定"点开网页等多久"），档内再按**实测吞吐**
//      取快组（吞吐决定"下载/视频跑多快"）。
//   2. 吞吐用服务端内嵌的测速端点（streamKindSpeed）实测，探测流量走隧道本身，
//      测到的就是"手机 → CF 入口 → CF 骨干 → 节点"整段真实质量。
//   3. 以「入口 IP」为单位探测（同 IP 的多条通道共享一份成绩），
//      每轮每入口最多下行 256KB，只测延迟最优的前 8 个入口，控制流量。
//   4. 周期性复测（15 分钟）：移动网络质量剧烈波动，优选必须"活着"。
// ============================================================

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtaci/smux"
)

// streamKindSpeed 测速流类型（老服务端不认识该类型时会关闭流，
// 客户端探测失败后自动退回纯 RTT 选路，可向后兼容）
const streamKindSpeed byte = 4

const (
	probeDownBytes  = 256 * 1024             // 单入口每次下行探测上限（控制流量）
	probeUpBytes    = 128 * 1024             // 单入口每次上行探测上限
	probeWindow     = 400 * time.Millisecond // 取样时间窗
	probeCycle      = 15 * time.Minute       // 复测周期
	probeMinSample  = 8 * 1024               // 样本下限
	probeWorkers    = 3                      // 并发探测数
	probeMaxEntries = 8                      // 每轮最多实测的入口数
)

// speedProbePayload 预置不可压缩伪随机块（避免被中间层压缩导致测速虚高）
var speedProbePayload = func() []byte {
	b := make([]byte, 64*1024)
	x := uint64(0x9E3779B97F4A7C15)
	for i := range b {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		b[i] = byte(x)
	}
	return b
}()

// rttTier 延迟分档；rtt<=0 表示尚未测到，给中性档位靠吞吐胜负
func rttTier(rtt time.Duration) int {
	switch {
	case rtt <= 0:
		return 1
	case rtt < 100*time.Millisecond:
		return 0
	case rtt < 200*time.Millisecond:
		return 1
	case rtt < 400*time.Millisecond:
		return 2
	default:
		return 3
	}
}

// channelIPIdx 通道 → 入口 IP 序号
func (p *ECHPool) channelIPIdx(idx int) int {
	if len(p.targetIPs) == 0 || p.connectionNum <= 0 {
		return -1
	}
	return idx / p.connectionNum
}

// StartThroughputProbe 启动周期性吞吐探测（幂等）
func (p *ECHPool) StartThroughputProbe() {
	p.probeOnce.Do(func() {
		go func() {
			time.Sleep(4 * time.Second)
			p.probeAllChannels()
			time.Sleep(25 * time.Second) // 补测晚建连的通道
			p.probeAllChannels()
			for {
				time.Sleep(probeCycle)
				p.probeAllChannels()
			}
		}()
	})
}

// probeAllChannels 以「入口 IP」为单位做一轮吞吐探测，成绩广播给同 IP 的所有通道
func (p *ECHPool) probeAllChannels() {
	p.wsConnsMu.RLock()
	reps := make(map[int]int)
	for i, sess := range p.smuxConns {
		if sess == nil || sess.IsClosed() {
			continue
		}
		ipIdx := p.channelIPIdx(i)
		if _, ok := reps[ipIdx]; !ok {
			reps[ipIdx] = i
		}
	}
	sessions := make([]*smux.Session, len(p.smuxConns))
	copy(sessions, p.smuxConns)
	p.wsConnsMu.RUnlock()
	if len(reps) == 0 {
		return
	}

	type entry struct {
		ipIdx int
		chIdx int
		rtt   int64
	}
	entries := make([]entry, 0, len(reps))
	for ipIdx, chIdx := range reps {
		rtt := atomic.LoadInt64(&p.channelRTT[chIdx])
		if rtt <= 0 {
			rtt = int64(cfg.RTTProbeTimeout.Nanoseconds()) / 2
		}
		entries = append(entries, entry{ipIdx, chIdx, rtt})
	}
	for i := 1; i < len(entries); i++ { // 按 RTT 升序插入排序
		for j := i; j > 0 && entries[j].rtt < entries[j-1].rtt; j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}
	if len(entries) > probeMaxEntries {
		entries = entries[:probeMaxEntries]
	}

	type result struct {
		ipIdx    int
		down, up int64
	}
	out := make(chan result, len(entries))
	sem := make(chan struct{}, probeWorkers)
	var wg sync.WaitGroup
	first := atomic.LoadInt64(&p.probeRounds) == 0
	for _, e := range entries {
		sess := sessions[e.chIdx]
		if sess == nil || sess.IsClosed() {
			continue
		}
		wg.Add(1)
		go func(ipIdx int, s *smux.Session) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			down := p.measureDown(s, probeDownBytes)
			up := int64(0)
			if first {
				up = p.measureUp(s, probeUpBytes)
			}
			out <- result{ipIdx, down, up}
		}(e.ipIdx, sess)
	}
	wg.Wait()
	close(out)

	measured := 0
	for r := range out {
		if r.down <= 0 && r.up <= 0 {
			continue
		}
		measured++
		for i := range sessions {
			if p.channelIPIdx(i) != r.ipIdx {
				continue
			}
			if r.down > 0 {
				atomic.StoreInt64(&p.channelDown[i], r.down)
			}
			if r.up > 0 {
				atomic.StoreInt64(&p.channelUp[i], r.up)
			}
		}
	}
	atomic.AddInt64(&p.probeRounds, 1)
	if measured > 0 {
		p.logChannelRanking()
	}
}

// measureDown 限定时间窗内实测下行吞吐（字节/秒）
func (p *ECHPool) measureDown(sess *smux.Session, maxBytes int) int64 {
	s, err := sess.OpenStream()
	if err != nil {
		return 0
	}
	defer s.Close()
	_ = s.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeSmuxOpenHeader(s, streamKindSpeed, 0, fmt.Sprintf("d:%d", maxBytes)); err != nil {
		return 0
	}
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(s, hdr); err != nil {
		return 0 // 老服务端不支持：静默退回 RTT 选路
	}
	want := int(binary.BigEndian.Uint32(hdr))
	if want <= 0 {
		return 0
	}
	if want > maxBytes {
		want = maxBytes
	}
	buf := make([]byte, 32*1024)
	start := time.Now()
	got := 0
	for got < want {
		if time.Since(start) > probeWindow {
			break
		}
		n, err := s.Read(buf)
		if n > 0 {
			got += n
		}
		if err != nil {
			break
		}
	}
	el := time.Since(start)
	if got < probeMinSample || el <= 0 {
		return 0
	}
	return int64(float64(got) / el.Seconds())
}

// measureUp 限定时间窗内实测上行吞吐（字节/秒）
func (p *ECHPool) measureUp(sess *smux.Session, maxBytes int) int64 {
	s, err := sess.OpenStream()
	if err != nil {
		return 0
	}
	defer s.Close()
	_ = s.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeSmuxOpenHeader(s, streamKindSpeed, 0, fmt.Sprintf("u:%d", maxBytes)); err != nil {
		return 0
	}
	buf := make([]byte, 16*1024)
	start := time.Now()
	sent := 0
	for sent < maxBytes {
		if time.Since(start) > probeWindow {
			break
		}
		n := len(buf)
		if n > maxBytes-sent {
			n = maxBytes - sent
		}
		w, err := s.Write(buf[:n])
		sent += w
		if err != nil {
			break
		}
	}
	el := time.Since(start)
	ack := make([]byte, 1)
	_, _ = io.ReadFull(s, ack)
	if sent < probeMinSample || el <= 0 {
		return 0
	}
	return int64(float64(sent) / el.Seconds())
}

// logChannelRanking 输出本轮优选排名（会进入 App 的诊断日志）
func (p *ECHPool) logChannelRanking() {
	type row struct {
		ip   string
		tier int
		rtt  int64
		down int64
	}
	seen := make(map[int]bool)
	rows := make([]row, 0, 16)
	for i := range p.smuxConns {
		ipIdx := p.channelIPIdx(i)
		if ipIdx < 0 || seen[ipIdx] {
			continue
		}
		seen[ipIdx] = true
		ip := p.targetIPs[ipIdx]
		if strings.TrimSpace(ip) == "" {
			ip = "DNS 自动解析"
		}
		rtt := atomic.LoadInt64(&p.channelRTT[i])
		rows = append(rows, row{ip: ip, tier: rttTier(time.Duration(rtt)), rtt: rtt, down: atomic.LoadInt64(&p.channelDown[i])})
	}
	if len(rows) == 0 {
		return
	}
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0; j-- {
			better := rows[j].tier < rows[j-1].tier ||
				(rows[j].tier == rows[j-1].tier && rows[j].down > rows[j-1].down)
			if !better {
				break
			}
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
	parts := make([]string, 0, len(rows))
	for k, r := range rows {
		mark := "  "
		if k == 0 {
			mark = "★ "
		}
		parts = append(parts, fmt.Sprintf("%s%s %d档 RTT=%dms 下行=%.2fMB/s",
			mark, r.ip, r.tier, r.rtt/1e6, float64(r.down)/1024/1024))
	}
	log.Printf("[入口优选] 本轮实测排名（按延迟分档 + 档内吞吐）: %s", strings.Join(parts, " ｜ "))
}
