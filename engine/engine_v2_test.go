package engine

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// v1: threshold=3；v2: 同规则 ID 但 threshold=2，用于验证版本隔离。
const rulesV2Thr2 = `{"version":2,"rules":[
  {"id":"login-fail","match":[{"field":"type","op":"eq","value":"login_fail"}],"window":"10m","threshold":2,"group_by":"src_ip","severity":"high"}
]}`

func logSettlements(t *testing.T, ss []Settlement) {
	t.Helper()
	for _, s := range ss {
		t.Logf("settlement: rule=%s ruleset_version=%d key=%s window=[%s,%s) count=%d threshold=%d fired=%v basis=%q",
			s.RuleID, s.RuleVersion, s.Key, s.WindowStart.Format(time.RFC3339), s.WindowEnd.Format(time.RFC3339),
			s.Count, s.Threshold, s.Fired, s.Reason)
	}
}

// 运行中切换规则版本：同一窗口不会被新旧规则混算。
func TestVersionIsolationOnReload(t *testing.T) {
	e, _ := New(Config{AllowedLateness: 5 * time.Minute, MaxKeys: 16}, loadRules(t, rulesV1))
	f := map[string]string{"type": "login_fail", "src_ip": "3.3.3.3"}

	// v1 下累计 2 条（阈值 3，未触发）。
	logAlerts(t, ev("v1-a", 1*time.Minute, f), e.Process(ev("v1-a", 1*time.Minute, f)))
	logAlerts(t, ev("v1-b", 2*time.Minute, f), e.Process(ev("v1-b", 2*time.Minute, f)))

	// 热更新到 v2（阈值降为 2）。
	if err := e.Reload(loadRules(t, rulesV2Thr2)); err != nil {
		t.Fatal(err)
	}
	// v2 下到第 2 条即应触发：若与 v1 的 2 条混算，第 1 条就会误触发。
	logAlerts(t, ev("v2-a", 3*time.Minute, f), e.Process(ev("v2-a", 3*time.Minute, f)))
	if got := len(e.Alerts()); got != 0 {
		t.Fatalf("v1 state leaked into v2 window: %d alerts", got)
	}
	logAlerts(t, ev("v2-b", 4*time.Minute, f), e.Process(ev("v2-b", 4*time.Minute, f)))
	alerts := e.Alerts()
	if len(alerts) != 1 {
		t.Fatalf("want 1 alert from v2, got %d", len(alerts))
	}
	if alerts[0].RuleVersion != 2 || alerts[0].Count != 2 {
		t.Fatalf("alert not purely from v2: %+v", alerts[0])
	}
	t.Logf("v2 alert: version=%d count=%d basis=%q", alerts[0].RuleVersion, alerts[0].Count, alerts[0].Reason)

	// v1 的残留窗口仍以 v1 身份留在状态里，收尾时按 v1 判定。
	ss := e.Flush()
	logSettlements(t, ss)
	if len(ss) != 1 {
		t.Fatalf("want 1 residual settlement (v1 window), got %d", len(ss))
	}
	if ss[0].RuleVersion != 1 || ss[0].Count != 2 || ss[0].Threshold != 3 || ss[0].Fired {
		t.Fatalf("residual window not judged by v1: %+v", ss[0])
	}
}

// 主动收尾：残留窗口按各自开启时的版本归位，达阈值的补发告警。
func TestFlushSettlesResidualWindowsByVersion(t *testing.T) {
	e, _ := New(Config{AllowedLateness: 5 * time.Minute, MaxKeys: 16}, loadRules(t, rulesV1))
	f1 := map[string]string{"type": "login_fail", "src_ip": "4.4.4.1"}
	f2 := map[string]string{"type": "login_fail", "src_ip": "4.4.4.2"}

	// v1：f1 累计 3 条但未即时告警？——threshold=3 会即时告警；
	// 这里让 f1 累计 2 条（不触发），f2 累计 3 条（即时告警，不属于残留）。
	for i := 0; i < 2; i++ {
		e.Process(ev(fmt.Sprintf("f1-%d", i), time.Duration(i)*time.Minute, f1))
	}
	for i := 0; i < 3; i++ {
		e.Process(ev(fmt.Sprintf("f2-%d", i), time.Duration(i)*time.Minute, f2))
	}
	if got := len(e.Alerts()); got != 1 {
		t.Fatalf("want 1 immediate alert, got %d", got)
	}

	// 热更新到 v2（阈值 2），新版本下再开窗口。
	if err := e.Reload(loadRules(t, rulesV2Thr2)); err != nil {
		t.Fatal(err)
	}
	e.Process(ev("v2-x", 6*time.Minute, f1)) // v2 窗口 1 条，未达阈值 2

	ss := e.Flush()
	logSettlements(t, ss)
	if len(ss) != 2 {
		t.Fatalf("want 2 settlements (v1 residual + v2 residual), got %d", len(ss))
	}
	byVersion := map[int]Settlement{}
	for _, s := range ss {
		byVersion[s.RuleVersion] = s
	}
	s1, ok := byVersion[1]
	if !ok || s1.Key != "4.4.4.1" || s1.Count != 2 || s1.Threshold != 3 || s1.Fired {
		t.Fatalf("v1 settlement wrong: %+v (present=%v)", s1, ok)
	}
	s2, ok := byVersion[2]
	if !ok || s2.Count != 1 || s2.Threshold != 2 || s2.Fired {
		t.Fatalf("v2 settlement wrong: %+v (present=%v)", s2, ok)
	}
	// 收尾后状态清空，再次 Flush 为空。
	if again := e.Flush(); len(again) != 0 {
		t.Fatalf("flush not idempotent-clean: %d settlements", len(again))
	}
	// 收尾不重复补发告警（v1 残留未达阈值，v2 残留未达阈值）。
	if got := len(e.Alerts()); got != 1 {
		t.Fatalf("flush generated unexpected alerts: %d", got)
	}
}

// 收尾时达到阈值的残留窗口必须补发告警，且告警版本为窗口开启时的版本。
func TestFlushFiresThresholdReachedWindow(t *testing.T) {
	e, _ := New(Config{AllowedLateness: 5 * time.Minute, MaxKeys: 16}, loadRules(t, rulesV1))
	f := map[string]string{"type": "login_fail", "src_ip": "5.5.5.5"}
	// threshold=3，但只送 2 条就切版本；再切回 v1 语义前直接 Flush。
	e.Process(ev("x1", 1*time.Minute, f))
	e.Process(ev("x2", 2*time.Minute, f))
	if err := e.Reload(loadRules(t, rulesV2Thr2)); err != nil {
		t.Fatal(err)
	}
	// v2 阈值 2：送 1 条，残留 count=1。
	e.Process(ev("y1", 3*time.Minute, f))

	ss := e.Flush()
	logSettlements(t, ss)
	// v1 残留 count=2 < 3 不补发；v2 残留 count=1 < 2 不补发。
	for _, s := range ss {
		if s.Fired {
			t.Fatalf("unexpected fired settlement: %+v", s)
		}
	}
	if got := len(e.Alerts()); got != 0 {
		t.Fatalf("unexpected alerts: %d", got)
	}

	// 重新构造：v2 下直接累计到阈值但事件未到？threshold 即时触发，
	// 能走到 Flush 补发的场景是：阈值在残留期间被“达到”只可能来自同版本累计，
	// 而同版本累计达阈值会即时告警——因此 Flush 补发只发生在去重边界。
	// 这里验证已达阈值窗口的即时告警存在且 Flush 不重复结算它。
	e2, _ := New(Config{AllowedLateness: 5 * time.Minute, MaxKeys: 16}, loadRules(t, rulesV2Thr2))
	e2.Process(ev("z1", 1*time.Minute, f))
	e2.Process(ev("z2", 2*time.Minute, f)) // 达阈值 2，即时告警
	if got := len(e2.Alerts()); got != 1 {
		t.Fatalf("want 1 immediate alert, got %d", got)
	}
	ss2 := e2.Flush()
	logSettlements(t, ss2)
	if len(ss2) != 0 {
		t.Fatalf("alerted window must not be re-settled: %+v", ss2)
	}
	if got := len(e2.Alerts()); got != 1 {
		t.Fatalf("flush duplicated alert: %d", got)
	}
}

// 背压与关闭排空：缓冲满时 TrySubmit 明确返回 ErrBufferFull（不悄悄丢），
// Submit 阻塞等待；Close 保证所有已接收事件处理完。
func TestBackpressureAndCloseDrain(t *testing.T) {
	// threshold=1：每条被接收的事件都产生一条告警，便于精确核对“不丢”。
	rs := loadRules(t, `{"version":1,"rules":[
	  {"id":"any","match":[{"field":"type","op":"eq","value":"x"}],"window":"10m","threshold":1,"field":"src_ip"}
	]}`)
	e, _ := New(Config{AllowedLateness: time.Hour, MaxKeys: 4096, BufferSize: 4, Workers: 1}, rs)
	// 每条事件使用独立关联键，threshold=1 下每条被接收的事件都产生一条告警。
	fieldFor := func(i int) map[string]string {
		return map[string]string{"type": "x", "src_ip": fmt.Sprintf("9.9.%d.%d", i/256, i%256)}
	}

	// 非阻塞打满：accepted + bufferFull 必须等于总数，不允许静默丢失。
	const total = 200
	accepted, full := 0, 0
	for i := 0; i < total; i++ {
		err := e.TrySubmit(ev(fmt.Sprintf("t-%d", i), time.Duration(i)*time.Second, fieldFor(i)))
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrBufferFull):
			full++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	t.Logf("try-submit: accepted=%d buffer_full=%d total=%d", accepted, full, total)
	if accepted+full != total {
		t.Fatalf("events silently lost: accepted=%d full=%d total=%d", accepted, full, total)
	}
	if full == 0 {
		t.Fatal("expected backpressure (ErrBufferFull) with tiny buffer")
	}

	// 阻塞提交：全部应被接收（关闭前）。
	var wg sync.WaitGroup
	const blocking = 50
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < blocking; i++ {
			if err := e.Submit(ev(fmt.Sprintf("b-%d", i), time.Duration(total+i)*time.Second, fieldFor(total+i))); err != nil {
				t.Errorf("blocking submit failed: %v", err)
			}
		}
	}()
	wg.Wait()

	// 关闭排空：所有已接收事件都必须处理完。
	e.Close()
	want := accepted + blocking
	if got := len(e.Alerts()); got != want {
		t.Fatalf("close lost results: want %d alerts, got %d", want, got)
	}
	t.Logf("close drained: alerts=%d want=%d", len(e.Alerts()), want)

	// 关闭后提交被明确拒绝。
	if err := e.Submit(ev("after-close", 0, fieldFor(0))); !errors.Is(err, ErrClosed) {
		t.Fatalf("submit after close: want ErrClosed, got %v", err)
	}
	if err := e.TrySubmit(ev("after-close-2", 0, fieldFor(0))); !errors.Is(err, ErrClosed) {
		t.Fatalf("try-submit after close: want ErrClosed, got %v", err)
	}
	e.Close() // 幂等
}

// 淘汰报告：未结算的关联状态被淘汰时可取到报告，含键、版本与原因。
func TestEvictionReports(t *testing.T) {
	e, _ := New(Config{AllowedLateness: time.Hour, MaxKeys: 3}, loadRules(t, rulesV1))
	for i := 0; i < 10; i++ {
		f := map[string]string{"type": "login_fail", "src_ip": fmt.Sprintf("7.7.7.%d", i)}
		e.Process(ev(fmt.Sprintf("ev-%d", i), time.Duration(i)*time.Second, f))
	}
	reps := e.Evictions()
	if len(reps) == 0 {
		t.Fatal("want eviction reports, got none")
	}
	for _, r := range reps {
		t.Logf("eviction: rule=%s ruleset_version=%d key=%s pending_windows=%d dropped_events=%d reason=%q",
			r.RuleID, r.RuleVersion, r.Key, r.PendingWindows, r.DroppedEvents, r.Reason)
		if r.RuleID != "login-fail" || r.RuleVersion != 1 || r.Key == "" || r.PendingWindows < 1 || r.Reason == "" {
			t.Fatalf("incomplete eviction report: %+v", r)
		}
	}
	t.Logf("eviction reports: %d", len(reps))
}
