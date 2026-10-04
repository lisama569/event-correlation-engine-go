package engine

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// 内存吃紧淘汰关联状态时，尚未结算的窗口不得静默丢失：
// 必须能通过 Evictions() 取回报告，且报告含关联键、规则版本与淘汰原因。
func TestEvictionReportsRecoverable(t *testing.T) {
	rsV1 := loadRules(t, `{"version":1,"rules":[
	  {"id":"login-fail","match":[{"field":"type","op":"eq","value":"login_fail"}],"window":"1h","threshold":100,"field":"src_ip"}
	]}`)
	// 阈值设得很高、窗口很长，保证这些窗口在被淘汰时都是"未结算"状态。
	// 允许迟到也拉长，避免被水位 gc 提前回收，确保是 LRU 容量淘汰。
	e, _ := New(Config{AllowedLateness: time.Hour, MaxKeys: 3}, rsV1)

	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		ip := fmt.Sprintf("10.0.0.%d", i)
		seen[ip] = true
		f := map[string]string{"type": "login_fail", "src_ip": ip}
		e.Process(ev(fmt.Sprintf("k%d", i), time.Duration(i)*time.Second, f))
	}

	e.mu.Lock()
	n := len(e.states)
	e.mu.Unlock()
	if n > 3 {
		t.Fatalf("state count %d exceeds MaxKeys=3", n)
	}

	evictions := e.Evictions()
	// 8 个键、上限 3：最早创建的 5 个键被淘汰，每个键 1 个未结算窗口。
	if len(evictions) != 5 {
		t.Fatalf("want 5 eviction reports, got %d: %+v", len(evictions), evictions)
	}
	reported := map[string]bool{}
	for _, ev := range evictions {
		t.Logf("eviction rule=%s version=%d key=%s window=[%s,%s) count=%d/%d reason=%q",
			ev.RuleID, ev.RuleVersion, ev.Key,
			ev.WindowStart.Format(time.RFC3339), ev.WindowEnd.Format(time.RFC3339),
			ev.Count, ev.Threshold, ev.Reason)
		if ev.RuleID != "login-fail" || ev.Key == "" {
			t.Fatalf("eviction must identify rule and key: %+v", ev)
		}
		if ev.RuleVersion != 1 {
			t.Fatalf("eviction must carry pinned ruleset version=1: %+v", ev)
		}
		if ev.Count != 1 || ev.Threshold != 100 {
			t.Fatalf("eviction must carry pending counts: %+v", ev)
		}
		if !strings.Contains(ev.Reason, "evicted by LRU") {
			t.Fatalf("eviction reason must explain why: %q", ev.Reason)
		}
		if reported[ev.Key] {
			t.Fatalf("duplicate eviction report for key %s", ev.Key)
		}
		reported[ev.Key] = true
	}

	// 被淘汰的应是最早创建的 5 个键（访问顺序即创建顺序，无重复访问）。
	for i := 0; i < 5; i++ {
		ip := fmt.Sprintf("10.0.0.%d", i)
		if !reported[ip] {
			t.Fatalf("expected eviction report for earliest key %s, got keys %v", ip, reported)
		}
	}
}

// 已经触发过告警的窗口在被 LRU 淘汰时不重复出报告（结论已在 Alerts 中可查）。
func TestEvictionSkipsAlertedWindows(t *testing.T) {
	rs := loadRules(t, `{"version":2,"rules":[
	  {"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"1h","threshold":1,"group_by":"g"}
	]}`)
	e, _ := New(Config{AllowedLateness: time.Hour, MaxKeys: 2}, rs)
	// fired1/fired2 达到阈值=1 立即告警，随后 c0..c4 不断把旧键挤出容量上限。
	for _, g := range []string{"fired1", "fired2"} {
		e.Process(ev("f-"+g, 0, map[string]string{"t": "x", "g": g}))
	}
	for i := 0; i < 5; i++ {
		e.Process(ev(fmt.Sprintf("c%d", i), time.Duration(1+i)*time.Second,
			map[string]string{"t": "x", "g": fmt.Sprintf("c%d", i)}))
	}
	if got := len(e.Evictions()); got != 0 {
		t.Fatalf("alerted windows should not be reported on eviction, got %d: %+v", got, e.Evictions())
	}
	if got := len(e.Alerts()); got != 7 { // fired1,fired2 + c0..c4
		t.Fatalf("all reached-threshold windows must remain retrievable alerts, got %d: %+v", got, e.Alerts())
	}
	t.Logf("alerts remain retrievable: %d; no noisy eviction reports", len(e.Alerts()))
}
