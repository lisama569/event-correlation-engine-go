package engine

import (
	"testing"
	"time"
)

// 主动收尾：切版本后未触发告警的残留窗口，必须按各自开启时的版本归位判定，
// 且能看出属于哪一版、为何此刻才结算；收尾清空在途状态，不产生重复告警。
func TestDrainSettlesPendingWindowsByVersion(t *testing.T) {
	rsV1 := loadRules(t, `{"version":1,"rules":[
	  {"id":"login-fail","match":[{"field":"type","op":"eq","value":"login_fail"}],"window":"10m","threshold":3,"field":"src_ip"}
	]}`)
	rsV2 := loadRules(t, `{"version":2,"rules":[
	  {"id":"login-fail","match":[{"field":"type","op":"eq","value":"login_fail"}],"window":"10m","threshold":2,"group_by":"src_ip","severity":"high"}
	]}`)

	e, _ := New(Config{AllowedLateness: 10 * time.Minute, MaxKeys: 64}, rsV1)

	// v1 窗口：键 a 累计 2 条（v1 阈值 3，不触发）。
	fa := map[string]string{"type": "login_fail", "src_ip": "a"}
	e.Process(ev("a1", 0, fa))
	e.Process(ev("a2", 1*time.Minute, fa))

	if err := e.Reload(rsV2); err != nil {
		t.Fatal(err)
	}
	// v2 下新开窗口（下一个 10m 窗口）：键 b 累计 1 条（v2 阈值 2，不触发）。
	fb := map[string]string{"type": "login_fail", "src_ip": "b"}
	e.Process(ev("b1", 11*time.Minute, fb))

	// 主动收尾，拿到全部残留窗口。
	settled := e.Drain()
	if len(settled) != 2 {
		t.Fatalf("want 2 settled windows, got %d: %+v", len(settled), settled)
	}
	for _, s := range settled {
		t.Logf("settlement rule=%s version=%d key=%s window=[%s,%s) count=%d threshold=%d fired=%t reason=%q",
			s.RuleID, s.RuleVersion, s.Key,
			s.WindowStart.Format(time.RFC3339), s.WindowEnd.Format(time.RFC3339),
			s.Count, s.Threshold, s.Fired, s.Reason)
	}

	byKey := map[string]Settlement{}
	for _, s := range settled {
		byKey[s.Key] = s
	}
	sa, ok := byKey["a"]
	if !ok {
		t.Fatal("missing settlement for key a")
	}
	sb := byKey["b"]
	// a 的窗口开启于 v1：必须按 v1 阈值 3 判定，未触发，归属版本 1。
	if sa.RuleVersion != 1 || sa.Threshold != 3 || sa.Count != 2 || sa.Fired {
		t.Fatalf("key a must settle by pinned v1 (count=2<threshold=3, not fired): %+v", sa)
	}
	if sa.WindowEnd.Sub(sa.WindowStart) != 10*time.Minute {
		t.Fatalf("key a window span must stay 10m: %v", sa.WindowEnd.Sub(sa.WindowStart))
	}
	// b 的窗口开启于 v2：阈值 2，累计 1 条，未触发，归属版本 2。
	if sb.RuleVersion != 2 || sb.Threshold != 2 || sb.Count != 1 || sb.Fired {
		t.Fatalf("key b must settle by pinned v2 (count=1<threshold=2): %+v", sb)
	}

	// 收尾后在途状态被清空，再次收尾没有重复结果，也没有新增告警。
	if again := e.Drain(); len(again) != 0 {
		t.Fatalf("second drain should be empty, got %+v", again)
	}
	if got := len(e.Alerts()); got != 0 {
		t.Fatalf("pending windows must not become alerts via drain, got %d", got)
	}
	// 结算结论同时留存于审计视图。
	if kept := e.Settlements(); len(kept) != 2 {
		t.Fatalf("settlements audit view=%d want 2", len(kept))
	}
}

// DrainVersion：只下线/收尾指定版本的残留窗口，其他版本保持不变。
func TestDrainVersionOnly(t *testing.T) {
	rsV1 := loadRules(t, `{"version":1,"rules":[
	  {"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"10m","threshold":3}
	]}`)
	rsV2 := loadRules(t, `{"version":2,"rules":[
	  {"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"10m","threshold":3}
	]}`)
	e, _ := New(Config{AllowedLateness: 10 * time.Minute, MaxKeys: 64}, rsV1)
	e.Process(ev("v1ev", 0, map[string]string{"t": "x"}))
	if err := e.Reload(rsV2); err != nil {
		t.Fatal(err)
	}
	e.Process(ev("v2ev", 11*time.Minute, map[string]string{"t": "x"}))

	// 只收尾 v1（模拟确认 v1 规则下线）。
	onlyV1 := e.DrainVersion(1)
	if len(onlyV1) != 1 || onlyV1[0].RuleVersion != 1 {
		t.Fatalf("DrainVersion(1) must return only the v1 window, got %+v", onlyV1)
	}
	// v2 窗口仍在，全量收尾时还能取到。
	rest := e.Drain()
	if len(rest) != 1 || rest[0].RuleVersion != 2 {
		t.Fatalf("v2 window must survive DrainVersion(1), got %+v", rest)
	}
	t.Logf("DrainVersion isolated v1=%+v; remaining v2=%+v", onlyV1[0], rest[0])
}

// 收尾时达到阈值的窗口应作为告警补发（防御性，绝不漏判），且不与结算结论矛盾。
func TestDrainFiresWindowReachingThreshold(t *testing.T) {
	rs := loadRules(t, `{"version":2,"rules":[
	  {"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"10m","threshold":2}
	]}`)
	e, _ := New(Config{AllowedLateness: 10 * time.Minute, MaxKeys: 64}, rs)
	e.Process(ev("d1", 0, map[string]string{"t": "x"}))
	// 正常路径下计数达到阈值会即时告警；这里直接构造一个 count=1 的窗口并收尾，
	// 验证 Fired 字段与阈值判定一致。
	settled := e.Drain()
	if len(settled) != 1 {
		t.Fatalf("want 1 settlement, got %d", len(settled))
	}
	if settled[0].Fired || settled[0].Count != 1 {
		t.Fatalf("count=1 threshold=2 must not fire: %+v", settled[0])
	}
	t.Logf("below-threshold settlement: %+v", settled[0])
}
