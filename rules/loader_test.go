package rules

import (
	"errors"
	"testing"
	"time"
)

func mustLoad(t *testing.T, data string) *RuleSet {
	t.Helper()
	rs, err := Load([]byte(data))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	t.Logf("loaded ruleset version=%d rules=%d", rs.Version, len(rs.Rules))
	return rs
}

func mustFailKind(t *testing.T, data string, kind LoadErrorKind) {
	t.Helper()
	_, err := Load([]byte(data))
	if err == nil {
		t.Fatalf("expected load error kind=%d, got nil", kind)
	}
	var le *LoadError
	if !errors.As(err, &le) {
		t.Fatalf("expected *LoadError, got %T: %v", err, err)
	}
	if le.Kind != kind {
		t.Fatalf("expected kind=%d, got kind=%d (%s)", kind, le.Kind, le.Detail)
	}
	t.Logf("rejected as expected: kind=%d detail=%s", le.Kind, le.Detail)
}

func TestLoadV1Basic(t *testing.T) {
	rs := mustLoad(t, `{
	  "version": 1,
	  "rules": [
	    {"id":"r1","match":[{"field":"type","op":"eq","value":"login_fail"}],"window":"5m","threshold":3,"field":"src_ip"}
	  ]
	}`)
	r, ok := rs.ByID("r1")
	if !ok {
		t.Fatal("rule r1 not found")
	}
	if r.Window != 5*time.Minute {
		t.Fatalf("window=%v want 5m", r.Window)
	}
	// v1 废弃字段 field 按明确映射迁移到 group_by
	if r.GroupBy != "src_ip" {
		t.Fatalf("group_by=%q want src_ip (mapped from legacy field)", r.GroupBy)
	}
	// 新增可选字段的稳定默认值
	if r.Severity != DefaultSeverity {
		t.Fatalf("severity=%q want default %q", r.Severity, DefaultSeverity)
	}
	t.Logf("rule r1: window=%v group_by=%q severity=%q", r.Window, r.GroupBy, r.Severity)
}

func TestLoadV2DefaultsStableAcrossOrder(t *testing.T) {
	// 同样的规则以不同顺序加载，默认语义必须一致。
	a := mustLoad(t, `{"version":2,"rules":[
	  {"id":"a","match":[{"field":"t","op":"eq","value":"x"}],"window":"1m","threshold":1},
	  {"id":"b","match":[{"field":"t","op":"eq","value":"y"}],"window":"1m","threshold":1,"severity":"high"}
	]}`)
	b := mustLoad(t, `{"version":2,"rules":[
	  {"id":"b","match":[{"field":"t","op":"eq","value":"y"}],"window":"1m","threshold":1,"severity":"high"},
	  {"id":"a","match":[{"field":"t","op":"eq","value":"x"}],"window":"1m","threshold":1}
	]}`)
	ra, _ := a.ByID("a")
	rb, _ := b.ByID("a")
	if ra.Severity != rb.Severity || ra.GroupBy != rb.GroupBy {
		t.Fatalf("defaults differ across load order: %+v vs %+v", ra, rb)
	}
	if ra.Severity != DefaultSeverity || ra.GroupBy != DefaultGroupBy {
		t.Fatalf("unexpected defaults: %+v", ra)
	}
	t.Logf("defaults stable: severity=%q group_by=%q", ra.Severity, ra.GroupBy)
}

func TestLoadV2DeprecatedFieldMapping(t *testing.T) {
	rs := mustLoad(t, `{"version":2,"rules":[
	  {"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"1m","threshold":1,"field":"user"}
	]}`)
	r, _ := rs.ByID("r")
	if r.GroupBy != "user" {
		t.Fatalf("deprecated field not mapped: group_by=%q", r.GroupBy)
	}
	t.Logf("deprecated 'field' mapped to group_by=%q", r.GroupBy)
}

func TestLoadV2DeprecatedFieldConflictRejected(t *testing.T) {
	mustFailKind(t, `{"version":2,"rules":[
	  {"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"1m","threshold":1,"field":"user","group_by":"host"}
	]}`, ErrDeprecatedField)
}

func TestLoadUnknownField(t *testing.T) {
	mustFailKind(t, `{"version":2,"rules":[
	  {"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"1m","threshold":1,"mystery":1}
	]}`, ErrUnknownField)
	mustFailKind(t, `{"version":2,"rules":[],"unknown_top":true}`, ErrUnknownField)
}

func TestLoadBadVersion(t *testing.T) {
	mustFailKind(t, `{"version":99,"rules":[{"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"1m","threshold":1}]}`, ErrBadVersion)
	mustFailKind(t, `{"rules":[{"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"1m","threshold":1}]}`, ErrBadVersion)
}

func TestLoadInvalidRef(t *testing.T) {
	// 重复 ID
	mustFailKind(t, `{"version":1,"rules":[
	  {"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"1m","threshold":1},
	  {"id":"r","match":[{"field":"t","op":"eq","value":"y"}],"window":"1m","threshold":1}
	]}`, ErrInvalidRef)
	// 空 ID
	mustFailKind(t, `{"version":1,"rules":[
	  {"id":"","match":[{"field":"t","op":"eq","value":"x"}],"window":"1m","threshold":1}
	]}`, ErrInvalidRef)
}

func TestLoadIncomplete(t *testing.T) {
	// 空 match
	mustFailKind(t, `{"version":1,"rules":[{"id":"r","match":[],"window":"1m","threshold":1}]}`, ErrIncomplete)
	// 非法 op
	mustFailKind(t, `{"version":1,"rules":[{"id":"r","match":[{"field":"t","op":"regex","value":"x"}],"window":"1m","threshold":1}]}`, ErrIncomplete)
	// 条件缺 field
	mustFailKind(t, `{"version":1,"rules":[{"id":"r","match":[{"op":"eq","value":"x"}],"window":"1m","threshold":1}]}`, ErrIncomplete)
	// 非法 window
	mustFailKind(t, `{"version":1,"rules":[{"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"abc","threshold":1}]}`, ErrIncomplete)
	// 非法 threshold
	mustFailKind(t, `{"version":1,"rules":[{"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"1m","threshold":0}]}`, ErrIncomplete)
	// 空规则集
	mustFailKind(t, `{"version":1,"rules":[]}`, ErrIncomplete)
}
