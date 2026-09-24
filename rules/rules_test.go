package rules

import (
	"errors"
	"testing"
)

func kindOf(err error) ErrorKind {
	var le *LoadError
	if errors.As(err, &le) {
		return le.Kind
	}
	return ""
}

func TestLoadV1DefaultsStable(t *testing.T) {
	set, err := Load([]byte(`{"version":1,"rules":[
		{"id":"b","event_type":"e2","window_seconds":30},
		{"id":"a","event_type":"e1","window_seconds":60,"threshold":3}
	]}`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if set.Version != 1 || len(set.Rules) != 2 {
		t.Fatalf("unexpected set: %+v", set)
	}
	// 规则按 ID 排序，与文件中的书写顺序无关。
	if set.Rules[0].ID != "a" || set.Rules[1].ID != "b" {
		t.Fatalf("rules not sorted by id: %+v", set.Rules)
	}
	// 默认值稳定
	if got := set.Rules[1].KeyField; got != DefaultKeyField {
		t.Fatalf("default key_field = %q, want %q", got, DefaultKeyField)
	}
	if got := set.Rules[1].Threshold; got != DefaultThreshold {
		t.Fatalf("default threshold = %d, want %d", got, DefaultThreshold)
	}
	if got := set.Rules[1].Severity; got != DefaultSeverity {
		t.Fatalf("default severity = %q, want %q", got, DefaultSeverity)
	}
	t.Logf("loaded set version=%d rules=%+v", set.Version, set.Rules)
}

func TestLoadV1DeprecatedWindowMapped(t *testing.T) {
	set, err := Load([]byte(`{"version":1,"rules":[
		{"id":"r","event_type":"e","window":120}
	]}`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if set.Rules[0].WindowSeconds != 120 {
		t.Fatalf("deprecated window not mapped: %+v", set.Rules[0])
	}
}

func TestLoadV1DeprecatedWindowConflict(t *testing.T) {
	_, err := Load([]byte(`{"version":1,"rules":[
		{"id":"r","event_type":"e","window":120,"window_seconds":60}
	]}`))
	if got := kindOf(err); got != ErrDeprecatedField {
		t.Fatalf("kind = %q, want %q (err=%v)", got, ErrDeprecatedField, err)
	}
}

func TestLoadV2RejectsDeprecatedWindow(t *testing.T) {
	_, err := Load([]byte(`{"version":2,"rules":[
		{"id":"r","event_type":"e","window":120}
	]}`))
	if got := kindOf(err); got != ErrDeprecatedField {
		t.Fatalf("kind = %q, want %q (err=%v)", got, ErrDeprecatedField, err)
	}
}

func TestLoadUnknownFieldRejected(t *testing.T) {
	for _, v := range []string{
		`{"version":1,"rules":[{"id":"r","event_type":"e","window_seconds":5,"bogus":1}]}`,
		`{"version":2,"rules":[{"id":"r","event_type":"e","window_seconds":5}],"bogus":{}}`,
	} {
		if got := kindOf(mustErr(t, v)); got != ErrUnknownField {
			t.Fatalf("kind = %q, want %q for %s", got, ErrUnknownField, v)
		}
	}
}

func TestLoadUnsupportedVersion(t *testing.T) {
	if got := kindOf(mustErr(t, `{"version":3,"rules":[]}`)); got != ErrUnsupportedVersion {
		t.Fatalf("kind = %q, want %q", got, ErrUnsupportedVersion)
	}
}

func TestLoadV2InvalidFilterReference(t *testing.T) {
	got := kindOf(mustErr(t, `{"version":2,"rules":[
		{"id":"r","event_type":"e","window_seconds":5,"filter":"nope"}
	]}`))
	if got != ErrInvalidReference {
		t.Fatalf("kind = %q, want %q", got, ErrInvalidReference)
	}
}

func TestLoadV2FilterReferenceOK(t *testing.T) {
	set, err := Load([]byte(`{"version":2,
		"filters":{"internal":[{"field":"net","equals":"10.0.0.0/8"}]},
		"rules":[{"id":"r","event_type":"e","window_seconds":5,"filter":"internal",
			"match":[{"field":"status","equals":"denied"}]}]
	}`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(set.Rules[0].Match) != 2 {
		t.Fatalf("expected filter conditions merged, got %+v", set.Rules[0].Match)
	}
	t.Logf("merged conditions: %+v", set.Rules[0].Match)
}

func TestLoadIncompleteCondition(t *testing.T) {
	for _, v := range []string{
		`{"version":1,"rules":[{"id":"r","event_type":"e","window_seconds":5,"match":[{"field":"","equals":"x"}]}]}`,
		`{"version":2,"rules":[{"id":"r","event_type":"e","window_seconds":5,"match":[{"field":"f"}]}]}`,
		`{"version":2,"filters":{"g":[{"equals":"x"}]},"rules":[{"id":"r","event_type":"e","window_seconds":5}]}`,
	} {
		if got := kindOf(mustErr(t, v)); got != ErrIncompleteCondition {
			t.Fatalf("kind = %q, want %q for %s", got, ErrIncompleteCondition, v)
		}
	}
}

func TestLoadInvalidRules(t *testing.T) {
	cases := map[string]string{
		"empty rules":   `{"version":1,"rules":[]}`,
		"missing id":    `{"version":1,"rules":[{"event_type":"e","window_seconds":5}]}`,
		"duplicate id":  `{"version":1,"rules":[{"id":"r","event_type":"e","window_seconds":5},{"id":"r","event_type":"e","window_seconds":5}]}`,
		"bad window":    `{"version":1,"rules":[{"id":"r","event_type":"e","window_seconds":-1}]}`,
		"bad threshold": `{"version":1,"rules":[{"id":"r","event_type":"e","window_seconds":5,"threshold":-2}]}`,
	}
	for name, doc := range cases {
		if got := kindOf(mustErr(t, doc)); got != ErrInvalidRule {
			t.Fatalf("%s: kind = %q, want %q", name, got, ErrInvalidRule)
		}
	}
}

func TestLoadMalformedJSON(t *testing.T) {
	if got := kindOf(mustErr(t, `{"version":1,`)); got != ErrMalformed {
		t.Fatalf("kind = %q, want %q", got, ErrMalformed)
	}
}

func mustErr(t *testing.T, doc string) error {
	t.Helper()
	_, err := Load([]byte(doc))
	if err == nil {
		t.Fatalf("expected error for %s", doc)
	}
	t.Logf("rejected as expected: %v", err)
	return err
}
