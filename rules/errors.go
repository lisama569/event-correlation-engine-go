package rules

// ErrorKind 区分不同类型的加载错误，便于调用方分别处理。
type ErrorKind string

const (
	ErrUnsupportedVersion  ErrorKind = "unsupported_version"
	ErrUnknownField        ErrorKind = "unknown_field"
	ErrInvalidReference    ErrorKind = "invalid_reference"
	ErrIncompleteCondition ErrorKind = "incomplete_condition"
	ErrDeprecatedField     ErrorKind = "deprecated_field"
	ErrInvalidRule         ErrorKind = "invalid_rule"
	ErrMalformed           ErrorKind = "malformed"
)

// LoadError 为可区分的规则加载错误。
type LoadError struct {
	Kind   ErrorKind
	RuleID string
	Field  string
	Msg    string
}

func (e *LoadError) Error() string { return e.Msg }
