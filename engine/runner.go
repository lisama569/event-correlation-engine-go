package engine

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/lisama569/event-correlation-engine-go/rules"
)

// 背压策略：缓冲打满时如何反馈"处理不过来"。
type BackpressureMode int

const (
	// BackpressureBlock 缓冲满时 Submit 等待空位（关闭时以 ErrNotRunning 明确拒绝）。
	BackpressureBlock BackpressureMode = iota
	// BackpressureReject 缓冲满时立即返回 ErrQueueFull，调用方可明确区分"事件未被接收"。
	BackpressureReject
)

var (
	// ErrQueueFull 有界缓冲已满（Reject 模式）：事件没有被接收，调用方可安全重试或转存。
	ErrQueueFull = errors.New("engine input buffer full: event not accepted")
	// ErrNotRunning 引擎已关闭（或尚未启动）：事件没有被接收。
	ErrNotRunning = errors.New("engine is not running: event not accepted")
)

// RunnerConfig 并发处理层配置。
type RunnerConfig struct {
	BufferSize   int              // 有界输入缓冲容量；<=0 取默认 1024
	Workers      int              // 并发处理 worker 数；<=0 取默认 1
	Backpressure BackpressureMode // 缓冲满时的反馈方式
}

// RunnerStats 接收/处理计数，用于验证"接收的都处理完了"。
type RunnerStats struct {
	Accepted  int64 // 已成功进入有界缓冲（即被接收）的事件数
	Processed int64 // 已完成判定的事件数
}

type job struct {
	ev      Event
	rs      *rules.RuleSet
	matched []matchedRule
}

// Runner 在 Engine 之上提供有界输入缓冲、可配置并发与优雅关闭。
type Runner struct {
	engine *Engine
	cfg    RunnerConfig

	queue chan job
	done  chan struct{} // 关闭信号
	wg    sync.WaitGroup

	submitMu sync.Mutex // 保护 closed 与 inflight 的 Add 临界区
	closed   bool
	inflight sync.WaitGroup // 已通过关闭检查、尚未完成入队定论的提交

	accepted  atomic.Int64
	processed atomic.Int64

	// beforeApply 在 worker 真正判定前调用（包内测试钩子，用于制造可预期的处理延迟）。
	beforeApply func(Event)
}

// NewRunner 创建并发处理层（尚未启动，需调用 Start）。
func NewRunner(engine *Engine, cfg RunnerConfig) *Runner {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1024
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	return &Runner{
		engine: engine,
		cfg:    cfg,
		queue:  make(chan job, cfg.BufferSize),
		done:   make(chan struct{}),
	}
}

// Start 启动后台 worker。
func (r *Runner) Start() {
	for i := 0; i < r.cfg.Workers; i++ {
		r.wg.Add(1)
		go r.worker()
	}
}

// Submit 提交一条事件。
//   - Block 模式：缓冲满则等待空位；引擎关闭期间返回 ErrNotRunning。
//   - Reject 模式：缓冲满立即返回 ErrQueueFull；关闭期间返回 ErrNotRunning。
//
// 只要返回 nil，事件即被"接收"，Close 返回前它一定被处理完。
func (r *Runner) Submit(ev Event) error {
	// 锁外完成纯匹配计算，不占提交临界区。
	rs, matched := r.engine.Match(ev)
	j := job{ev: ev, rs: rs, matched: matched}

	r.submitMu.Lock()
	if r.closed {
		r.submitMu.Unlock()
		return ErrNotRunning
	}
	r.inflight.Add(1)
	r.submitMu.Unlock()
	defer r.inflight.Done()

	if r.cfg.Backpressure == BackpressureReject {
		select {
		case r.queue <- j:
		case <-r.done:
			return ErrNotRunning
		default:
			return ErrQueueFull // 明确反馈：未被接收
		}
	} else {
		select {
		case r.queue <- j:
		case <-r.done:
			return ErrNotRunning
		}
	}
	r.accepted.Add(1)
	return nil
}

// TrySubmit 总是非阻塞提交：等价于 Reject 模式的 Submit。
func (r *Runner) TrySubmit(ev Event) error {
	rs, matched := r.engine.Match(ev)
	j := job{ev: ev, rs: rs, matched: matched}

	r.submitMu.Lock()
	if r.closed {
		r.submitMu.Unlock()
		return ErrNotRunning
	}
	r.inflight.Add(1)
	r.submitMu.Unlock()
	defer r.inflight.Done()

	select {
	case r.queue <- j:
		r.accepted.Add(1)
		return nil
	case <-r.done:
		return ErrNotRunning
	default:
		return ErrQueueFull
	}
}

// worker 从有界缓冲取任务判定；收到关闭信号后，先等所有在途提交定论，
// 再把缓冲中已接收的事件全部排空，保证"接收即处理完"。
func (r *Runner) worker() {
	defer r.wg.Done()
	for {
		select {
		case j := <-r.queue:
			r.apply(j)
		case <-r.done:
			// 此后不再有新提交能入队；等在途提交者全部定论（入队或被拒绝）。
			r.inflight.Wait()
			// 排空已接收事件：多个 worker 并发抢读，直到各自看到空缓冲。
			for {
				select {
				case j := <-r.queue:
					r.apply(j)
				default:
					return
				}
			}
		}
	}
}

func (r *Runner) apply(j job) {
	if r.beforeApply != nil {
		r.beforeApply(j.ev)
	}
	r.engine.Apply(j.ev, j.rs, j.matched)
	r.processed.Add(1)
}

// Close 停止接收新事件并排空：所有此前已被接收（成功入队）的事件都会处理完才返回。
// 幂等，可重复调用。
func (r *Runner) Close() {
	r.submitMu.Lock()
	if !r.closed {
		r.closed = true
		close(r.done) // 唤醒阻塞在满缓冲上的 Submit，使其明确得到 ErrNotRunning
	}
	r.submitMu.Unlock()
	r.wg.Wait()
}

// Stats 返回接收/处理计数快照。
func (r *Runner) Stats() RunnerStats {
	return RunnerStats{Accepted: r.accepted.Load(), Processed: r.processed.Load()}
}
