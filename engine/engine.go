// Package engine 实现本地流式事件关联检测引擎。
//
// 并发模型：固定数量的 worker goroutine 按事件类型哈希分区，
// 每个分区内的关联状态仅由所属 worker 访问，无需加锁；
// 规则集通过 atomic.Pointer 整体替换，单次判定只见一个完整版本，
// 热更新期间事件处理不中断，也不会出现新旧规则混合。
package engine

import (
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/event-correlation-engine-go/event"
	"github.com/lisama569/event-correlation-engine-go/rules"
)

// ErrClosed 表示引擎已关闭。
var ErrClosed = errors.New("engine: closed")

// ResultKind 区分引擎输出的结果类型。
type ResultKind int

const (
	ResultAlert     ResultKind = iota // 窗口闭合且达到阈值
	ResultLateEvent                   // 超出容忍范围的迟到事件（未计入任何窗口）
	ResultEvicted                     // 因内存上限被淘汰的键，其未闭合窗口被强制结算
)

// Result 为引擎的统一输出，携带判定依据以便解释。
type Result struct {
	Kind           ResultKind
	RuleID         string
	RuleSetVersion int // 命中（或结算）时使用的规则集版本
	Key            string
	Count          int
	WindowStart    time.Time
	WindowEnd      time.Time
	Event          event.Event // 仅 LateEvent 使用
	Basis          string      // 判定依据（可解释）
}

// Config 为引擎配置。
type Config struct {
	Workers         int           // worker 数（有界并发），<=0 时默认 4
	MaxKeys         int           // 每个 worker 按键维护状态的最大键数，<=0 时默认 1024
	InputBuffer     int           // 每个 worker 的输入缓冲，满时 Submit 阻塞（背压）
	AllowedLateness time.Duration // 允许的事件迟到时长
}

// Engine 为关联检测引擎。
type Engine struct {
	workers []*worker
	out     chan Result
	rules   atomic.Pointer[rules.Set]
	quit    chan struct{}
	wg      sync.WaitGroup

	mu     sync.RWMutex // 保护 Submit 与 Close 的互斥
	closed bool
}

// New 创建并启动引擎。
func New(cfg Config, rs *rules.Set) (*Engine, error) {
	if rs == nil {
		return nil, errors.New("engine: initial ruleset must not be nil")
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.MaxKeys <= 0 {
		cfg.MaxKeys = 1024
	}
	if cfg.InputBuffer <= 0 {
		cfg.InputBuffer = 256
	}
	e := &Engine{
		out:  make(chan Result, cfg.Workers*cfg.InputBuffer),
		quit: make(chan struct{}),
	}
	e.rules.Store(rs)
	e.workers = make([]*worker, cfg.Workers)
	for i := range e.workers {
		w := &worker{
			in:       make(chan event.Event, cfg.InputBuffer),
			out:      e.out,
			rules:    &e.rules,
			lateness: int64(cfg.AllowedLateness),
			store:    newStateStore(cfg.MaxKeys),
			wg:       &e.wg,
		}
		e.workers[i] = w
		e.wg.Add(1)
		go w.run()
	}
	return e, nil
}

// Submit 提交事件。输入缓冲满时阻塞直至有容量或引擎关闭（背压）。
func (e *Engine) Submit(ev event.Event) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return ErrClosed
	}
	w := e.workers[e.partition(ev)]
	select {
	case w.in <- ev:
		return nil
	case <-e.quit:
		return ErrClosed
	}
}

// partition 按事件类型哈希分区：同一事件类型（因而同一规则的全部
// 关联键）总是落到同一 worker，保证状态不被拆分。
func (e *Engine) partition(ev event.Event) int {
	h := fnv.New32a()
	h.Write([]byte(ev.Type))
	return int(h.Sum32() % uint32(len(e.workers)))
}

// Reload 解析并整体替换规则集。替换是原子的：任一事件的判定
// 只会看到替换前或替换后的某一个完整版本，不会混合。
// 解析失败时当前规则集保持不变。
func (e *Engine) Reload(data []byte) error {
	rs, err := rules.Load(data)
	if err != nil {
		return err
	}
	e.rules.Store(rs)
	return nil
}

// Results 返回输出通道，引擎关闭后该通道随之关闭。
func (e *Engine) Results() <-chan Result { return e.out }

// Close 停止接收新事件，闭合所有残留窗口后关闭输出通道。
func (e *Engine) Close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	close(e.quit) // 唤醒阻塞在 Submit 上的调用方
	e.mu.Unlock()

	// 等待所有在途 Submit 退出后再关闭 worker 输入，避免向已关闭通道发送。
	e.mu.Lock()
	for _, w := range e.workers {
		close(w.in)
	}
	e.mu.Unlock()

	e.wg.Wait()
	close(e.out)
}
