package engine

import "errors"

// 异步输入管线的可区分错误。
var (
	// ErrClosed 引擎已关闭，不再接收新事件。
	ErrClosed = errors.New("engine closed")
	// ErrBufferFull 输入缓冲已满，事件未被接收（TrySubmit 专用，调用方可重试或降级）。
	ErrBufferFull = errors.New("input buffer full")
	// ErrPipelineDisabled 未配置 Workers，异步管线未启用，请使用同步 Process。
	ErrPipelineDisabled = errors.New("async pipeline disabled: Config.Workers <= 0")
)

// Submit 异步提交一条事件：缓冲未满时立即入队；缓冲满时阻塞等待（背压），
// 直到有 worker 腾出位置或引擎关闭。返回 ErrClosed 表示事件未被接收。
// 凡返回 nil 的事件，保证在 Close 返回前被处理完。
func (e *Engine) Submit(ev Event) error {
	e.pMu.RLock()
	defer e.pMu.RUnlock()
	if e.in == nil {
		return ErrPipelineDisabled
	}
	if e.pClosed {
		return ErrClosed
	}
	e.in <- ev // 缓冲满时在此阻塞，形成可预期的背压
	return nil
}

// TrySubmit 非阻塞提交：缓冲满时立即返回 ErrBufferFull，明确区分“没被接收”，
// 绝不悄悄丢弃；引擎关闭返回 ErrClosed。
func (e *Engine) TrySubmit(ev Event) error {
	e.pMu.RLock()
	defer e.pMu.RUnlock()
	if e.in == nil {
		return ErrPipelineDisabled
	}
	if e.pClosed {
		return ErrClosed
	}
	select {
	case e.in <- ev:
		return nil
	default:
		return ErrBufferFull
	}
}

// Close 关闭引擎：停止接收新事件，排空缓冲——所有已被接收的事件
// 处理完毕后本方法才返回，不因关闭丢结果。幂等，可重复调用。
func (e *Engine) Close() {
	e.pMu.Lock()
	if e.in == nil || e.pClosed {
		e.pMu.Unlock()
		return
	}
	e.pClosed = true
	close(e.in) // 在写锁保护下关闭，保证没有提交者正在发送
	e.pMu.Unlock()
	e.wg.Wait() // 等待 worker 排空缓冲并处理完所有已接收事件
}

// startPipeline 启动有界缓冲与 worker 池。仅 New 在 Workers > 0 时调用。
func (e *Engine) startPipeline() {
	e.in = make(chan Event, e.cfg.BufferSize)
	for i := 0; i < e.cfg.Workers; i++ {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			for ev := range e.in {
				e.Process(ev)
			}
		}()
	}
}
