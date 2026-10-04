package engine

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/event-correlation-engine-go/rules"
)

func runnerRules(t *testing.T) *rules.RuleSet {
	t.Helper()
	return loadRules(t, `{"version":2,"rules":[
	  {"id":"r","match":[{"field":"t","op":"eq","value":"x"}],"window":"1h","threshold":100}
	]}`)
}

// 缓冲打满时 Reject 模式必须以可区分的结果明确反馈"没被接收"，且不无限占内存。
func TestRunnerBackpressureReject(t *testing.T) {
	eng, _ := New(Config{AllowedLateness: time.Hour, MaxKeys: 4096}, runnerRules(t))
	r := NewRunner(eng, RunnerConfig{BufferSize: 4, Workers: 1, Backpressure: BackpressureReject})

	release := make(chan struct{})
	var entered atomic.Int64
	r.beforeApply = func(Event) {
		entered.Add(1)
		<-release // 唯一 worker 卡在第一条，后续事件只能堆在有界缓冲里
	}
	r.Start()

	// 第 1 条被 worker 取走（正在处理中卡住），先确认它已离开缓冲。
	if err := r.Submit(ev("e0", 0, map[string]string{"t": "x"})); err != nil {
		t.Fatalf("first event should be accepted, got %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for entered.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if entered.Load() != 1 {
		t.Fatalf("worker should be stuck on first event, entered=%d", entered.Load())
	}
	// 第 2..5 条填满 4 个缓冲位。
	for i := 1; i <= 4; i++ {
		if err := r.Submit(ev(fmt.Sprintf("e%d", i), time.Duration(i)*time.Second, map[string]string{"t": "x"})); err != nil {
			t.Fatalf("event %d should be accepted, got %v", i, err)
		}
	}

	// 再提交：缓冲满 + worker 忙，必须立即得到 ErrQueueFull，而不是无限等待或静默丢弃。
	err := r.Submit(ev("overflow", 6*time.Second, map[string]string{"t": "x"}))
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("overflow submit must return ErrQueueFull, got %v", err)
	}
	// TrySubmit 行为一致。
	if err := r.TrySubmit(ev("overflow2", 7*time.Second, map[string]string{"t": "x"})); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("TrySubmit must return ErrQueueFull when full, got %v", err)
	}
	t.Logf("buffer full reported distinctly as ErrQueueFull; accepted=%d", r.Stats().Accepted)

	close(release) // 放行 worker
	r.Close()      // 幂等：重复 Close 不 panic
	r.Close()

	st := r.Stats()
	// 被接收的 5 条全部处理完；被拒绝的 2 条从未被接收，因此不要求处理。
	if st.Accepted != 5 || st.Processed != 5 {
		t.Fatalf("accepted must equal processed after close: accepted=%d processed=%d", st.Accepted, st.Processed)
	}
	// 被拒绝事件确实没进入引擎（引擎侧看不到对应事件计数）：已处理事件 ID 可通过状态核对。
	t.Logf("drained cleanly: accepted=%d processed=%d", st.Accepted, st.Processed)
}

// Block 模式下缓冲打满时 Submit 等待；关闭时等待中的提交被明确拒绝（ErrNotRunning），
// 而已接收事件全部处理完才从 Close 返回，不丢结果。
func TestRunnerBlockModeDrainOnClose(t *testing.T) {
	eng, _ := New(Config{AllowedLateness: time.Hour, MaxKeys: 4096}, runnerRules(t))
	r := NewRunner(eng, RunnerConfig{BufferSize: 2, Workers: 1, Backpressure: BackpressureBlock})

	release := make(chan struct{})
	r.beforeApply = func(ev Event) {
		if ev.ID == "slow" {
			<-release
		}
	}
	r.Start()

	// slow 被 worker 取走并卡住；e1/e2 填满缓冲。
	mustSubmit := func(id string, off time.Duration) {
		t.Helper()
		if err := r.Submit(ev(id, off, map[string]string{"t": "x"})); err != nil {
			t.Fatalf("submit %s: %v", id, err)
		}
	}
	mustSubmit("slow", 0)
	mustSubmit("e1", time.Second)
	mustSubmit("e2", 2*time.Second)

	// e3 将在满缓冲上阻塞等待。
	blockedDone := make(chan error, 1)
	go func() {
		blockedDone <- r.Submit(ev("e3", 3*time.Second, map[string]string{"t": "x"}))
	}()
	time.Sleep(50 * time.Millisecond) // 让 e3 进入等待
	select {
	case <-blockedDone:
		t.Fatal("e3 should be blocked waiting for buffer space")
	default:
	}

	// 关闭引擎：等待中的 e3 必须得到明确的"未被接收"，而不是卡住或假装成功。
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.Close()
	}()
	time.Sleep(50 * time.Millisecond) // Close 发出关闭信号
	if err := <-blockedDone; !errors.Is(err, ErrNotRunning) {
		t.Fatalf("blocked submit during close must get ErrNotRunning, got %v", err)
	}
	// 关闭后再提交一律明确拒绝。
	if err := r.Submit(ev("after", 4*time.Second, map[string]string{"t": "x"})); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("submit after close must get ErrNotRunning, got %v", err)
	}

	close(release) // 放行 slow，worker 排空所有已接收事件
	wg.Wait()      // Close 在排空完成后才返回

	st := r.Stats()
	// 被接收的 slow/e1/e2 全部处理；e3 在关闭竞争中未入队，不计入接收。
	if st.Accepted != 3 || st.Processed != 3 {
		t.Fatalf("all accepted events must be processed: accepted=%d processed=%d", st.Accepted, st.Processed)
	}
	// 已处理的三条事件确实进入了引擎状态（按规则 threshold=100 不会告警，故核对无迟到/无丢失）：
	// 通过 LateEvents 与 Stats 联合确认没有事件因关闭丢失。
	if got := len(eng.LateEvents()); got != 0 {
		t.Fatalf("unexpected late events: %d", got)
	}
	t.Logf("block-mode close drained all accepted: accepted=%d processed=%d; blocked submit rejected distinctly",
		st.Accepted, st.Processed)
}

// 多 worker 持续写入 + 关闭排空：所有成功返回 nil 的提交最终都被处理，竞态检测下通过。
func TestRunnerConcurrentNoLoss(t *testing.T) {
	eng, _ := New(Config{AllowedLateness: time.Hour, MaxKeys: 1 << 16}, runnerRules(t))
	r := NewRunner(eng, RunnerConfig{BufferSize: 32, Workers: 4, Backpressure: BackpressureReject})
	r.Start()

	var accepted atomic.Int64
	var rejected atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				err := r.TrySubmit(ev(fmt.Sprintf("g%d-%d", g, i), time.Duration(i)*time.Second,
					map[string]string{"t": "x"}))
				if errors.Is(err, ErrQueueFull) || errors.Is(err, ErrNotRunning) {
					rejected.Add(1)
					continue
				}
				if err != nil {
					t.Errorf("unexpected submit error: %v", err)
					return
				}
				accepted.Add(1)
			}
		}(g)
	}
	wg.Wait()
	r.Close()

	st := r.Stats()
	if st.Accepted != st.Processed {
		t.Fatalf("accepted=%d but processed=%d after close (lost events!)", st.Accepted, st.Processed)
	}
	if st.Accepted != accepted.Load() {
		t.Fatalf("runner accepted=%d differs from caller-side accepted=%d", st.Accepted, accepted.Load())
	}
	if st.Accepted+rejected.Load() != 8*500 {
		t.Fatalf("every submit either accepted or rejected distinctly: accepted=%d rejected=%d", st.Accepted, rejected.Load())
	}
	t.Logf("concurrent writers: accepted=%d processed=%d rejected=%d (no accepted event lost)",
		st.Accepted, st.Processed, rejected.Load())
}

// 关闭排空后主动 Drain 仍能取到在途窗口的收尾结论（接收的事件结果可收回）。
func TestRunnerCloseThenDrain(t *testing.T) {
	eng, _ := New(Config{AllowedLateness: time.Hour, MaxKeys: 4096}, runnerRules(t))
	r := NewRunner(eng, RunnerConfig{BufferSize: 8, Workers: 2, Backpressure: BackpressureBlock})
	r.Start()
	for i := 0; i < 10; i++ {
		if err := r.Submit(ev(fmt.Sprintf("d%d", i), time.Duration(i)*time.Second, map[string]string{"t": "x"})); err != nil {
			t.Fatal(err)
		}
	}
	r.Close()
	if st := r.Stats(); st.Accepted != 10 || st.Processed != 10 {
		t.Fatalf("accepted/processed mismatch: %+v", st)
	}
	// 所有接收事件都处理完，其累计计数必须能通过收尾结果取回，不能因关闭而丢失。
	settled := eng.Drain()
	var totalCount int
	for _, s := range settled {
		totalCount += s.Count
	}
	if totalCount != 10 {
		t.Fatalf("drain after close must recover all accepted event counts, got %d: %+v", totalCount, settled)
	}
	t.Logf("close drained %d events; drain recovered counts=%d across %d windows", 10, totalCount, len(settled))
}
