package workerpool

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const aiGuard = 5 * time.Second

func aiDrain(t *testing.T, jobs <-chan Job, numWorkers int, timeout time.Duration) []Result {
	t.Helper()
	collected := make(chan []Result, 1)
	go func() {
		var out []Result
		for r := range RunPool(jobs, numWorkers, timeout) {
			out = append(out, r)
		}
		collected <- out
	}()
	select {
	case out := <-collected:
		return out
	case <-time.After(aiGuard):
		t.Fatalf("RunPool не закрив канал результатів за %v — дедлок або завислий воркер", aiGuard)
		return nil
	}
}

func aiCooperative(d time.Duration) func(ctx context.Context) (int, error) {
	return func(ctx context.Context) (int, error) {
		select {
		case <-time.After(d):
			return 4321, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func aiUnruly(d time.Duration) func(ctx context.Context) (int, error) {
	return func(ctx context.Context) (int, error) {
		time.Sleep(d)
		return 999, nil
	}
}

func aiIndex(t *testing.T, id string) int {
	t.Helper()
	n, err := strconv.Atoi(strings.TrimPrefix(id, "job-"))
	if err != nil {
		t.Fatalf("неочікуваний JobID %q", id)
	}
	return n
}

func TestRunPool_AI_TimeoutReportsDeadlineExceeded(t *testing.T) {
	jobs := make(chan Job, 2)
	jobs <- Job{ID: "job-0", Fetch: aiCooperative(1500 * time.Millisecond)}
	jobs <- Job{ID: "job-1", Fetch: aiCooperative(10 * time.Millisecond)}
	close(jobs)

	start := time.Now()
	got := aiDrain(t, jobs, 3, 200*time.Millisecond)
	elapsed := time.Since(start)

	if len(got) != 2 {
		t.Fatalf("отримано %d результатів, очікувалось 2", len(got))
	}

	byID := make(map[string]Result, len(got))
	for _, r := range got {
		byID[r.JobID] = r
	}

	if slow := byID["job-0"]; !errors.Is(slow.Err, context.DeadlineExceeded) {
		t.Errorf("job-0.Err = %v, want context.DeadlineExceeded", slow.Err)
	} else if slow.Size != 0 {
		t.Errorf("job-0.Size = %d, want 0 при тайм-ауті", slow.Size)
	}

	if fast := byID["job-1"]; fast.Err != nil {
		t.Errorf("job-1.Err = %v, want nil", fast.Err)
	} else if fast.Size != 4321 {
		t.Errorf("job-1.Size = %d, want 4321", fast.Size)
	}

	if elapsed > time.Second {
		t.Errorf("пул тривав %v, хоч тайм-аут усього 200ms — повільне завдання не перервано", elapsed)
	}
}

func TestRunPool_AI_FetchActuallyGetsDeadline(t *testing.T) {
	const timeout = 250 * time.Millisecond

	var (
		mu        sync.Mutex
		untils    []time.Duration
		undeadled bool
	)

	fetch := func(ctx context.Context) (int, error) {
		deadline, ok := ctx.Deadline()
		mu.Lock()
		if !ok {
			undeadled = true
		} else {
			untils = append(untils, time.Until(deadline))
		}
		mu.Unlock()
		<-ctx.Done()
		return 0, ctx.Err()
	}

	jobs := make(chan Job, 3)
	for i := range 3 {
		jobs <- Job{ID: fmt.Sprintf("job-%d", i), Fetch: fetch}
	}
	close(jobs)

	got := aiDrain(t, jobs, 3, timeout)
	if len(got) != 3 {
		t.Fatalf("отримано %d результатів, очікувалось 3", len(got))
	}

	mu.Lock()
	missing := undeadled
	samples := make([]time.Duration, len(untils))
	copy(samples, untils)
	mu.Unlock()

	if missing {
		t.Fatal("job.Fetch викликано з контекстом без дедлайна — тайм-аут не застосовано")
	}
	if len(samples) != 3 {
		t.Fatalf("записано %d значень дедлайна, очікувалось 3", len(samples))
	}
	for i, until := range samples {
		if until <= 0 || until > timeout {
			t.Errorf("job-%d: до дедлайна %v, мало бути в межах (0, %v]", i, until, timeout)
		}
	}
}

func TestRunPool_AI_TimeoutDoesNotLeakGoroutines(t *testing.T) {
	const jobs = 8

	before := runtime.NumGoroutine()

	ch := make(chan Job, jobs)
	for i := range jobs {
		d := 20 * time.Millisecond
		if i == 0 {
			d = 5 * time.Second
		}
		ch <- Job{ID: fmt.Sprintf("job-%d", i), Fetch: aiCooperative(d)}
	}
	close(ch)

	if got := aiDrain(t, ch, 3, 150*time.Millisecond); len(got) != jobs {
		t.Fatalf("отримано %d результатів, очікувалось %d", len(got), jobs)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		leaked := runtime.NumGoroutine() - before
		if leaked <= 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("після RunPool залишилося %d горутин — витік", leaked)
			return
		}
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunPool_AI_UnrulyFetchDoesNotStallPool(t *testing.T) {
	ch := make(chan Job, 2)
	ch <- Job{ID: "job-0", Fetch: aiUnruly(400 * time.Millisecond)}
	ch <- Job{ID: "job-1", Fetch: aiUnruly(400 * time.Millisecond)}
	close(ch)

	start := time.Now()
	got := aiDrain(t, ch, 1, 100*time.Millisecond)
	elapsed := time.Since(start)

	if len(got) != 2 {
		t.Fatalf("отримано %d результатів, очікувалось 2", len(got))
	}
	for _, r := range got {
		if !errors.Is(r.Err, context.DeadlineExceeded) {
			t.Errorf("%s: Err = %v, want context.DeadlineExceeded", r.JobID, r.Err)
		}
	}
	if elapsed > time.Second {
		t.Errorf("пул тривав %v через воркер, що зневажає ctx, — має близько 2 тайм-аути (200ms)", elapsed)
	}
}

func TestRunPool_AI_RunsJobsInParallel(t *testing.T) {
	const (
		numWorkers = 3
		numJobs    = 6
		perJob     = 150 * time.Millisecond
	)

	var (
		mu      sync.Mutex
		current int
		peak    int
	)

	track := func(ctx context.Context) (int, error) {
		mu.Lock()
		current++
		if current > peak {
			peak = current
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			current--
			mu.Unlock()
		}()

		select {
		case <-time.After(perJob):
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	ch := make(chan Job, numJobs)
	for i := range numJobs {
		ch <- Job{ID: fmt.Sprintf("job-%d", i), Fetch: track}
	}
	close(ch)

	start := time.Now()
	got := aiDrain(t, ch, numWorkers, 2*time.Second)
	elapsed := time.Since(start)

	if len(got) != numJobs {
		t.Fatalf("отримано %d результатів, очікувалось %d", len(got), numJobs)
	}
	for _, r := range got {
		if r.Err != nil {
			t.Errorf("%s: неочікувана помилка: %v", r.JobID, r.Err)
		}
	}
	if peak > numWorkers {
		t.Errorf("пік одночасних воркерів = %d, має бути <= %d", peak, numWorkers)
	}
	if peak != numWorkers {
		t.Errorf("пік одночасних воркерів = %d, очікувалось %d", peak, numWorkers)
	}

	serial := numJobs * perJob
	if elapsed >= serial {
		t.Errorf("пул тривав %v — це послідовне виконання, а не %d воркерів", elapsed, numWorkers)
	}
}

func TestRunPool_AI_EveryJobReportedExactlyOnce(t *testing.T) {
	const numJobs = 25

	ch := make(chan Job, numJobs)
	for i := range numJobs {
		size := 100 + i
		ch <- Job{
			ID:    fmt.Sprintf("job-%d", i),
			Fetch: func(context.Context) (int, error) { return size, nil },
		}
	}
	close(ch)

	seen := make(map[string]int, numJobs)
	for _, r := range aiDrain(t, ch, 3, time.Second) {
		seen[r.JobID]++
		if r.Err != nil {
			t.Errorf("%s: неочікувана помилка: %v", r.JobID, r.Err)
		}
		if want := 100 + aiIndex(t, r.JobID); r.Size != want {
			t.Errorf("%s: Size = %d, want %d — схоже, замикання чіпляло змінну циклу", r.JobID, r.Size, want)
		}
	}

	if len(seen) != numJobs {
		t.Errorf("унікальних JobID = %d, очікувалось %d", len(seen), numJobs)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("%s оброблено %d рази, очікував 1", id, count)
		}
	}
}

func TestRunPool_AI_NoWorkersStillClosesResults(t *testing.T) {
	ch := make(chan Job, 2)
	ch <- Job{ID: "job-0", Fetch: aiCooperative(time.Millisecond)}
	ch <- Job{ID: "job-1", Fetch: aiCooperative(time.Millisecond)}
	close(ch)

	if got := aiDrain(t, ch, 0, time.Second); len(got) != 0 {
		t.Errorf("отримано %d результатів без воркерів, очікувалось 0", len(got))
	}
}
