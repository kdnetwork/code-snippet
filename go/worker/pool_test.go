package worker_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kdnetwork/code-snippet/go/worker"
)

func TestRunWorkerPool(t *testing.T) {
	t.Run("TaskCompletionAndErrorCounting", func(t *testing.T) {
		tasks := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		var executeCount int64

		errs := worker.RunWorkerPool[int, string, int](
			context.Background(),
			tasks,
			3, // 3 workers
			func(ctx context.Context, task int, store map[string]int) error {
				atomic.AddInt64(&executeCount, 1)
				if task%2 == 0 {
					return fmt.Errorf("error-on-%d", task)
				}
				return nil
			},
		)

		if int(executeCount) != len(tasks) {
			t.Errorf("Expected %d executions, got %d", len(tasks), executeCount)
		}

		errCount := 0
		for _, err := range errs {
			if err != nil {
				errCount++
			}
		}
		if errCount != 5 { // 2, 4, 6, 8, 10 should fail
			t.Errorf("Expected 5 errors, got %d", errCount)
		}
	})

	t.Run("WorkerStorePersistence", func(t *testing.T) {
		// Use 1 worker to ensure all tasks reuse the same store
		tasks := []string{"a", "b", "c"}
		errs := worker.RunWorkerPool[string, string, int](
			context.Background(),
			tasks,
			1,
			func(ctx context.Context, task string, store map[string]int) error {
				store["sum"] += 1
				// If store is persistent, the last task should see sum=3
				if task == "c" && store["sum"] != 3 {
					return errors.New("persistence-failed")
				}
				return nil
			},
		)

		for _, err := range errs {
			if err != nil {
				t.Fatalf("Store persistence check failed: %v", err)
			}
		}
	})

	t.Run("ContextCancellationMidWay", func(t *testing.T) {
		tasks := make([]int, 100)
		ctx, cancel := context.WithCancel(context.Background())

		var startedCount int64
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel() // Cancel while tasks are processing
		}()

		errs := worker.RunWorkerPool[int, string, int](
			ctx,
			tasks,
			5,
			func(ctx context.Context, task int, store map[string]int) error {
				atomic.AddInt64(&startedCount, 1)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Millisecond):
					return nil
				}
			},
		)

		// The number of collected errors should still match the number of tasks,
		// but many might be nil if the pool finished or context.Canceled if it was interrupted.
		t.Logf("Started tasks before cancel: %d, Results collected: %d", atomic.LoadInt64(&startedCount), len(errs))

		if len(errs) != len(tasks) {
			t.Errorf("WorkerPool should return results for all task slots even on cancel, expected %d, got %d", len(tasks), len(errs))
		}
	})

	t.Run("EmptyTasksHandling", func(t *testing.T) {
		errs := worker.RunWorkerPool[int, string, int](context.Background(), nil, 10, nil)
		if errs == nil || len(errs) != 0 {
			t.Errorf("Empty tasks should return an empty non-nil slice, got %v", errs)
		}
	})

	t.Run("HighWorkerCountClamp", func(t *testing.T) {
		// Test if maxWorkers > tasksLen works correctly via utils.Clamp
		tasks := []int{1, 2}
		start := time.Now()
		worker.RunWorkerPool[int, string, int](
			context.Background(),
			tasks,
			100, // Much higher than tasks
			func(ctx context.Context, task int, store map[string]int) error {
				time.Sleep(10 * time.Millisecond)
				return nil
			},
		)
		duration := time.Since(start)

		// If clamping works, it shouldn't try to spawn 100 workers for 2 tasks.
		// (Though spawning 100 is fast, this is more about logic correctness)
		t.Logf("Pool with 2 tasks and 100 maxWorkers finished in %v", duration)
	})
}
