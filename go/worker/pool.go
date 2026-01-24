package worker

import (
	"context"
	"sync"

	"github.com/kdnetwork/code-snippet/go/utils"
)

func RunWorkerPool[T any, K comparable, V any](ctx context.Context, tasks []T, maxWorkers int, fn func(ctx context.Context, task T, store map[K]V) error) []error {
	tasksLen := len(tasks)

	if tasksLen == 0 {
		return []error{}
	}

	maxWorkers = utils.Clamp(tasksLen, 1, maxWorkers)

	tasksChan := make(chan T, tasksLen)
	errorsChan := make(chan error, tasksLen)

	var wg sync.WaitGroup

	for range maxWorkers {
		wg.Go(func() {
			store := make(map[K]V)
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-tasksChan:
					if !ok {
						return
					}
					errorsChan <- fn(ctx, task, store)
				}
			}
		})
	}

	go func() {
		defer close(tasksChan)
		for _, task := range tasks {
			select {
			case <-ctx.Done():
				return
			case tasksChan <- task:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(errorsChan)
	}()
	errs := make([]error, 0, tasksLen)
	for range tasks {
		errs = append(errs, <-errorsChan)
	}

	return errs
}
