package worker

func RunWorkerPool[T any, K comparable, V any](tasks []*T, maxWorkers int, fn func(task *T, store map[K]V) error) []error {
	tasksLen := len(tasks)

	if tasksLen == 0 {
		return []error{}
	}

	maxWorkers = min(max(maxWorkers, 1), tasksLen)

	tasksChan := make(chan *T, tasksLen)
	errorsChan := make(chan error, tasksLen)

	for range maxWorkers {
		go func() {
			store := make(map[K]V)
			for task := range tasksChan {
				errorsChan <- fn(task, store)
			}
		}()
	}

	for _, task := range tasks {
		tasksChan <- task
	}
	close(tasksChan)

	errs := make([]error, 0, tasksLen)
	for range tasks {
		errs = append(errs, <-errorsChan)
	}

	return errs
}
