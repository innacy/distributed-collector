package pipeline

import (
	"context"
	"sync"
)

type ProcessFunc func(ctx context.Context, msg []byte) ([]byte, error)

type ResultFunc func(output []byte, err error)

type WorkerPool struct {
	workers    int
	input      chan []byte
	process    ProcessFunc
	onResult   ResultFunc
	wg         sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
	stopped    chan struct{}
}

func NewWorkerPool(workers, bufferSize int, process ProcessFunc) *WorkerPool {
	return &WorkerPool{
		workers: workers,
		input:   make(chan []byte, bufferSize),
		process: process,
		stopped: make(chan struct{}),
	}
}

func (wp *WorkerPool) SetResultHandler(fn ResultFunc) {
	wp.onResult = fn
}

func (wp *WorkerPool) Start(ctx context.Context) {
	wp.ctx, wp.cancelFunc = context.WithCancel(ctx)

	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
}

func (wp *WorkerPool) worker() {
	defer wp.wg.Done()
	for {
		select {
		case msg, ok := <-wp.input:
			if !ok {
				return
			}
			output, err := wp.process(wp.ctx, msg)
			if wp.onResult != nil {
				wp.onResult(output, err)
			}
		case <-wp.ctx.Done():
			return
		}
	}
}

func (wp *WorkerPool) Submit(msg []byte) bool {
	if wp.ctx.Err() != nil {
		return false
	}
	select {
	case wp.input <- msg:
		return true
	case <-wp.ctx.Done():
		return false
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.input)
	wp.wg.Wait()
	close(wp.stopped)
}

func (wp *WorkerPool) Done() <-chan struct{} {
	return wp.stopped
}
