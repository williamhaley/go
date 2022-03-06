package stats

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const logRate = 10000

type RateTracker struct {
	in          chan struct{}
	label       string
	closeWaiter sync.WaitGroup
	format      string
}

type RateTrackerOptions struct {
	Label  string
	Format string
}

func NewRateTracker(opts *RateTrackerOptions) *RateTracker {
	tracker := &RateTracker{
		in:          make(chan struct{}),
		label:       opts.Label,
		closeWaiter: sync.WaitGroup{},
		format:      opts.Format,
	}

	if opts.Label != "" {
		tracker.label = opts.Label
		tracker.format = fmt.Sprintf("[%s] ", opts.Label) + opts.Format
	}

	tracker.start()
	return tracker
}

func (r *RateTracker) start() {
	r.closeWaiter.Add(1)

	go func() {
		lastWrite := time.Now()
		count := 0

		for range r.in {
			count += 1

			if count > 0 && count%logRate == 0 {
				rate := float64(logRate) / time.Since(lastWrite).Seconds()
				log.Debugf(r.format, count, rate)
				lastWrite = time.Now()
			}
		}

		log.Debugf("[%s] rate tracker done [total: %d]", r.label, count)

		r.closeWaiter.Done()
	}()
}

func (r *RateTracker) Add() {
	r.in <- struct{}{}
}

func (r *RateTracker) Done() {
	close(r.in)
	r.closeWaiter.Wait()
}
