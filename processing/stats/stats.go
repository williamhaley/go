package stats

import (
	log "github.com/sirupsen/logrus"
)

type StatEvent struct {
	CollectionName string
	StatType       EventType
}

type EventType string

const (
	EventTypeNew      = EventType("new")
	EventTypeError    = EventType("error")
	EventTypeNoChange = EventType("no-change")
	EventTypeObserved = EventType("observed")
)

type StatsTracker struct {
	in    chan *StatEvent
	stats map[string]map[EventType]int
}

func NewStatsTracker() *StatsTracker {
	tracker := &StatsTracker{
		in:    make(chan *StatEvent),
		stats: make(map[string]map[EventType]int),
	}
	tracker.start()
	return tracker
}

func (s *StatsTracker) Publish(collection string, statType EventType) {
	s.in <- &StatEvent{
		CollectionName: collection,
		StatType:       statType,
	}
}

func (s *StatsTracker) start() {
	go func() {
		for stat := range s.in {
			if _, ok := s.stats[stat.CollectionName]; !ok {
				s.stats[stat.CollectionName] = make(map[EventType]int)
			}

			s.stats[stat.CollectionName][stat.StatType] += 1
		}
	}()
}

func (s *StatsTracker) Print() {
	for collectionName, stats := range s.stats {
		log.Printf("%s:", collectionName)
		for statType, count := range stats {
			log.Printf("\t%s: %d", statType, count)
		}
	}
}
