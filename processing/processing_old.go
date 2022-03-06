package processing

import (
	"math"
	"sync"

	log "github.com/sirupsen/logrus"

	"net/http"
	_ "net/http/pprof"

	"github.com/williamhaley/us-govt-data/data_store"
	"github.com/williamhaley/us-govt-data/stats"
)

type ProcessingContext struct {
	Label           string
	RateTracker     *stats.RateTracker
	DataStore       *data_store.DataStore
	StatsTracker    *stats.StatsTracker
	ProcessorsTotal int
	Strategy        ProcessingStrategy
}

type Collection struct {
	Collection       string                 `json:"collection"`
	ComparisonFields []string               `json:"comparisonFields"`
	Mappings         map[string]interface{} `json:"mappings"`
}

type ProcessingPayload struct {
	Value interface{}
}

type extractFileResponse struct {
	collection        *Collection
	extractedFilePath string
}

type generateRecordsFromCSVResponse struct {
	collection *Collection
	record     *data_store.Record
}

type csvRecord struct {
	collection *Collection
	header     []string
	row        []string
}

type ImportManifest struct {
	SourceFiles []struct {
		Zip       string `json:"zip"`
		PathInZip string `json:"pathInZip"`
	} `json:"sourceFiles"`
}

type ProcessingStrategy string

const (
	WaitUntilAllDone ProcessingStrategy = "WaitUntilAllDone"
	SerialPerInput   ProcessingStrategy = "SerialPerInput"
)

func Process(ins []<-chan *ProcessingPayload, destinationDirectory string, deleteDestinationDirectory bool, dataStore *data_store.DataStore) {
	go http.ListenAndServe("localhost:8080", nil)

	statsPublisher := stats.NewStatsTracker()
	readerRateTracker := stats.NewRateTracker(&stats.RateTrackerOptions{Label: "reader", Format: "read %d rows (%f/s)\n"})
	generateRateTracker := stats.NewRateTracker(&stats.RateTrackerOptions{Label: "generator", Format: "generated %d records (%f/s)\n"})
	persistRateTracker := stats.NewRateTracker(&stats.RateTrackerOptions{Label: "persister", Format: "persisted %d records (%f/s)\n"})

	collectionsChannels := pipeline1Processing(&ProcessingContext{
		DataStore: dataStore,
		Label:     "Pipeline 1 (get collection data)",
	}, ins...)

	collectionManifestChannels := pipeline1_1Processing(&ProcessingContext{
		Label: "Pipeline 1.1 (get manifests)",
	}, collectionsChannels...)

	extractionJobsChannels := pipeline2Jobs(&ProcessingContext{
		Label: "Pipeline 2 (unzip jobs)",
	}, destinationDirectory, collectionManifestChannels...)

	extractedFileChannels := pipeline2_1Extract(&ProcessingContext{
		Label:    "Pipeline 2.1 (extract)",
		Strategy: WaitUntilAllDone,
	}, destinationDirectory, extractionJobsChannels...)

	csvResultsChannels := pipeline3Read(&ProcessingContext{
		Label:       "Pipeline 3 (read files)",
		RateTracker: readerRateTracker,
	}, destinationDirectory, deleteDestinationDirectory, extractedFileChannels...)

	recordsChannels := pipeline4Generate(&ProcessingContext{
		Label:       "Pipeline 4 (generate records)",
		RateTracker: generateRateTracker,
	}, csvResultsChannels...)

	// Larger batch sizes seem really fast, but can wreck memory especially when coupled with a lot of processors.
	batchSize := 2000
	doneChannels := pipeline5Persist(
		&ProcessingContext{
			Label:        "Pipeline 5 (persist records)",
			RateTracker:  persistRateTracker,
			StatsTracker: statsPublisher,
			DataStore:    dataStore,
		},
		batchSize,
		recordsChannels...,
	)

	allDoneChannel := mergeChannels(doneChannels...)
	<-allDoneChannel
	log.Warn("all done...")

	readerRateTracker.Done()
	persistRateTracker.Done()
	statsPublisher.Print()
}

func mergeChannels(cs ...<-chan *ProcessingPayload) <-chan *ProcessingPayload {
	var wg sync.WaitGroup
	out := make(chan *ProcessingPayload)

	output := func(c <-chan *ProcessingPayload) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

type ProcessorHandler func(ctx *ProcessingContext, in <-chan *ProcessingPayload) <-chan *ProcessingPayload

func fanInOrOut(ctx *ProcessingContext, handler ProcessorHandler, ins ...<-chan *ProcessingPayload) []<-chan *ProcessingPayload {
	processorsTotal := ctx.ProcessorsTotal
	if processorsTotal == 0 {
		processorsTotal = len(ins)
	}

	var outs []<-chan *ProcessingPayload
	if len(ins) > processorsTotal {
		outs = fanIn(ctx, processorsTotal, handler, ins...)
	} else {
		outs = fanOut(ctx, processorsTotal, handler, ins...)
	}

	switch ctx.Strategy {
	case WaitUntilAllDone:
		backlog := make([]*ProcessingPayload, 0)

		merged := mergeChannels(outs...)
		// This is the trick. We _block_ functionality here until we read all the
		// inputs. Use with caution! We're holding this data in memory.
		for backlogItem := range merged {
			backlog = append(backlog, backlogItem)
		}
		next := make(chan *ProcessingPayload)
		go func() {
			for _, backlogItem := range backlog {
				next <- backlogItem
			}
			close(next)
		}()

		outs = []<-chan *ProcessingPayload{next}
	}

	return outs
}

func fanOut(ctx *ProcessingContext, processorsTotal int, handler ProcessorHandler, ins ...<-chan *ProcessingPayload) []<-chan *ProcessingPayload {
	outs := make([]<-chan *ProcessingPayload, 0, processorsTotal)

	groupSize := int(math.Floor(float64(processorsTotal) / float64(len(ins))))
	remainder := processorsTotal % len(ins)

	for _, in := range ins {
		for index := 0; index < groupSize; index++ {
			out := handler(ctx, in)
			outs = append(outs, out)
		}
	}

	for index := 0; index < remainder; index++ {
		in := ins[index]
		out := handler(ctx, in)
		outs = append(outs, out)
	}

	log.Infof("fanOut %q from %d to %d", ctx.Label, len(ins), len(outs))

	return outs
}

func fanIn(ctx *ProcessingContext, processorsTotal int, handler ProcessorHandler, ins ...<-chan *ProcessingPayload) []<-chan *ProcessingPayload {
	outs := make([]<-chan *ProcessingPayload, 0, processorsTotal)

	groupSize := int(math.Floor(float64(len(ins)) / float64(processorsTotal)))
	remainder := len(ins) % processorsTotal

	for index := range outs {
		isLast := index == len(outs)-1
		sliceSize := groupSize
		if isLast {
			sliceSize = groupSize + remainder
		}
		var nextSection []<-chan *ProcessingPayload
		nextSection, ins = ins[0:sliceSize], ins[sliceSize:]
		merged := mergeChannels(nextSection...)
		out := handler(ctx, merged)
		outs[index] = out
	}

	return outs
}
