package processing

import (
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/williamhaley/us-govt-data/data_store"
	"github.com/williamhaley/us-govt-data/stats"
)

func pipeline5Persist(ctx *ProcessingContext, batchSize int, ins ...<-chan *ProcessingPayload) []<-chan *ProcessingPayload {
	return fanInOrOut(ctx, func(ctx *ProcessingContext, in <-chan *ProcessingPayload) <-chan *ProcessingPayload {
		return persistRecords(ctx, batchSize, in)
	}, ins...)
}

func persistRecords(ctx *ProcessingContext, batchSize int, in <-chan *ProcessingPayload) <-chan *ProcessingPayload {
	out := make(chan *ProcessingPayload)

	go func() {
		defer func() {
			close(out)
		}()

		batch := []struct {
			Key    string
			Record interface{}
		}{}

		for payload := range in {
			csvResponse := payload.Value.(*generateRecordsFromCSVResponse)

			if false {
				log.Warn(csvResponse)
			}

			id := (*csvResponse.record)["Id"].(string)

			ctx.StatsTracker.Publish(csvResponse.collection.Collection, stats.EventTypeObserved)
			ctx.RateTracker.Add()

			// TODO WFH Blind writes are so much faster. If we really care about comparisons
			// how about Redis (or in-memory) where we just track id and the comparison field(s)?

			// existing, err := ctx.DataStore.Get(csvResponse.collection.Collection, id)
			// if err != nil {
			// 	log.WithError(err).Errorf("error checking for existing record %q:%q", csvResponse.collection.Collection, id)

			// 	ctx.StatsTracker.Publish(csvResponse.collection.Collection, stats.EventTypeError)
			// 	continue
			// }

			// if same := areEqual(existing, csvResponse.record, csvResponse.collection.ComparisonFields); same {
			// 	ctx.StatsTracker.Publish(csvResponse.collection.Collection, stats.EventTypeNoChange)
			// 	continue
			// }

			if err := ctx.DataStore.SetRecord(csvResponse.collection.Collection, id, csvResponse.record); err != nil {
				log.WithError(err).Error("error writing to DB")
				ctx.StatsTracker.Publish(csvResponse.collection.Collection, stats.EventTypeError)
			} else {
				ctx.StatsTracker.Publish(csvResponse.collection.Collection, stats.EventTypeNew)
			}

			batch = append(batch, struct {
				Key    string
				Record interface{}
			}{
				Key:    data_store.RecordKey(csvResponse.collection.Collection, id),
				Record: csvResponse.record,
			})

			if len(batch) == batchSize {
				if err := ctx.DataStore.AddBatch(batch); err != nil {
					log.WithError(err).Error("error writing batch to DB")

					for _, entry := range batch {
						collectionName := strings.Split(entry.Key, ":")[0]
						ctx.StatsTracker.Publish(collectionName, stats.EventTypeError)
					}
					continue
				} else {
					for _, entry := range batch {
						collectionName := strings.Split(entry.Key, ":")[0]
						ctx.StatsTracker.Publish(collectionName, stats.EventTypeNew)
					}
				}

				batch = []struct {
					Key    string
					Record interface{}
				}{}
			}

			switch csvResponse.collection.Collection {
			case "crash", "violation":
				if err := ctx.DataStore.AddForOther(csvResponse.collection.Collection, "carrier", (*csvResponse.record)["DOTNumber"].(string), csvResponse.record); err != nil {
					log.WithError(err).Error("error writing record to DB")
					continue
				}
			}
		}

		if len(batch) > 0 {
			if err := ctx.DataStore.AddBatch(batch); err != nil {
				log.WithError(err).Error("error writing batch to DB")

				for _, entry := range batch {
					collectionName := strings.Split(entry.Key, ":")[0]
					ctx.StatsTracker.Publish(collectionName, stats.EventTypeError)
				}
			}
		}
	}()

	return out
}

func areEqual(a, b *data_store.Record, comparisonFields []string) bool {
	if a == nil || b == nil {
		return false
	}

	for _, field := range comparisonFields {
		if (*a)[field] != (*b)[field] {
			return false
		}
	}
	return true
}
