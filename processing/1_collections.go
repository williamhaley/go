package processing

import (
	"encoding/json"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
)

func pipeline1Processing(ctx *ProcessingContext, ins ...<-chan *ProcessingPayload) []<-chan *ProcessingPayload {
	return fanInOrOut(ctx, processCollections, ins...)
}

func processCollections(ctx *ProcessingContext, in <-chan *ProcessingPayload) <-chan *ProcessingPayload {
	out := make(chan *ProcessingPayload)

	go func() {
		defer close(out)

		// We only ever expect to read one record.
		payload := <-in
		collectionPath := payload.Value.(string)

		collectionJSON, err := ioutil.ReadFile(collectionPath)
		if err != nil {
			log.WithError(err).Fatal("error reading collection file")
		}
		var collection Collection
		if err := json.Unmarshal(collectionJSON, &collection); err != nil {
			log.WithError(err).Fatal("error loading collection")
		}

		out <- &ProcessingPayload{
			Value: &collection,
		}

		log.Infof("Loaded config for collection: %s", payload.Value)
	}()

	return out
}
