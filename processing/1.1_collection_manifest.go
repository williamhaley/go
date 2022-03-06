package processing

import (
	"encoding/json"
	"os"

	log "github.com/sirupsen/logrus"
)

func pipeline1_1Processing(ctx *ProcessingContext, ins ...<-chan *ProcessingPayload) []<-chan *ProcessingPayload {
	importManifestJsonPath := os.Getenv("IMPORT_MANIFEST_JSON_PATH")
	file, err := os.Open(importManifestJsonPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	var importManifest = make(map[string]*ImportManifest)
	if err := json.NewDecoder(file).Decode(&importManifest); err != nil {
		panic(err)
	}

	return fanInOrOut(ctx, func(ctx *ProcessingContext, in <-chan *ProcessingPayload) <-chan *ProcessingPayload {
		return processManifestFiles(ctx, importManifest, in)
	}, ins...)
}

func processManifestFiles(ctx *ProcessingContext, importManifest map[string]*ImportManifest, in <-chan *ProcessingPayload) <-chan *ProcessingPayload {
	out := make(chan *ProcessingPayload)

	go func() {
		defer close(out)

		// We only ever expect to read one record.
		payload := <-in
		collection := payload.Value.(*Collection)

		out <- &ProcessingPayload{
			Value: map[string]interface{}{
				"collection": collection,
				"manifest":   importManifest[collection.Collection],
			},
		}

		log.Infof("Loaded manifest for collection: %s", collection.Collection)
	}()

	return out
}
