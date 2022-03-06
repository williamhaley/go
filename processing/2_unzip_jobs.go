package processing

import (
	"os"
	"path"

	log "github.com/sirupsen/logrus"
)

func pipeline2Jobs(ctx *ProcessingContext, destinationDirectory string, ins ...<-chan *ProcessingPayload) []<-chan *ProcessingPayload {
	return fanInOrOut(ctx, func(ctx *ProcessingContext, in <-chan *ProcessingPayload) <-chan *ProcessingPayload {
		return createJob(ctx, destinationDirectory, in)
	}, ins...)
}

func createJob(ctx *ProcessingContext, destinationDirectory string, in <-chan *ProcessingPayload) <-chan *ProcessingPayload {
	out := make(chan *ProcessingPayload)

	log.Debugf("using unzip destination directory %s\n", destinationDirectory)
	if err := os.MkdirAll(destinationDirectory, 0755); err != nil && !os.IsExist(err) {
		log.WithError(err).Fatal("error creating unzip destination directory")
	}

	go func() {
		defer close(out)

		for payload := range in {
			payloadMap := payload.Value.(map[string]interface{})

			manifest := payloadMap["manifest"].(*ImportManifest)

			for _, sourceFile := range manifest.SourceFiles {
				pathOfFileWithinZip := sourceFile.PathInZip
				pathOfZipFile := os.ExpandEnv(sourceFile.Zip)
				unzippedDestinationPath := path.Join(destinationDirectory, pathOfFileWithinZip)

				out <- &ProcessingPayload{
					Value: map[string]interface{}{
						"pathOfFileWithinZip":     pathOfFileWithinZip,
						"pathOfZipFile":           pathOfZipFile,
						"unzippedDestinationPath": unzippedDestinationPath,
						"collection":              payloadMap["collection"],
					},
				}
			}
		}
	}()

	return out
}
