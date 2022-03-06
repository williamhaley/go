package processing

import (
	"encoding/csv"
	"io"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

func pipeline3Read(ctx *ProcessingContext, destinationDirectory string, deleteDestinationDirectory bool, ins ...<-chan *ProcessingPayload) []<-chan *ProcessingPayload {
	return fanInOrOut(ctx, func(ctx *ProcessingContext, in <-chan *ProcessingPayload) <-chan *ProcessingPayload {
		return readRowsFromCSV(ctx, destinationDirectory, deleteDestinationDirectory, in)
	}, ins...)
}

func readRowsFromCSV(ctx *ProcessingContext, destinationDirectory string, deleteDestinationDirectory bool, in <-chan *ProcessingPayload) <-chan *ProcessingPayload {
	out := make(chan *ProcessingPayload)

	go func() {
		defer close(out)

		for payload := range in {
			response := payload.Value.(*extractFileResponse)

			file, err := os.Open(response.extractedFilePath)
			if err != nil {
				log.WithError(err).Fatalf("error opening extracted file %s\n", response.extractedFilePath)
			}
			log.Infof("opened file %q", response.extractedFilePath)
			csvReader := csv.NewReader(file)

			collection := response.collection

			foundHeader := false
			header := []string{}

			for {
				row, err := csvReader.Read()

				if err == io.EOF {
					break
				}

				if err != nil {
					// TODO WFH What to do for these?
					log.WithError(err).Errorf("error reading CSV row for collection %q", response.collection.Collection)
					continue
				}

				if !foundHeader {
					for i := 0; i < len(row); i++ {
						header = append(header, strings.TrimSpace(row[i]))
					}
					foundHeader = true
				} else {
					out <- &ProcessingPayload{
						Value: &csvRecord{
							header:     header,
							row:        row,
							collection: collection,
						},
					}

					ctx.RateTracker.Add()
				}
			}

			file.Close()

			if deleteDestinationDirectory {
				defer func() {
					log.Debug("removing unzip destination directory")
					if err := os.RemoveAll(destinationDirectory); err != nil {
						panic(err)
					}
				}()
			}
		}
	}()

	return out
}
