package processing

import (
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/williamhaley/us-govt-data/data_store"
)

func pipeline4Generate(ctx *ProcessingContext, ins ...<-chan *ProcessingPayload) []<-chan *ProcessingPayload {
	return fanInOrOut(ctx, generateRecordsFromRows, ins...)
}

type handler func(record map[string]interface{}, source interface{}) (interface{}, error)

var handlers = map[string]handler{
	"normalizeFMCSADate": func(record map[string]interface{}, source interface{}) (interface{}, error) {
		original := record[source.(string)].(string)
		if original == "" {
			return nil, nil
		}
		value, err := time.Parse("02-Jan-06", original)
		if err != nil {
			return nil, err
		}
		// Don't use a true time.Time. Use a string to make sure that we are
		// serializing this consistently across data stores.
		return value.Format(time.RFC3339), nil
	},
	"normalizeFMCSABoolean": func(record map[string]interface{}, source interface{}) (interface{}, error) {
		switch record[source.(string)].(string) {
		case "N":
			return false, nil
		case "Y":
			return true, nil
		default:
			return false, nil
		}
	},
	"compoundKey": func(record map[string]interface{}, source interface{}) (interface{}, error) {
		// It's assumed all the source values are raw strings
		sources := source.([]interface{})

		values := make([]string, len(sources))

		for index, source := range sources {
			values[index] = record[source.(string)].(string)
		}

		return strings.Join(values, ":"), nil
	},
}

func generateRecordsFromRows(ctx *ProcessingContext, in <-chan *ProcessingPayload) <-chan *ProcessingPayload {
	out := make(chan *ProcessingPayload)

	go func() {
		defer close(out)

		for payload := range in {
			r := payload.Value.(*csvRecord)

			record, err := rowToRecord(r.header, r.row, r.collection.Mappings)
			if err != nil {
				log.WithError(err).Error("error processing row")
				continue
			}

			ctx.RateTracker.Add()

			out <- &ProcessingPayload{
				Value: &generateRecordsFromCSVResponse{
					collection: r.collection,
					record:     record,
				},
			}
		}
	}()

	return out
}

func rowToRecord(headers []string, row []string, mappings map[string]interface{}) (*data_store.Record, error) {
	mappedRecord := map[string]interface{}{}
	if len(row) != len(headers) {
		log.Fatal("malformed CSV data")
	}
	for i := 0; i < len(row); i++ {
		mappedRecord[headers[i]] = row[i]
	}

	output := &data_store.Record{}

	for key, source := range mappings {
		transform := ""
		switch handler := source.(type) {
		case map[string]interface{}:
			source = handler["source"]
			transform = handler["transform"].(string)
		default:
			source = handler
		}

		var value interface{}

		handler, ok := handlers[transform]
		if !ok {
			// No transform. Just assume a generic source string and value
			value = mappedRecord[source.(string)]
		} else {
			var err error
			value, err = handler(mappedRecord, source)
			if err != nil {
				return nil, err
			}
		}

		output.Set(key, value)
	}

	return output, nil
}
