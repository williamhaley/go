package uploading

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/williamhaley/us-govt-data/data_store"
)

func Upload(dataStore *data_store.DataStore) error {
	reader, err := dataStore.Iterate(data_store.RecordKey("carrier", ""))
	if err != nil {
		return err
	}

	batchSize := 200
	batch := make([]*data_store.Record, 0, batchSize)

	for payload := range reader {
		typedRecord := payload.Record.(*data_store.Record)

		batch = append(batch, typedRecord)
		if len(batch) == batchSize {
			if err := postMany(batch); err != nil {
				return err
			}
			batch = make([]*data_store.Record, 0, batchSize)
		}
	}

	if err := postMany(batch); err != nil {
		return err
	}

	return nil
}

func postMany(records []*data_store.Record) error {
	var buffer = new(bytes.Buffer)
	for _, record := range records {
		meta := fmt.Sprintf(`{ "create" : { "_index" : "carrier", "_id" : "%s" } }`, record.Get("Id"))
		buffer.Write([]byte(meta))
		buffer.WriteRune('\n')
		if err := json.NewEncoder(buffer).Encode(&record); err != nil {
			return err
		}
	}

	url := "http://localhost:9200/_bulk"

	req, err := http.NewRequest(http.MethodPost, url, buffer)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("non-200 status: %s", resp.Status)
	}

	log.Warnf("successfully posted batch of %d", len(records))

	// body, _ := ioutil.ReadAll(resp.Body)

	return nil
}

func post(record *data_store.Record) error {
	url := fmt.Sprintf("http://localhost:9200/carrier/_doc/%s", record.Get("Id"))

	var buffer = new(bytes.Buffer)
	if err := json.NewEncoder(buffer).Encode(&record); err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, buffer)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("non-200 status: %s", resp.Status)
	}

	// body, _ := ioutil.ReadAll(resp.Body)

	return nil
}
