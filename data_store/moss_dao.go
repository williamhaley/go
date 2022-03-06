package data_store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/couchbase/moss"
)

type MossDao struct {
	store      *moss.Store
	collection moss.Collection
}

func NewMossDao(persistenceDirectory string) (*MossDao, error) {
	store, collection, err := moss.OpenStoreCollection(persistenceDirectory, moss.DefaultStoreOptions, moss.StorePersistOptions{})
	if err != nil {
		return nil, err
	}

	return &MossDao{
		store:      store,
		collection: collection,
	}, nil
}

func (d *MossDao) SetRecordsBatch(recordsBatch []struct {
	Key    string
	Record interface{}
}) error {
	batch, err := d.collection.NewBatch(0, 0)
	if err != nil {
		return err
	}
	defer batch.Close()

	for _, writeRequest := range recordsBatch {
		jsonBytes, err := json.Marshal(writeRequest.Record)
		if err != nil {
			return err
		}

		if err := batch.Set([]byte(writeRequest.Key), jsonBytes); err != nil {
			return err
		}
	}

	return d.collection.ExecuteBatch(batch, moss.WriteOptions{})
}

func (m *MossDao) GetRecords(prefix string, destination interface{}) error {
	if reflect.TypeOf(destination).Kind() != reflect.Ptr {
		return fmt.Errorf("getRecords called with non-pointer destination")
	}
	if reflect.Indirect(reflect.ValueOf(destination)).Kind() != reflect.Slice {
		return fmt.Errorf("getRecords called with non-slice pointer destination")
	}

	recordType := reflect.TypeOf(destination).Elem().Elem()
	valueOfDestination := reflect.ValueOf(destination)
	destinationElem := valueOfDestination.Elem()

	ss, err := m.collection.Snapshot()
	if err != nil {
		return err
	}
	defer ss.Close()

	iterator, err := ss.StartIterator([]byte(prefix), nil, moss.IteratorOptions{})
	if err != nil {
		return err
	}

	var iteratorError error
	for iteratorError == nil {
		var key, value []byte
		key, value, iteratorError = iterator.Current()

		if !strings.HasPrefix(string(key), prefix) {
			iteratorError = moss.ErrIteratorDone
			continue
		}

		var reader = bytes.NewBuffer(value)
		var record = reflect.New(recordType).Interface()
		if err := json.NewDecoder(reader).Decode(record); err != nil {
			return err
		}

		destinationElem.Set(
			reflect.Append(
				destinationElem,
				reflect.Indirect(reflect.ValueOf(record)).Convert(recordType),
			),
		)

		iteratorError = iterator.Next()
	}

	return nil
}

func (m *MossDao) GetRecord(key string, destination interface{}) error {
	if reflect.TypeOf(destination).Kind() != reflect.Ptr {
		return fmt.Errorf("non-pointer destination")
	}

	ss, err := m.collection.Snapshot()
	if err != nil {
		return err
	}
	defer ss.Close()

	value, err := ss.Get([]byte(key), moss.ReadOptions{})
	if err != nil {
		return err
	}

	if value == nil {
		return fmt.Errorf("not found")
	}

	var reader = bytes.NewBuffer(value)
	if err := json.NewDecoder(reader).Decode(&destination); err != nil {
		return err
	}

	return nil
}

func (d *MossDao) Count(key string) (int, error) {
	snapshot, err := d.collection.Snapshot()
	if err != nil {
		return 0, err
	}
	defer snapshot.Close()

	iterator, err := snapshot.StartIterator(nil, nil, moss.IteratorOptions{})
	if err != nil {
		return 0, err
	}

	count := 0

	for {
		currentKey, _, err := iterator.Current()
		if err != nil && err != moss.ErrIteratorDone {
			return 0, err
		}
		if !strings.HasPrefix(string(currentKey), string(key)) {
			break
		}

		count++

		if err := iterator.Next(); err != nil && err != moss.ErrIteratorDone {
			return 0, err
		}
	}

	return count, nil
}

type IteratePayload struct {
	Record interface{}
	Key    []byte
}

func (d *MossDao) Iterate(key string) (<-chan IteratePayload, error) {
	out := make(chan IteratePayload)

	// TODO WFH Don't swallow errors
	go func() {
		defer func() {
			close(out)
		}()

		snapshot, err := d.collection.Snapshot()
		if err != nil {
			return
		}
		defer snapshot.Close()

		iterator, err := snapshot.StartIterator([]byte(key), nil, moss.IteratorOptions{})
		if err != nil {
			return
		}

		for {
			currentKey, value, err := iterator.Current()
			if err != nil && err != moss.ErrIteratorDone {
				break
			}
			if !strings.HasPrefix(string(currentKey), key) {
				break
			}

			var record interface{}
			if err := json.Unmarshal(value, &record); err != nil {
				break
			}

			out <- struct {
				Record interface{}
				Key    []byte
			}{
				record,
				currentKey,
			}

			if err := iterator.Next(); err != nil && err != moss.ErrIteratorDone {
				break
			}
		}
	}()

	return out, nil
}

// https://github.com/couchbase/moss/issues/52
func (d *MossDao) waitToPersist() {
	hadToWait := false
	start := time.Now()

	for {
		if stats, err := d.collection.Stats(); err == nil && stats.CurDirtyOps <= 0 {
			break
		}
		hadToWait = true
		time.Sleep(1 * time.Millisecond)
	}

	if hadToWait {
		fmt.Printf("moss waited %v to persist pending data", time.Since(start))
	}
}

func (d *MossDao) Close() error {
	d.waitToPersist()

	// "Both the store and collection should be closed by the caller when done."
	if err := d.collection.Close(); err != nil {
		return err
	}
	if err := d.store.Close(); err != nil {
		return err
	}
	return nil
}
