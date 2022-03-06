package data_store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"reflect"

	bolt "go.etcd.io/bbolt"
)

type DocumentDao struct {
	store *bolt.DB
}

func NewDocumentDao(persistenceDirectory string) (*DocumentDao, error) {
	db, err := bolt.Open(path.Join(persistenceDirectory, "store.db"), 0666, nil)
	if err != nil {
		return nil, err
	}

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("default"))
		if err != nil {
			return err
		}
		return nil
	})

	return &DocumentDao{
		store: db,
	}, nil
}

func (d *DocumentDao) SetRecord(key string, record any) error {
	err := d.store.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("default"))

		jsonBytes, err := json.Marshal(record)
		if err != nil {
			return err
		}

		if err := b.Put([]byte(key), jsonBytes); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *DocumentDao) SetRecordsBatch(recordsBatch []struct {
	Key    string
	Record any
}) error {
	err := d.store.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("default"))

		for _, writeRequest := range recordsBatch {
			jsonBytes, err := json.Marshal(writeRequest.Record)
			if err != nil {
				return err
			}

			if err := b.Put([]byte(writeRequest.Key), jsonBytes); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *DocumentDao) DeleteRecord(key string) error {
	err := d.store.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("default"))

		if err := b.Delete([]byte(key)); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *DocumentDao) GetRecords(prefix string, destination any) error {
	if reflect.TypeOf(destination).Kind() != reflect.Ptr {
		return fmt.Errorf("getRecords called with non-pointer destination")
	}
	if reflect.Indirect(reflect.ValueOf(destination)).Kind() != reflect.Slice {
		return fmt.Errorf("getRecords called with non-slice pointer destination")
	}

	recordType := reflect.TypeOf(destination).Elem().Elem()
	valueOfDestination := reflect.ValueOf(destination)
	destinationElem := valueOfDestination.Elem()

	err := d.store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("default")).Cursor()

		prefix := []byte(prefix)
		for k, value := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, value = c.Next() {
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
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *DocumentDao) GetRecord(key string, destination any) error {
	if reflect.TypeOf(destination).Kind() != reflect.Ptr {
		return fmt.Errorf("non-pointer destination")
	}

	err := d.store.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("default"))
		value := b.Get([]byte(key))

		if value == nil {
			return fmt.Errorf("not found")
		}

		var reader = bytes.NewBuffer(value)
		if err := json.NewDecoder(reader).Decode(&destination); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *DocumentDao) Count(prefix string) (int, error) {
	count := 0

	err := d.store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("default")).Cursor()

		prefix := []byte(prefix)
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			count++
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return count, nil
}

type IteratorResult struct {
	cursor *bolt.Cursor
	Key    string
	Record any
}

func (r *IteratorResult) Delete() error {
	return r.cursor.Delete()
}

// Not defined as a method in order to use generics https://github.com/golang/go/issues/48793#issuecomment-1079910818
func Iterate[T any](d *DocumentDao, prefix string, handler func(result *IteratorResult) error) error {
	err := d.store.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte("default")).Cursor()

		prefix := []byte(prefix)
		for key, value := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
			record := new(T)
			if err := json.Unmarshal(value, &record); err != nil {
				return err
			}

			iteratorResult := &IteratorResult{
				cursor: cursor,
				Key:    string(key),
				Record: record,
			}

			if err := handler(iteratorResult); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *DocumentDao) Close() error {
	return d.store.Close()
}
