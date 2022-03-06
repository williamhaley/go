package data_store

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/williamhaley/go/data_store"
	ds "github.com/williamhaley/go/data_store"
)

type DataStore struct {
	dao *ds.PostgresDao
}

type Dao interface {
	SetRecord(collectionName, id string, record interface{}) error
	AddBatch(batchRecords []struct {
		Key    string
		Record interface{}
	}) error
	AddForOther(collectionName, forCollection, id string, record *Record) error
	GetRecord(collectionName, id string) (*Record, error)
	GetForOther(collectionName, forOther, key string) ([]*Record, error)
	Iterate(collectionName, startKey string) (<-chan data_store.IteratePayload, error)
	Count(collectionName string) (int, error)
	Close() error
}

type CursorResult struct {
	Id   string
	Data map[string]interface{}
}

type Record map[string]interface{}

func (r Record) Set(key string, value interface{}) {
	r[key] = value
}

func (r Record) Get(key string) interface{} {
	return r[key]
}

func RecordKey(collectionName, id string) string {
	return fmt.Sprintf("%s:%s", collectionName, id)
}

func New(persistenceDirectory string) *DataStore {
	postgresDao, err := ds.NewPostgresDao("postgres_user", "us-gov-data-import")
	if err != nil {
		log.WithError(err).Fatal("error creating data store")
	}

	return &DataStore{
		dao: postgresDao,
	}
}

func (d *DataStore) SetRecord(collectionName, id string, record interface{}) error {
	return d.dao.SetRecord(RecordKey(collectionName, id), record)
}

func (d *DataStore) AddBatch(batchRecords []struct {
	Key    string
	Record interface{}
}) error {
	return nil
	// return d.dao.SetRecords(batchRecords)
}

func (d *DataStore) AddForOther(collectionName, forCollection, id string, record *Record) error {
	key := fmt.Sprintf("%s:for-%s-%s-", collectionName, forCollection, id)
	return d.dao.SetRecord(key, record)
}

func (d *DataStore) GetRecord(collectionName, id string) (*Record, error) {
	var record Record
	if err := d.dao.GetRecord(RecordKey(collectionName, id), &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (d *DataStore) GetForOther(collectionName, forOther, id string) ([]*Record, error) {
	key := fmt.Sprintf("%s:for-%s-%s-", collectionName, forOther, id)
	var records []*Record
	if err := d.dao.GetRecords(key, &records); err != nil {
		return nil, err
	}
	return records, nil
}

func (d *DataStore) Iterate(key string) (<-chan data_store.IteratePayload, error) {
	// return d.dao.Iterate(key)
	return nil, nil
}

func (d *DataStore) Count(collectionName string) (int, error) {
	return d.dao.Count(collectionName)
}

func (d *DataStore) Close() error {
	return d.dao.Close()
}
