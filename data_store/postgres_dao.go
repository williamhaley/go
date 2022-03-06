package data_store

import (
	"fmt"
	"log"

	"database/sql"

	_ "github.com/lib/pq"
)

type PostgresDao struct {
	db *sql.DB
}

func NewPostgresDao(user, databaseName string) (*PostgresDao, error) {
	connStr := fmt.Sprintf("user=%s dbname=%s sslmode=verify-full", user, databaseName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	db.Exec("CREATE TABLE IF NOT EXISTS records(id SERIAL PRIMARY KEY, data JSON);")

	return &PostgresDao{
		db: db,
	}, nil
}

func (d *PostgresDao) SetRecord(key string, record interface{}) error {
	d.db.Exec("INSERT INTO records (data) VALUES (%s)", record)
	return nil
}

func (d *PostgresDao) GetRecords(prefix string, destination interface{}) error {
	return nil
}

func (d *PostgresDao) GetRecord(key string, destination interface{}) error {
	return nil
}

func (d *PostgresDao) Count(key string) (int, error) {
	return 0, nil
}

func (d *PostgresDao) Close() error {
	return d.db.Close()
}
