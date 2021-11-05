package dbunit

import (
	"database/sql"
	"log"

	"github.com/jmoiron/sqlx"
)

type DatabaseFactory interface {
	Exec(cmds []Command)

	Close() error

	DB() *sql.DB
}

type DataSet interface {
	Load(fixtureName string) ([]Record, error)
}

func NewPostgresDatabaseFactory(driver, ds string, opts ...Options) DatabaseFactory {
	var conn *sqlx.DB

	if driver != "" && ds != "" {
		db, err := sqlx.Connect(driver, ds)
		if err != nil {
			log.Fatalf("Failed to connect to database: %v\n", err)
			return nil
		} else {
			conn = db
		}
	}

	cfg := options{}
	for _, apply := range opts {
		apply(&cfg)
	}

	return &PostgresDatabaseFactory{db: conn, opts: cfg}
}

func NewFlatYmlDataSet(dir string) DataSet {
	return &FlatYmlDataSet{Folder: dir}
}

func Delete(dbFactory DatabaseFactory, dataSet DataSet) Operation {
	return &DeleteOperation{dbFactory: dbFactory, dataSet: dataSet}
}

func DeleteAll(dbFactory DatabaseFactory, dataSet DataSet) Operation {
	return &DeleteAllOperation{dbFactory: dbFactory, dataSet: dataSet}
}

func Insert(dbFactory DatabaseFactory, dataSet DataSet) Operation {
	return &InsertOperation{dbFactory: dbFactory, dataSet: dataSet}
}

func DeleteAndInsert(dbFactory DatabaseFactory, dataSet DataSet) Operation {
	delete := DeleteAll(dbFactory, dataSet)
	insert := Insert(dbFactory, dataSet)
	return &ComposeOperation{op1: delete, op2: insert}
}
