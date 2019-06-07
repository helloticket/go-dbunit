package dbunit

import "github.com/jmoiron/sqlx"

type DatabaseFactory interface {
	Exec(cmds []Command)
}

type DatabaseConfig interface {
}

type DataSet interface {
	Load(fixtureName string) ([]Record, error)
}

func NewPostgresDatabaseFactory(db *sqlx.DB) DatabaseFactory {
	return &PostgresDatabaseFactory{db: db}
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
