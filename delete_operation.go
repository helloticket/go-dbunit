package dbunit

import (
	"fmt"
	"log"
	"strings"
)

type DeleteOperation struct {
	dbFactory DatabaseFactory
	dataSet   DataSet
}

type DeleteAllOperation struct {
	dbFactory DatabaseFactory
	dataSet   DataSet
}

func (d *DeleteOperation) Execute(fixtureName string) {
	d.ExecuteWithFilter(extractFileName(fixtureName), fixtureName, NoFilter())
}

func (d *DeleteAllOperation) Execute(fixtureName string) {
	d.ExecuteWithFilter(extractFileName(fixtureName), fixtureName, NoFilter())
}

func (d *DeleteOperation) ExecuteWith(tableName string, fixtureName string) {
	d.ExecuteWithFilter(tableName, fixtureName, NoFilter())
}

func (d *DeleteAllOperation) ExecuteWith(tableName string, fixtureName string) {
	d.ExecuteWithFilter(tableName, fixtureName, NoFilter())
}

func (d *DeleteOperation) ExecuteWithFilter(tableName string, fixtureName string, filter Filter) {
	records, err := d.dataSet.Load(fixtureName)
	if err != nil {
		log.Fatal(err)
	}

	commands := []Command{}

	for _, r := range records {
		if !filter(r) {
			sql := fmt.Sprintf(
				"DELETE FROM %s WHERE %s ",
				tableName,
				strings.Join(r.ColumnsByValues(), " AND "),
			)

			commands = append(commands, Command{record: r, sql: sql, tableName: tableName})
		}
	}

	if len(commands) > 0 {
		d.dbFactory.Exec(commands)
	}
}

func (d *DeleteAllOperation) ExecuteWithFilter(tableName string, fixtureName string, filter Filter) {
	records, err := d.dataSet.Load(fixtureName)
	if err != nil {
		log.Fatal(err)
	}

	commands := []Command{}

	for _, r := range records {
		if !filter(r) {
			sql := fmt.Sprintf("DELETE FROM %s ", tableName)
			commands = append(commands, Command{record: r, sql: sql, tableName: tableName})
		}
	}

	if len(commands) > 0 {
		d.dbFactory.Exec(commands)
	}
}
