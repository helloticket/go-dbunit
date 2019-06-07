package dbunit

import (
	"fmt"
	"log"
	"strings"
)

type InsertOperation struct {
	dbFactory DatabaseFactory
	dataSet   DataSet
}

func (d *InsertOperation) Execute(fixtureName string) {
	d.ExecuteWithFilter(extractFileName(fixtureName), fixtureName, NoFilter())
}

func (d *InsertOperation) ExecuteWith(tableName string, fixtureName string) {
	d.ExecuteWithFilter(tableName, fixtureName, NoFilter())
}

func (d *InsertOperation) ExecuteWithFilter(tableName string, fixtureName string, filter Filter) {
	records, err := d.dataSet.Load(fixtureName)
	if err != nil {
		log.Fatal(err)
	}

	commands := []Command{}

	for _, r := range records {
		if !filter(r) {
			sql := fmt.Sprintf(
				"INSERT INTO %s (%s) VALUES (%s)",
				tableName,
				strings.Join(r.Columns(), ","),
				strings.Join(r.ColumnsByAlias(), ","),
			)

			commands = append(commands, Command{record: r, sql: sql})
		}
	}

	if len(commands) > 0 {
		d.dbFactory.Exec(commands)
	}
}
