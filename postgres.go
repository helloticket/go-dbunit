package dbunit

import (
	"log"

	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
)

type PostgresDatabaseFactory struct {
	db *sqlx.DB
}

func (p *PostgresDatabaseFactory) Exec(cmds []Command) {
	tx, err := p.db.Beginx()
	if err != nil {
		log.Fatal(err)
	}

	var checkRollback error

	for _, c := range cmds {
		if _, err := tx.NamedExec(c.sql, c.record.Values()); err != nil {
			checkRollback = err
			log.Println("file:", c.record.fileName, " error:", err, " sql:", c.sql, " values:", c.record.values)
		}
	}

	if checkRollback != nil {
		tx.Rollback()
	} else {
		tx.Commit()
	}
}
