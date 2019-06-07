package dbunit

import (
	"database/sql"
	"log"

	"github.com/jmoiron/sqlx"
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

func (p *PostgresDatabaseFactory) Close() error {
	return p.db.Close()
}

func (p *PostgresDatabaseFactory) DB() *sql.DB {
	return p.db.DB
}
