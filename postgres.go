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
	for _, c := range cmds {
		if _, err := p.db.NamedExec(c.sql, c.record.Values()); err != nil {
			log.Fatal("file:", c.record.fileName, " error:", err, " sql:", c.sql, " values:", c.record.values)
		}
	}
}

func (p *PostgresDatabaseFactory) Close() error {
	return p.db.Close()
}

func (p *PostgresDatabaseFactory) DB() *sql.DB {
	return p.db.DB
}
