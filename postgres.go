package dbunit

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/vulcand/predicate"
)

type PostgresDatabaseFactory struct {
	db   *sqlx.DB
	opts options
}

type dbPredicate func(tableName string, column string, db *PostgresDatabaseFactory) int64

func nextSequenceValue(val int) dbPredicate {
	return func(tableName string, column string, db *PostgresDatabaseFactory) int64 {
		for i := 0; i <= val; i++ {
			db.nextSequenceVal(tableName, column)
		}

		return int64(val)
	}
}

func (p *PostgresDatabaseFactory) Exec(cmds []Command) {
	for _, c := range cmds {
		values := map[string]interface{}{}

		for column, val := range c.record.Values() {
			if expression, ok := val.(string); ok {
				if strings.HasPrefix(expression, "FUNC=") {
					values[column] = p.parseValue(c.tableName, column, expression)
				} else {
					values[column] = val
				}
			} else {
				values[column] = val
			}
		}

		if p.opts.sql {
			fmt.Println("[DBUnit] Debug SQL")
			fmt.Println("---")
			fmt.Println(c.sql)

			if len(values) != 0 {
				args := []string{}

				for k, v := range values {
					value := strings.ReplaceAll(fmt.Sprintf("%v", v), "RAW=", "")
					args = append(args, fmt.Sprintf("%v=%v", k, value))
				}

				fmt.Println(strings.Join(args, ", "))
			}

			fmt.Println("---")
			fmt.Println()
		}

		if _, err := p.db.NamedExec(c.sql, values); err != nil {
			log.Println("file:", c.record.fileName, " error:", err, " sql:", c.sql, " values:", c.record.values)
		}
	}
}

func (p *PostgresDatabaseFactory) Close() error {
	return p.db.Close()
}

func (p *PostgresDatabaseFactory) DB() *sql.DB {
	return p.db.DB
}

func (p *PostgresDatabaseFactory) nextSequenceVal(tableName, column string) {
	_, err := p.db.Exec(fmt.Sprintf("select nextval(pg_get_serial_sequence('%s', '%s'))", tableName, column))
	if err != nil {
		log.Println(err)
	}
}

func (p *PostgresDatabaseFactory) parseValue(tableName, column, expression string) interface{} {
	parser, err := predicate.NewParser(predicate.Def{
		Functions: map[string]interface{}{
			"NextValue": nextSequenceValue,
		},
	})
	if err != nil {
		log.Println(err)
		return nil
	}

	pr, err := parser.Parse(strings.TrimPrefix(expression, "FUNC="))
	if err != nil {
		log.Println(fmt.Sprintf("Could not parse %s, %v", expression, err))
		return nil
	}

	return pr.(dbPredicate)(tableName, column, p)
}
