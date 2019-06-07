package dbunit

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

var globalConnect *sqlx.DB
var schemas = map[string]string{"postgres": "testdata/schemas/postgresql.sql"}

func TestMain(m *testing.M) {
	ds := "user=dbunit password=dbunit00 dbname=dbunit host=localhost port=5454 sslmode=disable"
	if os.Getenv("DBUNIT_DS") != "" {
		ds = os.Getenv("DBUNIT_DS")
	}

	driver := "postgres"
	if os.Getenv("DBUNIT_DRIVER") != "" {
		ds = os.Getenv("DBUNIT_DRIVER")
	}

	db, err := sqlx.Connect(driver, ds)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v\n", err)
	} else {
		globalConnect = db
	}

	content, err := ioutil.ReadFile(schemas[driver])
	if err != nil {
		log.Fatal(err)
	}

	if _, err := globalConnect.Exec(string(content)); err != nil {
		log.Fatal(err)
	}

	os.Exit(m.Run())

	defer globalConnect.Close()
}

func assertCount(t *testing.T, expected int, sql string) {
	var count int
	err := globalConnect.Get(&count, sql)

	assert.Nil(t, err)
	assert.Equal(t, expected, count)
}
