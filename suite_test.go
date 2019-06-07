package dbunit

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

var globalConnect *sqlx.DB
var schemas = map[string]string{"postgres": "testdata/schemas/postgresql.sql"}
var globalDataSource = "user=dbunit password=dbunit00 dbname=dbunit host=localhost port=5454 sslmode=disable"
var globalDriver = "postgres"

func TestMain(m *testing.M) {
	if os.Getenv("DBUNIT_DS") != "" {
		globalDataSource = os.Getenv("DBUNIT_DS")
	}

	if os.Getenv("DBUNIT_DRIVER") != "" {
		globalDataSource = os.Getenv("DBUNIT_DRIVER")
	}

	db, err := sqlx.Connect(globalDriver, globalDataSource)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v\n", err)
	} else {
		globalConnect = db
	}

	content, err := ioutil.ReadFile(schemas[globalDriver])
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
