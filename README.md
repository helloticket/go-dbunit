# go-dbunit

go-dbunit is a extension targeted at database-driven projects inspired by dbunit for java and go-testfixtures.

## Using

```bash
import "github.com/codenplaycorp/go-dbunit"

db, err := sql.Open("postgres", ds)
if err != nil {
    log.Fatalf("Failed to connect to database: %v\n", err)
}

dbFactory := dbunit.NewPostgresDatabaseFactory(db)
dataSet := dbunit.NewFlatYmlDataSet("testdata/fixtures")
dbunit.DeleteAndInsert(dbFactory, dataSet).Execute("posts.yml")
```

## Test (for only suite test go-dbunit)

```bash
docker run --rm -p 5454:5432 -e POSTGRES_USER=dbunit -e POSTGRES_PASSWORD=dbunit00 postgres:9.6
```

## Inspired

- http://dbunit.sourceforge.net
- https://github.com/go-testfixtures/testfixtures
