package dbunit

import (
	"testing"
)

func TestPostgresFixtureYmlToDeleteOperation(t *testing.T) {
	dbFactory := NewPostgresDatabaseFactory(globalDriver, globalDataSource)
	defer dbFactory.Close()

	dataSet := NewFlatYmlDataSet("testdata/fixtures")

	globalConnect.MustExec("insert into peoples values (1, 'John', '2a20b784-fa0e-452e-b36e-d582f2fdbf99')")
	globalConnect.MustExec("insert into peoples values (2, 'Mary', '1a20b784-fa0e-452e-b36e-d582f2fdbf91')")
	globalConnect.MustExec("insert into peoples values (3, 'Mikey', '3a20b784-fa0e-452e-b36e-d582f2fdbf91')")

	Delete(dbFactory, dataSet).ExecuteWith("peoples", "peoples.yml")

	assertCount(t, 0, "select count(*) FROM peoples")
}

func TestPostgresFixtureYmlToDeleteAllOperationByPeople(t *testing.T) {
	dbFactory := NewPostgresDatabaseFactory(globalDriver, globalDataSource)
	defer dbFactory.Close()

	dataSet := NewFlatYmlDataSet("testdata/fixtures")

	globalConnect.MustExec("insert into peoples values (1, 'John', '2a20b784-fa0e-452e-b36e-d582f2fdbf99')")
	globalConnect.MustExec("insert into peoples values (2, 'Mary', '1a20b784-fa0e-452e-b36e-d582f2fdbf91')")
	globalConnect.MustExec("insert into peoples values (3, 'Mikey', '3a20b784-fa0e-452e-b36e-d582f2fdbf91')")
	globalConnect.MustExec("insert into peoples values (500, 'Not defined peoples.yml', '12121')")

	DeleteAll(dbFactory, dataSet).ExecuteWith("peoples", "peoples.yml")

	assertCount(t, 0, "select count(*) FROM peoples")
}

func TestPostgresFixtureYmlToDeleteOperationByTags(t *testing.T) {
	dbFactory := NewPostgresDatabaseFactory(globalDriver, globalDataSource)
	defer dbFactory.Close()

	dataSet := NewFlatYmlDataSet("testdata/fixtures")

	globalConnect.MustExec("insert into tags (id, name, created_at, updated_at)  values (1, 'Go', '2016-01-01 12:30:12', '2016-01-01 12:30:12')")
	globalConnect.MustExec("insert into tags (id, name, created_at, updated_at)  values (2, 'Ruby', '2016-01-01 12:30:12', '2016-01-01 12:30:12')")
	globalConnect.MustExec("insert into tags (id, name, created_at, updated_at)  values (3, 'Java', '2016-01-01 12:30:12', '2016-01-01 12:30:12')")

	Delete(dbFactory, dataSet).ExecuteWith("tags", "tags.yml")

	assertCount(t, 0, "select count(*) FROM tags")
}

func TestPostgresFixtureYmlToDeleteOperationByUsers(t *testing.T) {
	dbFactory := NewPostgresDatabaseFactory(globalDriver, globalDataSource)
	defer dbFactory.Close()

	dataSet := NewFlatYmlDataSet("testdata/fixtures")

	DeleteAll(dbFactory, dataSet).ExecuteWith("users", "users.yml")

	assertCount(t, 0, "select count(*) FROM users")
}

func TestPostgresFixtureYmlToInsertOperationByUsers(t *testing.T) {
	dbFactory := NewPostgresDatabaseFactory(globalDriver, globalDataSource)
	defer dbFactory.Close()

	dataSet := NewFlatYmlDataSet("testdata/fixtures")

	Insert(dbFactory, dataSet).ExecuteWith("users", "users.yml")

	assertCount(t, 2, "select count(*) FROM users")
}

func TestPostgresFixtureYmlToComposeOperationByUsers(t *testing.T) {
	dbFactory := NewPostgresDatabaseFactory(globalDriver, globalDataSource)
	defer dbFactory.Close()

	dataSet := NewFlatYmlDataSet("testdata/fixtures")

	globalConnect.MustExec("insert into tags (id, name, created_at, updated_at)  values (1, 'Go', '2016-01-01 12:30:12', '2016-01-01 12:30:12')")

	DeleteAndInsert(dbFactory, dataSet).ExecuteWith("tags", "tags.yml")

	assertCount(t, 2, "select count(*) FROM users")
}

func TestPostgresFixtureYmlToSuiteOperations(t *testing.T) {
	dbFactory := NewPostgresDatabaseFactory(globalDriver, globalDataSource)
	defer dbFactory.Close()

	dataSet := NewFlatYmlDataSet("testdata/fixtures")

	DeleteAndInsert(dbFactory, dataSet).Execute("posts.yml")
	DeleteAndInsert(dbFactory, dataSet).Execute("tags.yml")
	DeleteAndInsert(dbFactory, dataSet).ExecuteWith("posts_tags", "posts_and_tags.yml")
	DeleteAndInsert(dbFactory, dataSet).Execute("comments.yml")
	DeleteAndInsert(dbFactory, dataSet).Execute("users.yml")

	assertCount(t, 2, "select count(*) FROM posts")
	assertCount(t, 3, "select count(*) FROM tags")
	assertCount(t, 2, "select count(*) FROM posts_tags")
	assertCount(t, 4, "select count(*) FROM comments")
	assertCount(t, 2, "select count(*) FROM users")
}

func TestPostgresFixtureYmlWithFilter(t *testing.T) {
	dbFactory := NewPostgresDatabaseFactory(globalDriver, globalDataSource)
	defer dbFactory.Close()

	dataSet := NewFlatYmlDataSet("testdata/fixtures")

	DeleteAndInsert(dbFactory, dataSet).ExecuteWithFilter("comments", "comments.yml", func(record Record) bool {
		return record.Values()["content"] == "Post 1 comment 2" || record.Values()["content"] == "Post 2 comment 2"
	})

	assertCount(t, 2, "select count(*) FROM comments")
}
