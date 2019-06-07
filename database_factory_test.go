package dbunit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateDataBaseConnectionByPostgres(t *testing.T) {
	factory := NewPostgresDatabaseFactory(nil)

	assert.NotNil(t, factory)
}
