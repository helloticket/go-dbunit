package dbunit

type Filter func(record Record) bool

type Operation interface {
	Execute(fixtureName string)

	ExecuteWith(tableName string, fixtureName string)

	ExecuteWithFilter(tableName string, fixtureName string, filter Filter)
}

func NoFilter() Filter {
	return func(r Record) bool {
		return false
	}
}
