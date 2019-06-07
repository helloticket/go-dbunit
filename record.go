package dbunit

import "fmt"

type Command struct {
	record Record
	sql    string
}

type Record struct {
	fileName string
	values   map[string]interface{}
	columns  []string
}

func (r *Record) FileName() string {
	return r.fileName
}

func (r *Record) Columns() []string {
	return r.columns
}

func (r *Record) ColumnsByValues() []string {
	cols := []string{}

	for _, c := range r.columns {
		cols = append(cols, fmt.Sprintf("%s = :%s", c, c))
	}

	return cols
}

func (r *Record) ColumnsByAlias() []string {
	cols := []string{}

	for _, c := range r.columns {
		cols = append(cols, fmt.Sprintf(":%s", c))
	}

	return cols
}

func (r *Record) Values() map[string]interface{} {
	return r.values
}
