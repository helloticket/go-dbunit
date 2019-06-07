package dbunit

import (
	"errors"
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type FlatYmlDataSet struct {
	Folder string
}

func (f *FlatYmlDataSet) Load(fixtureName string) ([]Record, error) {
	file := f.Folder + "/" + fixtureName

	content, err := ioutil.ReadFile(file)
	if err != nil {
		return []Record{Record{}}, err
	}

	var raw interface{}
	if err := yaml.Unmarshal(content, &raw); err != nil {
		return []Record{Record{}}, err
	}

	records := []Record{}

	switch rawRecords := raw.(type) {
	case []interface{}:
		for _, r := range rawRecords {
			recordMap, ok := r.(map[interface{}]interface{})
			if !ok {
				return []Record{}, errors.New("Wrong cast []interface{}")
			}

			records = append(records, f.mapper(file, recordMap))
		}

	case map[interface{}]interface{}:
		for _, record := range rawRecords {
			recordMap, ok := record.(map[interface{}]interface{})
			if !ok {
				return []Record{}, errors.New("Wrong cast map[interface{}]interface{}")
			}

			records = append(records, f.mapper(file, recordMap))
		}

	default:
		return []Record{}, errors.New("Records not defined to file '" + file + "'")
	}

	return records, nil
}

func (f *FlatYmlDataSet) mapper(file string, raw map[interface{}]interface{}) Record {
	columns := []string{}
	values := map[string]interface{}{}
	for k, v := range raw {
		columns = append(columns, fmt.Sprintf("%s", k))

		switch v.(type) {
		case []interface{}, map[interface{}]interface{}:
			values[fmt.Sprintf("%s", k)] = recursiveToJSON(v)

		default:
			values[fmt.Sprintf("%s", k)] = v
		}
	}

	return Record{
		fileName: file,
		columns:  columns,
		values:   values,
	}
}
