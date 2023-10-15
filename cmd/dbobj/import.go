package main

import (
	"encoding/json"
	"fmt"
	"os"
)

func NewDatasetFromFile(filename string, schema Schema) (*Dataset, error) {
	ds := NewDataset()

	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	data := make(map[string]ExtTable)
	err = json.Unmarshal(b, &data)
	if err != nil {
		return nil, err
	}

	for name, extTable := range data {
		tableSchema, ok := schema[name]
		if !ok {
			return nil, fmt.Errorf("Unknown table '%s'", name)
		}
		table := NewTable(name, tableSchema, extTable.Columns)

		for _, extRow := range extTable.Rows {
			table.append(extRow)
		}

		ds.tables[name] = table
	}

	return ds, nil
}
