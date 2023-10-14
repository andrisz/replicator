package main

import (
	"encoding/json"
	"os"
)

type Schema map[string]map[string]string

func loadSchema(filename string) (Schema, error) {
	schema := make(map[string]map[string]string)

	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(b, &schema)
	if err != nil {
		return nil, err
	}

	return schema, nil
}
