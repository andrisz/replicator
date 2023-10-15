package main

import (
	"fmt"

	"github.com/andrisz/dbutil"
)

func main() {

	schemaPath := "schemas/schema_trigger.json"

	schema, err := loadSchema(schemaPath)
	if err != nil {
		panic(fmt.Sprintf("Cannot load schema '%s': %s", schemaPath, err))
	}

	db, err := dbutil.Connect("localhost", "zb", "2b", "zbmaster")
	if err != nil {
		panic(fmt.Sprintf("Cannot connect to database: %s", err))
	}
	defer db.Close()

	/*
		ds, err := NewDatasetFromDB(db, schema, "triggers:triggerid", []string{"23364"})
		if err != nil {
			panic(fmt.Sprintf("Cannot read dataset: %s", err))
		}
	*/

	ds, err := NewDatasetFromFile("test.json", schema)
	if err != nil {
		panic(fmt.Sprintf("Cannot read dataset: %s", err))
	}

	for name, table := range ds.tables {
		fmt.Printf("TABLE: %s\n", name)

		for _, row := range table.rows {
			fmt.Printf("    ")
			for _, col := range row.fields {
				fmt.Printf("%s, ", col)
			}
			fmt.Printf("\n")
		}
		fmt.Printf("\n")
	}

	err = ds.importObjects(db, 2)
	if err != nil {
		panic(fmt.Sprintf("Cannot import dataset: %s", err))
	}
	/*
		err = ds.Export("test.json")
		if err != nil {
			panic(fmt.Sprintf("Cannot export dataset: %s", err))
		}
	*/
}
