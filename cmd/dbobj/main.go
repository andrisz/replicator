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

	ds, err := GetDataset(db, schema, "triggers:triggerid", []string{"23336"})
	if err != nil {
		panic(fmt.Sprintf("Cannot read dataset: %s", err))
	}

	fmt.Printf("DATASET: %+v\n", *ds)

	fmt.Printf("TABLE: %+v\n", ds.tables["triggers"])

	for i, v := range ds.tables["triggers"].rows[0] {
		fmt.Printf("%s: %+v (%T)\n", ds.tables["triggers"].cols[i], v, v)

	}
}
