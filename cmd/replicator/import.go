package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"
)

func (ds *Dataset) getSortedTables() []*Table {
	tables := make([]*Table, 0, len(ds.tables))
	for _, table := range ds.tables {
		tables = append(tables, table)
	}
loop:
	for {
		for i, ti := range tables {
			for j := i + 1; j < len(tables); j++ {
				tj := tables[j]
				for _, value := range ti.schema {
					ref := strings.Split(value, ":")
					if len(ref) != 2 || ref[0] != tj.name {
						continue
					}
					tables[i], tables[j] = tables[j], tables[i]
					continue loop
				}
			}
		}
		break loop
	}

	return tables
}

func (ds *Dataset) findField(tablename string, fieldname string, value string) (FieldAccessor, error) {
	table, ok := ds.tables[tablename]
	if !ok {
		return nil, fmt.Errorf("Cannot find reference table %s", tablename)
	}
	i, ok := table.colmap[fieldname]
	if !ok {
		return nil, fmt.Errorf("Cannot find reference column %s:%s", tablename, fieldname)
	}
	for _, row := range table.rows {
		v := row.fields[i].Raw()
		if v == nil {
			continue
		}
		if *v == value {
			return row.fields[i], nil
		}
	}

	return nil, fmt.Errorf("Cannot fied reference column %s:%s with value %s", tablename, fieldname, value)
}

func (ds *Dataset) initFields(db *sql.DB, num int) error {
	var err error
	increments := make(map[string]*Increment)

	iterlen := int(math.Log10(float64(num))) + 1
	reFunction := regexp.MustCompile(`{[0-9]+}`)

	for _, table := range ds.tables {
		for _, row := range table.rows {
			for j, f := range row.fields {
				switch v := f.(type) {
				case *AutoincField:
					tag := fmt.Sprintf("%s:%s", table.name, table.cols[j])
					if increment, ok := increments[tag]; ok {
						v.inc = increment
					} else {
						v.inc, err = NewIncrement(db, table.name, table.cols[j])
						if err != nil {
							return err
						}
						increments[tag] = v.inc
					}
				case *RefField:
					if v.value == nil {
						v.source = &Field{}
					} else {
						ref, err := ds.findField(v.srcTable, v.srcField, *v.value)
						if err != nil {
							return err
						}
						v.source = ref
					}
				case *IterField:
					v.len = iterlen
				case *TriggerExpressionField:
					v.functions = make(map[string]FieldAccessor)
					functions := reFunction.FindAllString(*v.value, -1)
					for _, f := range functions {
						field, err := ds.findField("functions", "functionid", f[1:len(f)-1])
						if err != nil {
							return err
						}
						v.functions[f] = field
					}
				}
			}
		}

	}

	return nil
}

func (ds *Dataset) importObjects(db *sql.DB, num int) error {
	tables := ds.getSortedTables()

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	err = ds.initFields(db, num)
	if err != nil {
		return err
	}

	for i := 0; i < num; i++ {
		for _, table := range tables {
			if i%1000 == 0 {
				fmt.Printf("Importing %s ...\n", table.name)
			}
			for _, row := range table.rows {
				for _, field := range row.fields {
					field.Prepare()
				}
			}
		}

		for _, table := range tables {
			err = table.flush(db)
			if err != nil {
				return err
			}
		}

		if (i+1)%1000 == 0 {
			err = tx.Commit()
			if err != nil {
				return err
			}
			tx, err = db.Begin()
			if err != nil {
				return err
			}
		}
	}

	_, err = db.Exec("delete from ids")
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

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
