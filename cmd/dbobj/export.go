package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Ref struct {
	srcField string
	dstTable string
	dstField string
}

func sortNumbers(data []string) ([]string, error) {
	var lastErr error
	sort.Slice(data, func(i, j int) bool {
		a, err := strconv.ParseInt(data[i], 10, 64)
		if err != nil {
			lastErr = err
			return false
		}
		b, err := strconv.ParseInt(data[j], 10, 64)
		if err != nil {
			lastErr = err
			return false
		}
		return a < b
	})
	return data, lastErr
}

func (ds *Dataset) getLinkedObjects(db *sql.DB, schema Schema, refs map[string][]*Ref) error {
	for newData := true; newData; {
		newData = false
		for name, table := range ds.tables {
			tableRefs, ok := refs[name]
			if !ok {
				continue
			}

			for _, ref := range tableRefs {
				ids := make([]string, 0)

				index := table.colmap[ref.srcField]

				for _, row := range table.rows {
					if !row.scanned {
						if row.fields[index].Value() != nil {
							ids = append(ids, *row.fields[index].Value())
						}
					}
				}

				if len(ids) > 0 {
					sortedIds, err := sortNumbers(ids)
					if err != nil {
						return err
					}
					num, err := ds.getObjects(db, schema, ref.dstTable, ref.dstField, sortedIds)
					if err != nil {
						return err
					}
					if num > 0 {
						newData = true
					}
				}
			}

			for _, row := range table.rows {
				row.scanned = true
			}

			if newData {
				break
			}
		}
	}

	return nil
}

func (ds *Dataset) getObjects(db *sql.DB, schema Schema, tableName string, fieldName string, ids []string) (
	int,
	error,
) {

	tableSchema, ok := schema[tableName]
	if !ok {
		return 0, fmt.Errorf("Unknown table '%s'", tableName)
	}

	rows, err := db.Query(fmt.Sprintf("select * from %s where %s in (%s)", tableName, fieldName, strings.Join(ids, ",")))
	if err != nil {
		return 0, err
	}

	table, ok := ds.tables[tableName]
	if !ok {
		cols, err := rows.Columns()
		if err != nil {
			return 0, err
		}
		table = NewTable(tableName, tableSchema, cols)
	}

	rowsNum := 0
	for rows.Next() {
		row := make([]*string, len(table.cols))
		values := make([]any, len(table.cols))
		for i := range values {
			values[i] = &row[i]
		}
		err = rows.Scan(values...)
		if err != nil {
			return 0, err
		}
		table.append(row)
		rowsNum++
	}

	if len(table.rows) > 0 {
		ds.tables[tableName] = table
	}

	return rowsNum, nil
}

func (ds *Dataset) Export(filename string) error {
	data := make(map[string]ExtTable)

	for name, table := range ds.tables {
		extTable := ExtTable{
			Columns: table.cols,
			Rows:    make([][]*string, 0),
		}

		for _, row := range table.rows {
			extRow := make([]*string, len(table.cols))

			for i, f := range row.fields {
				extRow[i] = f.Value()
			}

			extTable.Rows = append(extTable.Rows, extRow)
		}

		data[name] = extTable
	}

	b, err := json.Marshal(data)
	if err != nil {
		return err
	}

	err = os.WriteFile(filename, b, 0644)
	if err != nil {
		return err
	}

	return nil
}

func NewDatasetFromDB(db *sql.DB, schema Schema, ref string, ids []string) (*Dataset, error) {
	refs := make(map[string][]*Ref)
	ds := NewDataset()

	for table, fields := range schema {
		for field, value := range fields {
			d := strings.Split(value, ":")
			if len(d) >= 2 && (d[0] != table || d[len(d)-1] != field) && len(d[0]) > 1 {
				// create bidirectional links
				if _, ok := refs[d[0]]; !ok {
					refs[d[0]] = make([]*Ref, 0)
				}
				refs[d[0]] = append(refs[d[0]], &Ref{srcField: d[len(d)-1], dstTable: table, dstField: field})

				if _, ok := refs[table]; !ok {
					refs[table] = make([]*Ref, 0)
				}
				refs[table] = append(refs[table], &Ref{srcField: field, dstTable: d[0], dstField: d[len(d)-1]})
			}
		}

		r := strings.Split(ref, ":")
		_, err := ds.getObjects(db, schema, r[0], r[1], ids)
		if err != nil {
			return nil, err
		}

		err = ds.getLinkedObjects(db, schema, refs)
		if err != nil {
			return nil, err
		}

		// TODO: initialize fields ?
	}

	return ds, nil
}
