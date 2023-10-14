package main

import (
	"database/sql"
	"fmt"
	"strings"
)

type Ref struct {
	srcField string
	dstTable string
	dstField string
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

func GetDataset(db *sql.DB, schema Schema, ref string, ids []string) (*Dataset, error) {
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
	}

	return ds, nil
}
