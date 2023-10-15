package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

type Row struct {
	scanned bool
	fields  []FieldAccessor
}

type Table struct {
	name   string
	schema map[string]string
	cols   []string
	colmap map[string]int
	rows   []*Row
	index  map[uint64]bool
}

func NewTable(name string, schema map[string]string, cols []string) *Table {
	t := Table{
		name:   name,
		schema: schema,
		cols:   cols,
		colmap: make(map[string]int),
		rows:   make([]*Row, 0),
		index:  make(map[uint64]bool),
	}

	for i, col := range cols {
		t.colmap[col] = i
	}

	return &t
}

func (t *Table) append(row []*string) {
	id, err := strconv.ParseUint(*row[0], 10, 64)
	if err != nil {
		return
	}
	if _, ok := t.index[id]; ok {
		return
	}
	t.index[id] = true

	fields := make([]FieldAccessor, len(row))

	for i, v := range row {

		if def, ok := t.schema[t.cols[i]]; ok {
			switch def {
			case "TriggerExpressionField":
				fields[i] = &TriggerExpressionField{Field: Field{value: v}}
			case "$":
				fields[i] = &IterField{Field: Field{value: v}, pattern: v}
			default:
				ref := strings.Split(def, ":")
				if ref[0] == "$" {
					fields[i] = &IterField{Field: Field{value: v}, pattern: &ref[1]}
				} else if ref[0] == t.name && ref[len(ref)-1] == t.cols[i] {
					fields[i] = &AutoincField{Field: Field{value: v}}
				} else {
					fields[i] = &RefField{Field: Field{value: v}, srcTable: ref[0], srcField: ref[len(ref)-1]}
				}
			}
		} else {
			fields[i] = &Field{value: v}
		}
	}

	t.rows = append(t.rows, &Row{scanned: false, fields: fields})
}

func (t *Table) flush(db *sql.DB) error {

	fmt.Printf("TABLE: %s\n", t.name)

	for _, row := range t.rows {
		fmt.Printf("    ")
		for _, col := range row.fields {
			v := col.Value()
			if v == nil {
				s := "null"
				v = &s
			}
			fmt.Printf("%s, ", *v)
		}
		fmt.Printf("\n")
	}
	fmt.Printf("\n")

	return nil
}
