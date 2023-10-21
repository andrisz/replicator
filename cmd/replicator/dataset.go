package main

import "fmt"

type ExtTable struct {
	Columns []string    `json:"columns"`
	Rows    [][]*string `json:"rows"`
}

type Dataset struct {
	tables    map[string]*Table
	importNum int
	jobIndex  int
}

func NewDataset() *Dataset {
	ds := Dataset{
		tables: make(map[string]*Table),
	}

	return &ds
}

func (ds *Dataset) Dump() {
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
}
