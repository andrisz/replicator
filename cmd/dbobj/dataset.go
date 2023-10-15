package main

type ExtTable struct {
	Columns []string    `json:"columns"`
	Rows    [][]*string `json:"rows"`
}

type Dataset struct {
	tables map[string]*Table
}

func NewDataset() *Dataset {
	ds := Dataset{
		tables: make(map[string]*Table),
	}

	return &ds
}
