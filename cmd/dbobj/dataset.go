package main

type Dataset struct {
	tables map[string]*Table
}

func NewDataset() *Dataset {
	ds := Dataset{
		tables: make(map[string]*Table),
	}

	return &ds
}
