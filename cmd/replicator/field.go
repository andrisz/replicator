package main

import (
	"database/sql"
	"fmt"
	"strings"
)

type FieldAccessor interface {
	Value() *string
	Raw() *string
	Prepare()
}

type Field struct {
	value *string
}

func (f *Field) String() string {
	if f.value == nil {
		return "null"
	}
	return *f.value
}

func (f *Field) Prepare() {
}

func (f *Field) Raw() *string {
	return f.value
}

func (f *Field) Value() *string {
	return f.Raw()
}

type TriggerExpressionField struct {
	Field
	functions  map[string]FieldAccessor
	expression string
}

func (f *TriggerExpressionField) Prepare() {
}

func (f *TriggerExpressionField) Value() *string {
	e := *f.value

	for p, f := range f.functions {
		e = strings.ReplaceAll(e, p, fmt.Sprintf("{%s}", *f.Value()))
	}

	return &e
}

type IterField struct {
	Field
	len     int
	num     int
	pattern *string
}

func (f *IterField) Prepare() {
	f.num++
}

func (f *IterField) Value() *string {
	id := fmt.Sprintf("%0*d", f.len, f.num)
	var s string
	if f.pattern != nil {
		s = strings.ReplaceAll(*f.pattern, "{?}", id)
		if s == *f.pattern {
			s += " " + id
		}
	} else {
		s = id
	}
	return &s
}

type Increment struct {
	id       uint64
	refcount int
}

func NewIncrement(db *sql.DB, table string, field string) (*Increment, error) {
	rows, err := db.Query(fmt.Sprintf("select max(%s) from %s", field, table))
	if err != nil {
		return nil, err
	}

	var id uint64
	if rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			return nil, err
		}
	}

	return &Increment{id: id, refcount: 1}, nil
}

func (i *Increment) Inc() {
	i.id++
}

func (i *Increment) Value() *string {
	v := fmt.Sprintf("%d", i.id)
	return &v
}

type AutoincField struct {
	Field
	id  *string
	inc *Increment
}

func (f *AutoincField) Prepare() {
	f.inc.Inc()
	f.id = f.inc.Value()
}

func (f *AutoincField) Value() *string {
	return f.id
}

type RefField struct {
	Field
	srcTable string
	srcField string
	source   FieldAccessor
}

func (f *RefField) Prepare() {
}

func (f *RefField) Value() *string {
	return f.source.Value()
}
