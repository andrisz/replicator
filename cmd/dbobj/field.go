package main

type FieldSetter interface {
	Value() *string
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

func (f *Field) Value() *string {
	return f.value
}

type TriggerExpressionField struct {
	Field
}

type IterField struct {
	Field
}

type AutoincField struct {
	Field
}

type RefField struct {
	Field
}
