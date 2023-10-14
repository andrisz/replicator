package main

type Field struct {
	value any
}

func (f *Field) String() string {
	s := f.value.(*string)
	if s == nil {
		return "null"
	}
	return *s
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
