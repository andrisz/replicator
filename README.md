```
Zabbix database object replicator.

This utility can export an object with its related data and replicate
the exported data set number of times.

Build:
Depending on required database type set the mysql, postgres or oracle build 
tag, for example:
go build  -o . -tags mysql  ./...
go build  -o . -tags postgres  ./...
go build  -o . -tags oracle  ./...

The relations between database objects are described by schema in json format:
{
  "<table>": {
    "<field1>": "<definition1>",
    "<field2>": "<definition2>",
    ...
  },
  ...
}

Where:
    <table> - the table name
    <fieldN> - the field name
    <definitionN> the field definition:
        <table>:<field> - references an object in the specified table. 
			Reference to itself is treated as object identifier
        <table>::<field> - weekly references an object in the specified table.
			Used for circular references.
        TriggerExpressionField - trigger expression or recovery expression 
			field. References listed functions.
        $:<pattern> - for every cloned object the cloning iteration index will
			be either embedded into pattern replacing {?} with 
			index) or appended to it. If pattern is not specified \
			the index will be embedded/appended to the original 
			field value

Object reference to itself will autoincrement the field starting with 
max value + 1.
Object reference to another table will result in cloning the referred object.
Fields without definition will be copied as is.

Ready to use example schemas can be found in schema/ subfolder.

Usage examples:

Export host with hostid 10255 and related objects to host.json file:

./replicator -u zb -p 2b -d zbtest -s schema.json -o hosts:hostid -e -f host.json -id 10255

Import 10 copies of the exported host object set:

./replicator -u zb -p 2b -d zbtest -s schema.json -i -f host.json -n 10

```
