package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/andrisz/dbutil"
)

var (
	schemaPath string
	dbHost     string
	dbName     string
	dbUser     string
	dbPassword string
	modeExport bool
	modeImport bool
	objectRef  string
	objectId   uint64
	objectNum  int
	dataFile   string
)

func cmdlineError(msg string) {
	fmt.Printf("%s\n\n", msg)
	flag.Usage()
	os.Exit(1)
}

func main() {

	flag.StringVar(&schemaPath, "s", "", "path to schema file")
	flag.StringVar(&dbHost, "h", "localhost", "database server <address>[:port]")
	flag.StringVar(&dbName, "d", "", "database name")
	flag.StringVar(&dbUser, "u", "", "database user")
	flag.StringVar(&dbPassword, "p", "", "database password")
	flag.StringVar(&objectRef, "o", "", "object reference <table>:<field>")
	flag.StringVar(&dataFile, "f", "", "path to export/import file")
	flag.BoolVar(&modeExport, "e", false, "export data")
	flag.BoolVar(&modeImport, "i", false, "import data")
	flag.Uint64Var(&objectId, "id", 0, "export object id")
	flag.IntVar(&objectNum, "n", 0, "number of replications")
	flag.Parse()

	fmt.Printf("Database object replicator\n\n")

	if schemaPath == "" {
		cmdlineError("Schema is not set")
	}
	if dbName == "" {
		cmdlineError("Database name is not set")
	}
	if dbUser == "" {
		cmdlineError("Database user is not set")
	}
	if dbPassword == "" {
		cmdlineError("Database password is not set")
	}
	if dataFile == "" {
		cmdlineError("Data file path is not set")
	}
	if modeExport {
		if modeImport {
			cmdlineError("Cannot set export and import modes at the same time")
		}
		if objectRef == "" {
			cmdlineError("Object reference is not set")
		}
		if objectId == 0 {
			cmdlineError("Export object id is not set")
		}
	} else if modeImport {
		if objectNum == 0 {
			cmdlineError("Number of replications is not set")
		}
	} else {
		cmdlineError("Either export or import mode must be set")
	}

	schema, err := loadSchema(schemaPath)
	if err != nil {
		panic(fmt.Sprintf("Cannot load schema '%s': %s", schemaPath, err))
	}

	db, err := dbutil.Connect(dbHost, dbUser, dbPassword, dbName)
	if err != nil {
		panic(fmt.Sprintf("Cannot connect to database: %s", err))
	}
	defer db.Close()

	if modeExport {
		ds, err := NewDatasetFromDB(db, schema, objectRef, []string{fmt.Sprintf("%d", objectId)})
		if err != nil {
			panic(fmt.Sprintf("Cannot read dataset: %s", err))
		}
		err = ds.Export(dataFile)
		if err != nil {
			panic(fmt.Sprintf("Cannot export dataset: %s", err))
		}
		fmt.Printf("Data successfully exported to file %s\n", dataFile)

		return
	}

	if modeImport {
		ds, err := NewDatasetFromFile(dataFile, schema)
		if err != nil {
			panic(fmt.Sprintf("Cannot read dataset: %s", err))
		}

		err = ds.importObjects(db, objectNum)
		if err != nil {
			panic(fmt.Sprintf("Cannot import dataset: %s", err))
		}
		fmt.Printf("Data imported successfully\n")

		return
	}
}
