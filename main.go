// package main

// import (
// 	"fmt"
// 	"os"
// 	"time"

// 	"github.com/go-mysql-org/go-mysql/replication"
// )

// func main() {
// 	start := time.Now()
// 	defer func() {
// 		println(time.Since(start))
// 	}()

// 	// parser, err := sqlparser.New(sqlparser.Options{})
// 	// if err != nil {
// 	// 	println(err.Error())
// 	// 	return
// 	// }

// 	p := replication.NewBinlogParser()

// 	x := 0

// f := func(e *replication.BinlogEvent) error {

// 	if e.Header.EventType == replication.MARIADB_ANNOTATE_ROWS_EVENT {
// 		e.Dump(os.Stdout)
// 		// event := (e.Event).(*replication.MariadbAnnotateRowsEvent)
// 		// result := ExtractSQLMetadata(string(event.Query), parser)
// 		// print(result)

// 		x += 1
// 	} else if e.Header.EventType == replication.QUERY_EVENT {
// 		e.Dump(os.Stdout)
// 		x += 1
// 	} else if e.Header.EventType == replication.MARIADB_GTID_EVENT {
// 		e.Dump(os.Stdout)
// 		x += 1
// 	}

// 	// e.Dump(os.Stdout)
// 	return nil
// }

// 	name := "/home/tanmoy/Desktop/binlog-parser-3/mysql-bin.000300"
// 	var offset int64 = 0

// 	err := p.ParseFile(name, offset, f)
// 	if err != nil {
// 		println(err.Error())
// 	}
// 	fmt.Printf("x: %d\n", x)
// }

// package main

// import (
// 	"database/sql"
// 	"fmt"
// 	"log"
// 	"math/rand"
// 	"strings"
// 	"time"

// 	_ "github.com/marcboeker/go-duckdb/v2"
// )

// const (
// 	totalRecords = 2_00_000
// 	batchSize    = 1_000
// )

// func main() {
// 	db, err := sql.Open("duckdb", "benchmark.duckdb")
// 	if err != nil {
// 		log.Fatal("Failed to connect:", err)
// 	}
// 	defer db.Close()

// 	// Create table
// 	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
// 		id INTEGER,
// 		name TEXT
// 	);`)
// 	if err != nil {
// 		log.Fatal("Failed to create table:", err)
// 	}
// 	fmt.Println("Table ready.")

// 	start := time.Now()
// 	tx, err := db.Begin()
// 	if err != nil {
// 		log.Fatal("Failed to begin transaction:", err)
// 	}

// 	// Random generator
// 	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

// 	// Batching loop
// 	batch := make([]string, 0, batchSize)
// 	for i := 1; i <= totalRecords; i++ {
// 		id := rng.Intn(1_000_000_000)
// 		name := fmt.Sprintf("User-%d", id)
// 		batch = append(batch, fmt.Sprintf("(%d, '%s')", id, name))

// 		if len(batch) == batchSize || i == totalRecords {
// 			// Join the batch and insert
// 			query := fmt.Sprintf("INSERT INTO users (id, name) VALUES %s;", strings.Join(batch, ","))
// 			if _, err := tx.Exec(query); err != nil {
// 				log.Fatalf("Batch insert failed at record %d: %v", i, err)
// 			}
// 			fmt.Printf("Inserted %d records...\n", i)
// 			batch = batch[:0] // reset batch
// 		}
// 	}

// 	if err := tx.Commit(); err != nil {
// 		log.Fatal("Failed to commit transaction:", err)
// 	}

// 	duration := time.Since(start)
// 	fmt.Printf("Inserted %d records in %v\n", totalRecords, duration)
// }

package main

func main() {
	indexer, err := NewBinlogIndexer(
		"/home/tanmoy/Desktop/binlog-parser-4",
		"/home/tanmoy/Desktop/binlog-parser-3/mysql-bin.000300",
		"queries.db",
		10000,
	)
	if err != nil {
		println(err.Error())
		return
	}
	defer func() {
		indexer.Close()
	}()
	err = indexer.Index()
	if err != nil {
		println(err.Error())
		return
	}
}
