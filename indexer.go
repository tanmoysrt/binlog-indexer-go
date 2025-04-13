package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/marcboeker/go-duckdb/v2"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

const CREATE_TABLE_SQL string = `
CREATE TABLE IF NOT EXISTS query (
	binlog VARCHAR,
	db_name VARCHAR,
	table_name VARCHAR,
	timestamp INTEGER,
	type VARCHAR,
	row_id INTEGER,
	event_size INTEGER
)
`

const INSERT_QUERY_SQL string = "INSERT INTO query (binlog, db_name, table_name, timestamp, type, row_id, event_size) VALUES %s"

type Query struct {
	Timestamp uint32
	Metadata  SQLSourceMetadata
	RowId     int32
	EventSize uint32
	SQL       string
}

type BinlogIndexer struct {
	BatchSize   int
	binlogName string
	binlogPath string

	// State
	queries      []Query
	currentRowId int32

	// Internal
	db         *sql.DB
	fw         source.ParquetFile
	pw         *writer.ParquetWriter
	parser     *replication.BinlogParser
	sqlParser *sqlparser.Parser
	isClosed  bool
}

type ParquetRow struct {
	Id    int32  `parquet:"name=id, type=INT32"`
	Query string `parquet:"name=query, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
}

func NewBinlogIndexer(base_path string, binlog_path string, database_filename string) (*BinlogIndexer, error) {
	if _, err := os.Stat(base_path); os.IsNotExist(err) {
		return nil, fmt.Errorf("base path does not exist: %w", err)
	}
	db, err := sql.Open("duckdb", filepath.Join(base_path, database_filename))
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	// Create SQL sql_parser
	sql_parser, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to create sql parser: %w", err)
	}

	// Create table
	_, err = db.Exec(CREATE_TABLE_SQL)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Create parquet writer
	fw, err := local.NewLocalFileWriter(filepath.Join(base_path, fmt.Sprintf("queries_%s.parquet", filepath.Base(binlog_path))))
	if err != nil {
		return nil, fmt.Errorf("failed to create local file: %w", err)
	}

	parquetWriter, err := writer.NewParquetWriter(fw, new(ParquetRow), 4)
	parquetWriter.RowGroupSize = 50000
	parquetWriter.PageSize = 512 * 1024 // 512KB
	parquetWriter.CompressionType = parquet.CompressionCodec_ZSTD

	// Fetch Metadata

	return &BinlogIndexer{
		BatchSize:    10000,
		binlogName:  filepath.Base(binlog_path),
		binlogPath:  binlog_path,
		queries:      make([]Query, 0),
		currentRowId: 1,
		db:           db,
		fw:           fw,
		pw:           parquetWriter,
		parser:       replication.NewBinlogParser(),
		sqlParser:   sql_parser,
		isClosed:    false,
	}, nil
}

func (p *BinlogIndexer) Parse() error {
	err := p.parser.ParseFile(p.binlogPath, 0, p.onBinlogEvent)
	if err != nil {
		return fmt.Errorf("failed to parse binlog: %w", err)
	}
	// Flush the last batch
	err = p.flush()
	if err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}
	// Close everything
	p.Close()
	return nil
}

func (p *BinlogIndexer) onBinlogEvent(e *replication.BinlogEvent) error {
	switch e.Header.EventType {
	case replication.QUERY_EVENT:
		if event, ok := e.Event.(*replication.QueryEvent); ok {
			p.addQuery(string(event.Query), string(event.Schema), e.Header.Timestamp, e.Header.EventSize)
		}
	}

	return nil
}

func (p *BinlogIndexer) addQuery(query string, schema string, timestamp uint32, eventSize uint32) {
	metadata := ExtractSQLMetadata(query, p.sqlParser, string(schema))

	p.queries = append(p.queries, Query{
		Timestamp: timestamp,
		Metadata:  metadata,
		RowId:     p.currentRowId,
		EventSize: eventSize,
		SQL:       query,
	})
	p.currentRowId += 1
	if len(p.queries) >= p.BatchSize {
		err := p.flush()
		if err != nil {
			println(err.Error())
		}
	}
}

func (p *BinlogIndexer) flush() error {
	if len(p.queries) == 0 {
		return nil
	}
	// Create transaction
	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin db transaction: %w", err)
	}

	defer func() {
		// In case of error rollback changes
		_ = tx.Rollback()
	}()

	// Write the queries to db
	batch := make([]string, 0, len(p.queries))
	for _, query := range p.queries {
		for _, table := range query.Metadata.Tables {
			batch = append(batch, fmt.Sprintf("('%s', '%s', '%s', %d, '%s', %d, %d)",
				p.binlogName, table.Database, table.Table, query.Timestamp, query.Metadata.Type, query.RowId, query.EventSize))
		}
	}

	// Insert the queries
	_, err = tx.Exec(fmt.Sprintf(INSERT_QUERY_SQL, strings.Join(batch, ",")))
	if err != nil {
		return fmt.Errorf("failed to insert queries: %w", err)
	}

	// Release memory of batch
	batch = nil

	// Insert all query in parquet file
	for _, query := range p.queries {
		if err = p.pw.Write(ParquetRow{
			Id:    query.RowId,
			Query: query.SQL,
		}); err != nil {
			return fmt.Errorf("failed to write query to parquet file: %w", err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit db transaction: %w", err)
	}

	// Clear the queries
	p.queries = make([]Query, 0)

	return nil
}

func (p *BinlogIndexer) Close() {
	if p.isClosed {
		return
	}
	// Do a final flush
	if err := p.flush(); err != nil {
		fmt.Printf("[WARN] failed to flush: %v\n", err)
	}
	// try to stop the parquet writer
	if err := p.pw.WriteStop(); err != nil {
		fmt.Printf("[WARN] failed to stop parquet writer: %v\n", err)
	}

	// try to close the parquet file
	if err := p.fw.Close(); err != nil {
		fmt.Printf("[WARN] failed to close parquet file: %v\n", err)
	}
	// try to close the db
	if err := p.db.Close(); err != nil {
		fmt.Printf("[WARN] failed to close db: %v\n", err)
	}

	p.isClosed = true
}
