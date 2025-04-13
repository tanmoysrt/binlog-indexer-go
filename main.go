package main

func main() {
	indexer, err := NewBinlogIndexer(
		"/home/tanmoy/Desktop/binlog-parser-4",
		"/home/tanmoy/Desktop/binlog-parser-3/mysql-bin.000307",
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
