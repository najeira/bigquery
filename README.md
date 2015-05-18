# bigquery

A client for sending rows to BigQuery.

This is under construction.

# Usage

```go
// create a writer with config
config := Config{
	Email:  "your client id"
	Pem:    your_private_key
	Project "your project"
	Dataset "your dataset"
	Table   "your table"
}
writer := bigquery.NewWriter(config)

// add rows
writer.Add("insert id", row)
...
writer.Add("insert id", row)

// wait sending
writer.Wait()
```

# License

New BSD License.
