# bigquery

A client for sending rows to BigQuery.

This is **NOT stable**.

# Usage

```go
// create a writer with parameters
writer := bigquery.NewWriter("your project", "your dataset", "your table")

// connect to BigQuery
err := writer.Connect("your account", pem)
if err != nil {
	// authorization failed
	return err
}

// add rows
writer.Add("insert id", row)
...
writer.Add("insert id", row)

// wait sending
writer.Wait()
```

# License

New BSD License.
