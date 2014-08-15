# bigquery

A wrapper client for BigQuery.

# Usage

```go
bq := bigquery.New(iss, pem)
bq.Add("project", "dataset", "tableA", rows)
bq.Send()
```

# License

New BSD License.
