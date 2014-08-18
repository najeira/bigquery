package bigquery

import (
	"code.google.com/p/goauth2/oauth/jwt"
	bq "code.google.com/p/google-api-go-client/bigquery/v2"
	"encoding/json"
	"fmt"
	//"io"
	"strings"
)

const (
	MaxRowSize             = 20 * 1024
	MaxRowsCountPerRequest = 500
	MaxRequestSize         = 1 * 1024 * 1024
	MaxRowsPerSecond       = 10000
	MaxBytesPerSecond      = 10 * 1024 * 1024
	MaxRowsCountPerCall    = MaxRowsPerSecond - MaxRowsCountPerRequest
	MaxBytesPerCall        = MaxBytesPerSecond - MaxRequestSize
)

var (
	ErrRequestFull error = fmt.Errorf("request is full")
	//logger         io.Writer
)

// A client for BigQuery.
type Client struct {
	InsertId     string
	iss          string
	pem          []byte
	service      *bq.Service
	queues       map[string]chan *insertRows
	InsertErrors chan *InsertError
}

// Creates and returns a new Client.
func New(iss string, pem []byte) *Client {
	return &Client{
		iss:    iss,
		pem:    pem,
		queues: make(map[string]chan *insertRows),
	}
}

// Adds Tabledata.InsertAll data to the client.
func (w *Client) Add(project, dataset, table string, body []byte) {
	rows := &insertRows{Project: project, Dataset: dataset, Table: table, Body: body}
	key := rows.key()
	queue, ok := w.queues[key]
	if !ok {
		//writeLog("new queue %s", key)
		queue = make(chan *insertRows, 10000)
		w.queues[key] = queue
	}
	queue <- rows
	//writeLog("new rows %s %d bytes", key, len(body))
}

// Sends added data to BigQuery.
func (w *Client) Send() (int, error) {
	sent := 0
	for key, queue := range w.queues {
		n, err := w.flushQueue(key, queue)
		sent += n
		if err != nil {
			return sent, err
		}
	}
	return sent, nil
}

func (w *Client) ensure() error {
	if w.service == nil {
		return w.connect()
	}
	return nil
}

func (w *Client) connect() error {
	scope := bq.BigqueryScope
	token := jwt.NewToken(w.iss, scope, w.pem)
	transport, err := jwt.NewTransport(token)
	if err != nil {
		//writeLog("connect error %v", err)
		return err
	}
	client := transport.Client()
	bq, err := bq.New(client)
	if err != nil {
		//writeLog("connect error %v", err)
		return err
	}
	w.service = bq
	//writeLog("connected")
	return nil
}

func (w *Client) insertAll(r *tableDataInsertAllRequest) error {
	call := w.service.Tabledata.InsertAll(r.project, r.dataset, r.table, r.request)
	resp, err := call.Do()
	if err != nil {
		//writeLog("insertAll error %v", err)
		return err
	}
	//writeLog("insertAll sent")
	if w.InsertErrors != nil {
		for _, ie := range resp.InsertErrors {
			iee := ie.Errors
			row := r.request.Rows[ie.Index]
			ret := &InsertError{Errors: iee, Row: row}
			w.InsertErrors <- ret
		}
	}
	return nil
}

func (w *Client) putRowsToRequestFromQueue(
	req *tableDataInsertAllRequest, queue chan *insertRows) error {
	for len(queue) > 0 {
		rows := <-queue
		err := w.put(req, rows)
		if err != nil {
			// put the rows to queue for retrying
			queue <- rows

			if err != ErrRequestFull {
				return err
			}
			break
		}
	}
	return nil
}

func (w *Client) flushQueue(key string, queue chan *insertRows) (int, error) {
	totalRows := 0
	totalBytes := 0

	for len(queue) > 0 {

		if totalRows >= MaxRowsCountPerCall {
			//writeLog("insertAll reached limit rows %d", totalRows)
			return totalRows, nil
		} else if totalBytes >= MaxBytesPerCall {
			//writeLog("insertAll reached limit bytes %d", totalRows)
			return totalRows, nil
		}

		// ensure connection
		err := w.ensure()
		if err != nil {
			return totalRows, err
		}

		req := newTableDataInsertAllRequest(key)
		w.putRowsToRequestFromQueue(req, queue)
		//writeLog("request has %d rows %d bytes", len(req.request.Rows), req.size)

		err = w.insertAll(req)
		if err != nil {
			// it may be HTTP error. retry.
			for _, rows := range req.rowsArray {
				queue <- rows
			}

			// does not retry now.
			break

		} else {
			totalRows = len(req.request.Rows)
			totalBytes = req.size
		}
	}

	//writeLog("insertAll %d rows %d bytes", totalRows, totalBytes)
	return totalRows, nil
}

type insertRows struct {
	Project string
	Dataset string
	Table   string
	Body    []byte
}

func (r *insertRows) key() string {
	return fmt.Sprintf("%s|%s|%s", r.Project, r.Dataset, r.Table)
}

func (r *insertRows) decode() ([]interface{}, error) {
	var v interface{}
	err := json.Unmarshal(r.Body, &v)
	if err != nil {
		//writeLog("json decode error %v", err)
		return nil, err
	}

	switch v2 := v.(type) {
	case []interface{}:
		return v2, nil
	case map[string]interface{}:
		arr := make([]interface{}, 1)
		arr[0] = v2
		return arr, nil
	}
	return nil, fmt.Errorf("json decode error %v %T", v, v)
}

type tableDataInsertAllRequest struct {
	project   string
	dataset   string
	table     string
	size      int
	rowsArray []*insertRows
	request   *bq.TableDataInsertAllRequest
}

func newTableDataInsertAllRequest(key string) *tableDataInsertAllRequest {
	parts := strings.Split(key, "|")
	if parts == nil || len(parts) != 3 {
		panic(fmt.Errorf("invalid key %s", key))
	}
	project := parts[0]
	dataset := parts[1]
	table := parts[2]
	return &tableDataInsertAllRequest{
		project:   project,
		dataset:   dataset,
		table:     table,
		size:      0,
		rowsArray: make([]*insertRows, 0),
		request: &bq.TableDataInsertAllRequest{
			Rows: make([]*bq.TableDataInsertAllRequestRows, 0),
		},
	}
}

func (c *Client) put(r *tableDataInsertAllRequest, rows *insertRows) error {
	arr, err := rows.decode()
	if err != nil {
		//writeLog("request error %v", err)
		return err
	}

	if len(r.request.Rows)+len(arr) >= MaxRowsCountPerRequest {
		//writeLog("request full count %d + %d", len(r.request.Rows), len(arr))
		return ErrRequestFull
	}

	if r.size+len(rows.Body) >= MaxRequestSize {
		//writeLog("request full size %d + %d", r.size, len(rows.Body))
		return ErrRequestFull
	}

	for _, obj := range arr {
		var iid string
		var j map[string]bq.JsonValue
		switch row := obj.(type) {
		case map[string]interface{}:
			j = make(map[string]bq.JsonValue)
			for k, v := range row {
				if c.InsertId != "" && k == c.InsertId {
					iid, _ = v.(string)
				} else {
					j[k] = bq.JsonValue(v)
				}
			}
		case map[string]bq.JsonValue:
			if c.InsertId != "" {
				iid2, ok := row[c.InsertId]
				if ok {
					iid, _ = iid2.(string)
					delete(row, c.InsertId)
				}
			}
			j = row
		default:
			//writeLog("row is invalid %v %T", obj, obj)
			return fmt.Errorf("row is invalid %v", obj)
		}
		if iid != "" {
			//writeLog("InsertId %s", iid)
		}
		bqRow := &bq.TableDataInsertAllRequestRows{InsertId: iid, Json: j}
		r.request.Rows = append(r.request.Rows, bqRow)
	}

	r.rowsArray = append(r.rowsArray, rows)
	r.size += len(rows.Body)
	return nil
}

type InsertError struct {
	Errors []*bq.ErrorProto
	Row    *bq.TableDataInsertAllRequestRows
}
