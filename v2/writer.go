package bigquery

import (
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/jwt"
	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MaxRowBytes            = 1 * 1000 * 1000
	MaxRowsCountPerRequest = 500
	MaxRequestBytes        = 10 * 1000 * 1000
	MaxBytesPerSecond      = 100 * 1000 * 1000
	MaxRowsCountPerSecond  = 100000
)

var (
	errChunkFull      = fmt.Errorf("chunk is full")
	errRecordTooLarge = fmt.Errorf("record is too large")
)

type ErrorHandler func(err error)

type Logger interface {
	Verbosef(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Criticalf(format string, v ...interface{})
}

type Writer struct {
	email        string
	pem          []byte
	service      *bq.Service
	queues       map[string]*queue
	logger       Logger
	errorHandler ErrorHandler
	stats        writerStats
	delay        int
	mu           sync.RWMutex
	wg           sync.WaitGroup
	running      bool
}

// Creates and returns a new Writer.
func NewWriter(email string, pem []byte) *Writer {
	return &Writer{
		email:   email,
		pem:     pem,
		service: nil,
		delay:   0,
		queues:  make(map[string]*queue, 0),
		logger:  nil,
		running: false,
	}
}

func (w *Writer) ensureConnect() error {
	if w.service == nil {
		return w.connect()
	}
	return nil
}

func (w *Writer) connect() error {
	cfg := jwt.Config{
		Email:      w.email,
		PrivateKey: w.pem,
		Scopes:     []string{bq.BigqueryScope},
		TokenURL:   "https://accounts.google.com/o/oauth2/token",
	}
	ctx := context.Background()
	client := cfg.Client(ctx)
	bq, err := bq.New(client)
	if err != nil {
		w.warnf("connect error %v", err)
		return err
	}
	w.service = bq
	w.debugf("connected")
	return nil
}

func (w *Writer) SetLogger(l Logger) {
	w.logger = l
}

func (w *Writer) SetErrorHandler(h ErrorHandler) {
	w.errorHandler = h
}

// Adds the row to the writer.
// The row is stored queue in the writer.
// Rows in the queue will be sent asynchronously to BigQuery soon.
// The value is not able to encode json or too large then Add returns error.
func (w *Writer) Add(project, dataset, table, insertId string, value map[string]interface{}) error {
	key := queueKey(project, dataset, table)

	w.mu.Lock()
	q, ok := w.queues[key]
	if !ok {
		q = newQueue(project, dataset, table)
		w.queues[key] = q
	}
	w.mu.Unlock()

	err := q.add(insertId, value)
	w.runFlusher()
	return err
}

func (w *Writer) runFlusher() {
	w.mu.Lock()
	r := w.running
	w.running = true
	w.mu.Unlock()

	if !r {
		err := w.ensureConnect()
		if err != nil {
			return
		}

		w.debugf("wakeup flusher")
		w.wg.Add(1)
		go w.flusher()
	}
}

func (w *Writer) flusher() {
	w.debugf("flusher start")

	for {
		wait := (w.delay * w.delay) + 1
		w.debugf("wait %d seconds", wait)
		<-time.After(time.Duration(wait) * time.Second)

		w.flush()

		if w.empty() {
			w.debugf("empty")
			break
		}
	}

	w.mu.Lock()
	w.running = false
	w.mu.Unlock()

	if !w.empty() {
		w.runFlusher()
	}

	w.debugf("flusher end")
	w.wg.Done()
}

// Waits all records in the writer are sent.
func (w *Writer) Wait() {
	w.wg.Wait()
}

func (w *Writer) flush() {
	start := time.Now()
	beforeCount := w.stats.Count()

	w.updateRemainingCount()

	noErr := w.flushQueues()

	elapsed := time.Now().Sub(start)
	w.stats.setLatency(elapsed)

	afterCount := w.stats.Count()
	throughput := float64(beforeCount - afterCount)
	if elapsed > time.Second {
		throughput /= elapsed.Seconds()
	}
	w.stats.setThroughput(throughput)

	w.updateRemainingCount()

	if noErr {
		w.delay = 0
	} else {
		w.delay += 1
	}
}

func (w *Writer) flushQueues() bool {
	var errCnt int32 = 0
	size := 0
	count := 0

	var wg sync.WaitGroup
	w.mu.RLock()

	for _, q := range w.queues {
		c := q.popChunk()
		if c == nil {
			w.debugf("no chunks")
			continue
		}

		size += c.size
		count += len(c.rows)

		if size > MaxBytesPerSecond {
			w.infof("reached to MaxBytesPerSecond")
			break
		} else if count > MaxRowsCountPerSecond {
			w.infof("reached to MaxRowsCountPerSecond")
			break
		}

		w.debugf("flushing")

		wg.Add(1)
		go func() {
			if !w.insertAll(q, c) {
				atomic.AddInt32(&errCnt, 1)
			}
			wg.Done()
		}()
	}

	w.mu.RUnlock()
	wg.Wait()

	return errCnt <= 0
}

func (w *Writer) empty() bool {
	return w.count() <= 0
}

func (w *Writer) count() int {
	cnt := 0
	w.mu.RLock()
	for _, q := range w.queues {
		cnt += q.count()
	}
	w.mu.RUnlock()
	return cnt
}

// Returns statistics of the writer.
func (w *Writer) Stats() Stats {
	return &w.stats
}

func (w *Writer) updateRemainingCount() {
	w.stats.setRemainingCount(int64(w.count()))
}

func (w *Writer) insertAll(q *queue, c *chunk) bool {
	req := &bq.TableDataInsertAllRequest{Rows: c.rows}
	call := w.service.Tabledata.InsertAll(q.project, q.dataset, q.table, req)
	resp, err := call.Do()
	return w.handleInsertAll(q, c, resp, err)
}

func (w *Writer) handleInsertAll(q *queue, c *chunk, resp *bq.TableDataInsertAllResponse, err error) bool {
	if err != nil {
		w.handleInsertAllError(q, c, err)
		return false
	}

	if resp == nil {
		w.errorf("resp is nil")
		return false
	}

	return w.handleInsertAllResponse(q, c, resp)
}

func (w *Writer) handleInsertAllError(q *queue, c *chunk, err error) {
	cnt := int64(len(c.rows))
	w.stats.incrFailedCount(cnt)

	if gerr, ok := err.(*googleapi.Error); ok {
		w.warnf("%d %s", gerr.Code, gerr.Message)
		if 500 <= gerr.Code && gerr.Code <= 599 {
			// 5xx is temporary error. retry the chunk.
			q.pushChunk(c)
			w.stats.incrRetriedCount(cnt)
		}
	} else {
		// unknown error.
		w.errorf(err.Error())
	}

	if w.errorHandler != nil {
		w.errorHandler(err)
	}
}

func (w *Writer) handleInsertAllResponse(q *queue, c *chunk, resp *bq.TableDataInsertAllResponse) bool {
	retriedCount := 0

	for _, ie := range resp.InsertErrors {
		if int(ie.Index) < len(c.rows) {
			w.errorf("invalid index %d", ie.Index)
			continue
		}

		r := c.rows[ie.Index]

		if ie.Errors != nil {
			retry := true
			for _, ep := range ie.Errors {
				w.warnf("%d %s", ep.Reason, ep.Message)
				if ep.Reason != "backendError" || ep.Reason != "internalError" {
					// the record is not retriable
					retry = false
				}
			}
			if retry {
				q.addRow(r)
				retriedCount += 1
			}
		}
	}

	failedCount := len(resp.InsertErrors)
	sentCount := len(c.rows) - failedCount

	w.stats.incrBytes(int64(c.size))
	w.stats.incrCount(int64(sentCount))
	w.stats.incrFailedCount(int64(failedCount))
	w.stats.incrRetriedCount(int64(retriedCount))

	w.debugf("insertAll: %d rows, %d bytes, %d errors", sentCount, c.size, failedCount)
	return failedCount <= 0
}

func (w *Writer) verbosef(format string, v ...interface{}) {
	if w.logger != nil {
		w.logger.Verbosef(format, v...)
	}
}

func (w *Writer) debugf(format string, v ...interface{}) {
	if w.logger != nil {
		w.logger.Debugf(format, v...)
	}
}

func (w *Writer) infof(format string, v ...interface{}) {
	if w.logger != nil {
		w.logger.Infof(format, v...)
	}
}

func (w *Writer) warnf(format string, v ...interface{}) {
	if w.logger != nil {
		w.logger.Warnf(format, v...)
	}
}

func (w *Writer) errorf(format string, v ...interface{}) {
	if w.logger != nil {
		w.logger.Errorf(format, v...)
	}
}

func (w *Writer) criticalf(format string, v ...interface{}) {
	if w.logger != nil {
		w.logger.Criticalf(format, v...)
	}
}

type Stats interface {
	// The number of total rows were sent to Kinesis
	Count() int64

	// The number of total bytes were sent to Kinesis
	Bytes() int64

	// The number of total failued record count
	FailedCount() int64

	// The number of total retries count
	RetriedCount() int64

	// The number of rows in the writer
	RemainingCount() int64

	// The last latency of Kinesis
	Latency() time.Duration

	// The throughput
	Throughput() float64
}

type writerStats struct {
	count          int64
	bytes          int64
	failedCount    int64
	retriedCount   int64
	remainingCount int64
	latency        int64
	throughput     int64
}

func (s *writerStats) Count() int64 {
	return atomic.LoadInt64(&s.count)
}

func (s *writerStats) Bytes() int64 {
	return atomic.LoadInt64(&s.bytes)
}

func (s *writerStats) FailedCount() int64 {
	return atomic.LoadInt64(&s.failedCount)
}

func (s *writerStats) RetriedCount() int64 {
	return atomic.LoadInt64(&s.retriedCount)
}

func (s *writerStats) RemainingCount() int64 {
	return atomic.LoadInt64(&s.remainingCount)
}

func (s *writerStats) Latency() time.Duration {
	v := atomic.LoadInt64(&s.latency)
	return time.Duration(v)
}

func (s *writerStats) Throughput() float64 {
	v := atomic.LoadInt64(&s.throughput)
	return float64(v / 1000000)
}

func (s *writerStats) incrCount(value int64) {
	atomic.AddInt64(&s.count, value)
}

func (s *writerStats) incrBytes(value int64) {
	atomic.AddInt64(&s.bytes, value)
}

func (s *writerStats) incrFailedCount(value int64) {
	atomic.AddInt64(&s.failedCount, value)
}

func (s *writerStats) incrRetriedCount(value int64) {
	atomic.AddInt64(&s.retriedCount, value)
}

func (s *writerStats) setRemainingCount(value int64) {
	atomic.StoreInt64(&s.remainingCount, value)
}

func (s *writerStats) setLatency(value time.Duration) {
	atomic.StoreInt64(&s.latency, value.Nanoseconds())
}

func (s *writerStats) setThroughput(value float64) {
	atomic.StoreInt64(&s.throughput, int64(value*1000000))
}

type chunk struct {
	rows    []*bq.TableDataInsertAllRequestRows
	size    int
	full    bool
	created time.Time
}

func newChunk() *chunk {
	return &chunk{
		rows:    make([]*bq.TableDataInsertAllRequestRows, 0, MaxRowsCountPerRequest),
		size:    0,
		full:    false,
		created: time.Now(),
	}
}

func (c *chunk) add(row *bq.TableDataInsertAllRequestRows) error {
	// marshal value to json string to calculate size.
	data, err := json.Marshal(row.Json)
	if err != nil {
		return err
	}
	size := len(data)

	if size > MaxRowBytes {
		return errRecordTooLarge
	} else if len(c.rows) >= MaxRowsCountPerRequest {
		// mark the chunk as full
		// this chunk will be sent at next batch
		c.full = true
		return errChunkFull
	} else if c.size+size > MaxRequestBytes {
		// mark the chunk as full
		// this chunk will be sent at next batch
		c.full = true
		return errChunkFull
	}

	c.rows = append(c.rows, row)
	c.size += size
	return nil
}

func (c *chunk) ready() bool {
	if c.full {
		return true
	} else if len(c.rows) <= 0 {
		return false
	}
	delta := time.Now().Sub(c.created)
	return delta > (1 * time.Second)
}

type queue struct {
	project string
	dataset string
	table   string

	mu     sync.RWMutex
	chunks []*chunk
}

func newQueue(project, dataset, table string) *queue {
	return &queue{
		project: project,
		dataset: dataset,
		table:   table,
		chunks:  make([]*chunk, 0),
	}
}

func (q *queue) empty() bool {
	q.mu.RLock()
	n := len(q.chunks)
	q.mu.RUnlock()
	return n <= 0
}

func (q *queue) popChunk() *chunk {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.chunks) <= 0 {
		return nil // no chunks
	}

	c := q.chunks[0]
	if !c.ready() {
		return nil // not ready
	}

	q.chunks = q.chunks[1:]
	return c
}

func (q *queue) pushChunk(c *chunk) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.chunks = append(q.chunks, c)
}

func (q *queue) add(insertId string, value map[string]interface{}) error {
	// convert value to the BigQuery type
	entity := make(map[string]bq.JsonValue)
	for k, v := range value {
		entity[k] = bq.JsonValue(v)
	}
	row := &bq.TableDataInsertAllRequestRows{InsertId: insertId, Json: entity}
	return q.addRow(row)
}

func (q *queue) addRow(row *bq.TableDataInsertAllRequestRows) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.chunks) <= 0 {
		q.chunks = append(q.chunks, newChunk())
	}

	// a chunk that will be added the record
	c := q.chunks[len(q.chunks)-1]

	if err := c.add(row); err != nil {
		if err != errChunkFull {
			return err
		}

		// append a new chunk to tail of the queue
		nc := newChunk()
		q.chunks = append(q.chunks, nc)

		// add the record to the new chunk
		if err2 := nc.add(row); err2 != nil {
			return err2
		}
	}
	return nil
}

func (q *queue) count() int {
	var ret int = 0
	q.mu.RLock()
	for _, c := range q.chunks {
		ret += len(c.rows)
	}
	q.mu.RUnlock()
	return ret
}

func queueKey(project, dataset, table string) string {
	return fmt.Sprintf("%s|%s|%s", project, dataset, table)
}
