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
	ErrClosed         = fmt.Errorf("closed")
	ErrRecordTooLarge = fmt.Errorf("record is too large")
	errChunkFull      = fmt.Errorf("chunk is full")
)

// ErrorHandler is to get errors at writer.
type ErrorHandler func(err error)

// Logger define logging interface of this package.
type Logger interface {
	Verbosef(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Noticef(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Criticalf(format string, v ...interface{})
}

type Service interface {
	InsertAll(string, string, string, *bq.TableDataInsertAllRequest) *bq.TabledataInsertAllCall
}

// Config specify authorization and table of BigQuery.
type Config struct {
	Email   string
	Pem     []byte
	Project string
	Dataset string
	Table   string
}

// Writer writes rows to BigQuery.
type Writer struct {
	config       Config
	service      Service
	queue        *queue
	logger       Logger
	errorHandler ErrorHandler
	stats        writerStats
	delay        int
	mu           sync.RWMutex
	wg           sync.WaitGroup
	running      bool
	closed       bool
	closing      chan bool
}

// Creates and returns a new Writer.
func NewWriter(config Config) *Writer {
	return &Writer{
		config:  config,
		service: nil,
		queue:   newQueue(),
		logger:  nil,
		delay:   0,
		running: false,
		closed:  false,
		closing: make(chan bool, 1),
	}
}

func (w *Writer) Connect() error {
	if w.service == nil {
		return w.connect()
	}
	return nil
}

func (w *Writer) connect() error {
	cfg := jwt.Config{
		Email:      w.config.Email,
		PrivateKey: w.config.Pem,
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
	w.service = bq.Tabledata
	w.debugf("connected")
	return nil
}

func (w *Writer) SetLogger(l Logger) {
	w.logger = l
}

func (w *Writer) SetErrorHandler(h ErrorHandler) {
	w.errorHandler = h
}

func (w *Writer) SetService(service Service) {
	w.service = service
}

func (w *Writer) callErrorHandler(err error) {
	if w.errorHandler != nil {
		w.errorHandler(err)
	}
}

// Adds the row to the writer.
// The row is stored queue in the writer.
// Rows in the queue will be sent asynchronously to BigQuery soon.
// The value is not able to encode json or too large then Add returns error.
func (w *Writer) Add(insertId string, value map[string]interface{}) error {
	w.mu.RLock()
	closed := w.closed
	running := w.running
	w.mu.RUnlock()

	if closed {
		return ErrClosed
	}

	err := w.queue.add(insertId, value)

	if !running {
		w.runFlusher()
	}

	return err
}

func (w *Writer) runFlusher() {
	w.mu.Lock()
	closed := w.closed
	running := w.running
	w.running = true
	w.mu.Unlock()

	if closed || running {
		return
	}

	w.debugf("wakeup flusher")

	w.wg.Add(1)
	go w.flusher()
}

func (w *Writer) flusher() {
	w.debugf("flusher start")

	w.flusherLoop()

	// this loop will be done soon, running is false.
	w.mu.Lock()
	w.running = false
	w.mu.Unlock()

	// launch flushing loop if Add called between flusherLoop() and here.
	if !w.empty() {
		w.runFlusher()
	}

	w.debugf("flusher end")

	w.wg.Done()
}

func (w *Writer) flusherLoop() {
	w.debugf("flusher start")

	// ensure connect
	if err := w.Connect(); err != nil {
		w.errorf("connect error %v", err)
		return
	}

	// flushing until the writer has rows.
	for !w.empty() {

		// waiting duration increase if error occured.
		wait := (w.delay * w.delay) + 1
		sleep := time.Duration(wait) * time.Second
		w.debugf("wait %d seconds", wait)

		select {
		case <-time.After(sleep):
			w.flush()
		case <-w.closing:
			// Close was called. it should return from loop.
			w.debugf("flusher closing")
			return
		}
	}

	w.debugf("empty")
}

// Waits sending all records in the writer to BQ.
func (w *Writer) Wait() {
	w.wg.Wait()
}

func (w *Writer) Close() {
	w.mu.Lock()
	closed := w.closed
	w.closed = true
	w.mu.Unlock()

	if closed {
		// Close was called already.
		return
	}

	// send value to closing to finish flushing loop.
	// it does nothing if the writer is empty and not block.
	w.closing <- true

	// wait finish completely.
	w.wg.Wait()
}

func (w *Writer) Dump() ([]byte, error) {
	return w.queue.dump()
}

func (w *Writer) Load(data []byte) error {
	return w.queue.load(data)
}

func (w *Writer) flush() {
	start := time.Now()
	beforeCount := w.stats.Count()

	w.updateRemainingCount()

	noErr := w.flushQueue()

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

func (w *Writer) flushQueue() bool {
	var errCnt int32 = 0
	size := 0
	count := 0

	var wg sync.WaitGroup

	for {
		c := w.queue.popChunk()
		if c == nil {
			w.debugf("no chunks")
			break
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

		w.debugf("flushing %d rows", count)

		wg.Add(1)
		go func() {
			if !w.insertAll(c) {
				atomic.AddInt32(&errCnt, 1)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return errCnt <= 0
}

func (w *Writer) empty() bool {
	return w.count() <= 0
}

func (w *Writer) count() int {
	return w.queue.count()
}

// Returns statistics of the writer.
func (w *Writer) Stats() Stats {
	return &w.stats
}

func (w *Writer) updateRemainingCount() {
	w.stats.setRemainingCount(int64(w.count()))
}

func (w *Writer) insertAll(c *chunk) bool {
	req := &bq.TableDataInsertAllRequest{Rows: c.rows}
	call := w.service.InsertAll(w.config.Project, w.config.Dataset, w.config.Table, req)
	resp, err := call.Do()
	return w.handleInsertAll(c, resp, err)
}

func (w *Writer) handleInsertAll(c *chunk, resp *bq.TableDataInsertAllResponse, err error) bool {
	if err != nil {
		w.handleInsertAllError(c, err)
		return false
	}

	if resp == nil {
		w.errorf("resp is nil")
		return false
	}

	return w.handleInsertAllResponse(c, resp)
}

func (w *Writer) handleInsertAllError(c *chunk, err error) {
	cnt := int64(len(c.rows))
	w.stats.incrFailedCount(cnt)

	if gerr, ok := err.(*googleapi.Error); ok {
		w.warnf("%d %s", gerr.Code, gerr.Message)
		if 500 <= gerr.Code && gerr.Code <= 599 {
			// 5xx is temporary error. retry the chunk.
			w.queue.pushChunk(c)
			w.stats.incrRetriedCount(cnt)
		}
	} else {
		// unknown error.
		w.errorf(err.Error())
	}

	w.callErrorHandler(err)
}

func (w *Writer) handleInsertAllResponse(c *chunk, resp *bq.TableDataInsertAllResponse) bool {
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
				w.queue.addRow(r)
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
		return ErrRecordTooLarge
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
	mu     sync.RWMutex
	chunks []*chunk
}

func newQueue() *queue {
	return &queue{chunks: make([]*chunk, 0)}
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
	q.mu.RLock()
	ret := q.countImpl()
	q.mu.RUnlock()
	return ret
}

func (q *queue) countImpl() int {
	var ret int = 0
	for _, c := range q.chunks {
		ret += len(c.rows)
	}
	return ret
}

func (q *queue) dump() ([]byte, error) {
	q.mu.Lock()

	rows := make([]*bq.TableDataInsertAllRequestRows, 0, q.countImpl())

	for _, c := range q.chunks {
		if len(c.rows) > 0 {
			rows = append(rows, c.rows...)
		}
	}

	q.mu.Unlock()

	return json.Marshal(rows)
}

func (q *queue) load(data []byte) error {
	var rows []*bq.TableDataInsertAllRequestRows
	if err := json.Unmarshal(data, rows); err != nil {
		return err
	}
	for _, row := range rows {
		q.addRow(row)
	}
	return nil
}
