package bigquery

import (
	bq "google.golang.org/api/bigquery/v2"
	"testing"
	"time"
)

type stub struct {
	project string
	dataset string
	table   string
	req     *bq.TableDataInsertAllRequest
	resp    *bq.TableDataInsertAllResponse
	err     error
}

func (s *stub) setResponse(resp *bq.TableDataInsertAllResponse, err error) {
	s.resp = resp
	s.err = err
}

func (s *stub) InsertAll(project string, dataset string, table string, req *bq.TableDataInsertAllRequest) (*bq.TableDataInsertAllResponse, error) {
	s.project = project
	s.dataset = dataset
	s.table = table
	s.req = req
	return s.resp, s.err
}

func TestChunkNewAndReady(t *testing.T) {
	c := newChunk()
	if c.ready() {
		t.Errorf("chunk is ready")
	}
}

func TestChunkSingleAdd(t *testing.T) {
	c := newChunk()
	entity := map[string]bq.JsonValue{"foo": "bar", "baz": 123}
	row := &bq.TableDataInsertAllRequestRows{InsertId: "aaa", Json: entity}
	err := c.add(row)
	if err != nil {
		t.Errorf("chunk.add returns %s", err)
	}
	if len(c.rows) != 1 {
		t.Errorf("chunk.rows is invalid")
	}
	if c.size <= 0 {
		t.Errorf("chunk.size is invalid")
	}
	if c.full {
		t.Errorf("chunk.full is invalid")
	}
}

func TestChunkAddLessLimit(t *testing.T) {
	c := newChunk()
	size := 0
	for i := 0; i < MaxRowsCountPerRequest; i++ {
		entity := map[string]bq.JsonValue{"foo": "bar", "baz": 123}
		row := &bq.TableDataInsertAllRequestRows{InsertId: "aaa", Json: entity}
		err := c.add(row)
		if err != nil {
			t.Errorf("chunk.add returns %s", err)
		}
		if len(c.rows) != (i + 1) {
			t.Errorf("chunk.rows is invalid")
		}
		if c.size <= size {
			t.Errorf("chunk.size is invalid")
		}
		size = c.size
		if c.full {
			t.Errorf("chunk.full is invalid")
		}
	}
}

func TestChunkAddFullCount(t *testing.T) {
	c := newChunk()
	size := 0
	for i := 0; i < MaxRowsCountPerRequest; i++ {
		entity := map[string]bq.JsonValue{"foo": "bar", "baz": 123}
		row := &bq.TableDataInsertAllRequestRows{InsertId: "aaa", Json: entity}
		err := c.add(row)
		if err != nil {
			t.Errorf("chunk.add returns %s", err)
		}
		if len(c.rows) != (i + 1) {
			t.Errorf("chunk.rows is invalid")
		}
		if c.size <= size {
			t.Errorf("chunk.size is invalid")
		}
		size = c.size
		if c.full {
			t.Errorf("chunk.full is invalid")
		}
	}

	entity := map[string]bq.JsonValue{"foo": "bar", "baz": 123}
	row := &bq.TableDataInsertAllRequestRows{InsertId: "aaa", Json: entity}
	err := c.add(row)
	if err != errChunkFull {
		t.Errorf("chunk.add returns %s", err)
	}
	if len(c.rows) != MaxRowsCountPerRequest {
		t.Errorf("chunk.row is invalid")
	}
	if !c.full {
		t.Errorf("chunk.full is invalid")
	}
}

func TestQueueEmpty(t *testing.T) {
	q := newQueue()
	if !q.empty() {
		t.Errorf("queue is not empty")
	}

	entity := map[string]interface{}{"foo": "bar", "baz": 123}
	err := q.add("", entity)
	if err != nil {
		t.Errorf("queue.add returns %s", err)
	}

	if q.empty() {
		t.Errorf("queue.empty is invalid")
	}
}

func TestQueueCount(t *testing.T) {
	q := newQueue()
	if q.count() != 0 {
		t.Errorf("queue is not zero")
	}

	entity := map[string]interface{}{"foo": "bar", "baz": 123}
	err := q.add("", entity)
	if err != nil {
		t.Errorf("queue.add returns %s", err)
	}

	if q.count() != 1 {
		t.Errorf("queue.count is invalid")
	}
}

func TestQueueSingleAdd(t *testing.T) {
	q := newQueue()
	entity := map[string]interface{}{"foo": "bar", "baz": 123}
	err := q.add("", entity)
	if err != nil {
		t.Errorf("queue.add returns %s", err)
	}
	if q.empty() {
		t.Errorf("queue.empty is invalid")
	}
	if q.count() != 1 {
		t.Errorf("queue.count is invalid")
	}
}

func TestQueueAddLessLimit(t *testing.T) {
	q := newQueue()
	for i := 0; i < MaxRowsCountPerRequest; i++ {
		entity := map[string]interface{}{"foo": "bar", "baz": 123}
		err := q.add("", entity)
		if err != nil {
			t.Errorf("queue.add returns %s", err)
		}
		if q.empty() {
			t.Errorf("queue.empty is invalid")
		}
		if q.count() != (i + 1) {
			t.Errorf("queue.count is invalid")
		}
		if len(q.chunks) != 1 {
			t.Errorf("queue.chunks is invalid")
		}
	}
}

func TestQueueAddMultiChunks(t *testing.T) {
	q := newQueue()
	for i := 0; i < MaxRowsCountPerRequest; i++ {
		entity := map[string]interface{}{"foo": "bar", "baz": 123}
		err := q.add("", entity)
		if err != nil {
			t.Errorf("queue.add returns %s", err)
		}
		if q.empty() {
			t.Errorf("queue.empty is invalid")
		}
		if q.count() != (i + 1) {
			t.Errorf("queue.count is invalid")
		}
	}

	if len(q.chunks) != 1 {
		t.Errorf("queue.chunks is invalid")
	}

	entity := map[string]interface{}{"foo": "bar", "baz": 123}
	err := q.add("", entity)
	if err != nil {
		t.Errorf("queue.add returns %s", err)
	}
	if q.empty() {
		t.Errorf("queue should not empty")
	}
	if q.count() != (MaxRowsCountPerRequest + 1) {
		t.Errorf("queue.count is invalid")
	}
	if len(q.chunks) != 2 {
		t.Errorf("queue.chunks is invalid")
	}
}

func TestQueuePopChunkWhenEmpty(t *testing.T) {
	q := newQueue()
	c := q.popChunk()
	if c != nil {
		t.Errorf("queue.popChunk is invalid")
	}
}

func TestQueuePopChunkWhenNotReady(t *testing.T) {
	q := newQueue()
	for i := 0; i < MaxRowsCountPerRequest; i++ {
		entity := map[string]interface{}{"foo": "bar", "baz": 123}
		q.add("", entity)
		c := q.popChunk()
		if c != nil {
			t.Errorf("queue.popChunk is invalid")
		}
	}
}

func TestQueuePopChunkWhenReady(t *testing.T) {
	q := newQueue()
	for i := 0; i < MaxRowsCountPerRequest; i++ {
		entity := map[string]interface{}{"foo": "bar", "baz": 123}
		q.add("", entity)
		c := q.popChunk()
		if c != nil {
			t.Errorf("queue.popChunk is invalid")
		}
	}

	entity := map[string]interface{}{"foo": "bar", "baz": 123}
	q.add("", entity)
	c := q.popChunk()
	if c == nil {
		t.Errorf("queue.popChunk is invalid")
	}
	if len(c.rows) != MaxRowsCountPerRequest {
		t.Errorf("chunk.rows is invalid")
	}
}

func TestQueuePushChunk(t *testing.T) {
	q := newQueue()
	for i := 0; i < MaxRowsCountPerRequest; i++ {
		entity := map[string]interface{}{"foo": "bar", "baz": 123}
		q.add("", entity)
		c := q.popChunk()
		if c != nil {
			t.Errorf("queue.popChunk is invalid")
		}
	}

	entity := map[string]interface{}{"foo": "bar", "baz": 123}
	q.add("", entity)
	c := q.popChunk()

	q.pushChunk(c)
	if q.empty() {
		t.Errorf("queue.empty is invalid")
	}
	if len(q.chunks) != 2 {
		t.Errorf("queue.chunks is invalid")
	}
	if q.count() != MaxRowsCountPerRequest+1 {
		t.Errorf("queue.count is invalid")
	}
}

func TestWriterNewWriter(t *testing.T) {
	w := NewWriter("p", "d", "t")
	if w == nil {
		t.Errorf("NewWriter is invalid")
	}
	if w.project != "p" {
		t.Errorf("NewWriter is invalid")
	}
	if w.dataset != "d" {
		t.Errorf("NewWriter is invalid")
	}
	if w.table != "t" {
		t.Errorf("NewWriter is invalid")
	}
	if w.queue == nil {
		t.Errorf("NewWriter is invalid")
	}
	if w.running {
		t.Errorf("NewWriter is invalid")
	}
	if w.closed {
		t.Errorf("NewWriter is invalid")
	}
}

func TestWriterEmpty(t *testing.T) {
	s := &stub{}

	w := NewWriter("p", "d", "t")
	w.SetService(s)
	if !w.empty() {
		t.Errorf("Writer.empty is invalid")
	}
}

func TestWriterInsertAll(t *testing.T) {
	s := &stub{}

	w := NewWriter("p", "d", "t")
	w.SetService(s)

	c := newChunk()
	entity := map[string]bq.JsonValue{"foo": "bar", "baz": 123}
	row := &bq.TableDataInsertAllRequestRows{InsertId: "aaa", Json: entity}
	c.add(row)

	w.insertAll(c)
	if s.project != "p" {
		t.Errorf("insertAll is invalid")
	}
	if s.dataset != "d" {
		t.Errorf("insertAll is invalid")
	}
	if s.table != "t" {
		t.Errorf("insertAll is invalid")
	}
	if s.req == nil {
		t.Errorf("insertAll is invalid")
	} else if s.req.Rows == nil {
		t.Errorf("insertAll is invalid")
	} else {
		if len(s.req.Rows) != 1 {
			t.Errorf("insertAll is invalid")
		} else {
			row := s.req.Rows[0]
			if row.InsertId != "aaa" {
				t.Errorf("insertAll is invalid")
			}
			if row.Json == nil {
				t.Errorf("insertAll is invalid")
			} else {
				if row.Json["foo"] != "bar" {
					t.Errorf("insertAll is invalid")
				}
				if row.Json["baz"] != 123 {
					t.Errorf("insertAll is invalid")
				}
			}
		}
	}
}

func TestWriterFlushQueueWhenEmpty(t *testing.T) {
	s := &stub{}

	w := NewWriter("p", "d", "t")
	w.SetService(s)

	size, count, errs := w.flushQueue()
	if count != 0 {
		t.Errorf("flushQueue is invalid")
	}
	if size != 0 {
		t.Errorf("flushQueue is invalid")
	}
	if errs != 0 {
		t.Errorf("flushQueue is invalid")
	}
}

func TestWriterFlushWhenEmpty(t *testing.T) {
	s := &stub{}

	w := NewWriter("p", "d", "t")
	w.SetService(s)

	w.flush()

	if w.delay != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.count != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.bytes != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.failedCount != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.retriedCount != 0 {
		t.Errorf("flush is invalid")
	}
}

func TestWriterFlushQueueWhenSingleValue(t *testing.T) {
	s := &stub{}

	w := NewWriter("p", "d", "t")
	w.SetService(s)

	value := map[string]interface{}{"foo": "bar", "baz": 123}
	w.queue.add("aaa", value)

	size, count, errs := w.flushQueue()
	if count != 0 {
		t.Errorf("flushQueue is invalid")
	}
	if size != 0 {
		t.Errorf("flushQueue is invalid")
	}
	if errs != 0 {
		t.Errorf("flushQueue is invalid")
	}
}

func TestWriterFlushWhenSingleValue(t *testing.T) {
	s := &stub{}

	w := NewWriter("p", "d", "t")
	w.SetService(s)

	value := map[string]interface{}{"foo": "bar", "baz": 123}
	w.queue.add("aaa", value)

	w.flush()

	if w.delay != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.count != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.bytes != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.failedCount != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.retriedCount != 0 {
		t.Errorf("flush is invalid")
	}
}

func TestWriterFlushQueueWhenSingleChunkFull(t *testing.T) {
	s := &stub{}

	resp := &bq.TableDataInsertAllResponse{}
	s.setResponse(resp, nil)

	w := NewWriter("p", "d", "t")
	w.SetService(s)

	for i := 0; i < MaxRowsCountPerRequest + 1; i++ {
		entity := map[string]interface{}{"foo": "bar", "baz": 123}
		w.queue.add("aaa", entity)
	}

	size, count, errs := w.flushQueue()
	if count != MaxRowsCountPerRequest {
		t.Errorf("flushQueue is invalid %d", count)
	}
	if size <= 0 {
		t.Errorf("flushQueue is invalid %d", size)
	}
	if errs != 0 {
		t.Errorf("flushQueue is invalid %d", errs)
	}

	if s.project != "p" {
		t.Errorf("flushQueue is invalid")
	}
	if s.dataset != "d" {
		t.Errorf("flushQueue is invalid")
	}
	if s.table != "t" {
		t.Errorf("flushQueue is invalid")
	}
	if s.req == nil {
		t.Errorf("flushQueue is invalid")
	} else if s.req.Rows == nil {
		t.Errorf("flushQueue is invalid")
	} else {
		if len(s.req.Rows) != MaxRowsCountPerRequest {
			t.Errorf("flushQueue is invalid")
		} else {
			for _, row := range s.req.Rows {
				if row.InsertId != "aaa" {
					t.Errorf("flushQueue is invalid")
					break
				}
				if row.Json == nil {
					t.Errorf("flushQueue is invalid")
					break
				} else {
					if row.Json["foo"] != "bar" {
						t.Errorf("flushQueue is invalid")
						break
					}
					if row.Json["baz"] != 123 {
						t.Errorf("flushQueue is invalid")
						break
					}
				}
			}
		}
	}
}

func TestWriterFlushWhenSingleChunkFull(t *testing.T) {
	s := &stub{}

	resp := &bq.TableDataInsertAllResponse{}
	s.setResponse(resp, nil)

	w := NewWriter("p", "d", "t")
	w.SetService(s)

	for i := 0; i < MaxRowsCountPerRequest + 1; i++ {
		entity := map[string]interface{}{"foo": "bar", "baz": 123}
		w.queue.add("aaa", entity)
	}

	w.flush()

	if w.delay != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.count != MaxRowsCountPerRequest {
		t.Errorf("flush is invalid")
	}
	if w.stats.bytes <= 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.failedCount != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.retriedCount != 0 {
		t.Errorf("flush is invalid")
	}

	if s.project != "p" {
		t.Errorf("flushQueue is invalid")
	}
	if s.dataset != "d" {
		t.Errorf("flushQueue is invalid")
	}
	if s.table != "t" {
		t.Errorf("flushQueue is invalid")
	}
	if s.req == nil {
		t.Errorf("flushQueue is invalid")
	} else if s.req.Rows == nil {
		t.Errorf("flushQueue is invalid")
	} else {
		if len(s.req.Rows) != MaxRowsCountPerRequest {
			t.Errorf("flushQueue is invalid")
		} else {
			for _, row := range s.req.Rows {
				if row.InsertId != "aaa" {
					t.Errorf("flushQueue is invalid")
					break
				}
				if row.Json == nil {
					t.Errorf("flushQueue is invalid")
					break
				} else {
					if row.Json["foo"] != "bar" {
						t.Errorf("flushQueue is invalid")
						break
					}
					if row.Json["baz"] != 123 {
						t.Errorf("flushQueue is invalid")
						break
					}
				}
			}
		}
	}
}

func TestWriterRunFlusherWhenEmpty(t *testing.T) {
	s := &stub{}

	resp := &bq.TableDataInsertAllResponse{}
	s.setResponse(resp, nil)

	w := NewWriter("p", "d", "t")
	w.SetService(s)

	w.runFlusher()
	time.Sleep(1 * time.Nanosecond)

	if w.delay != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.count != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.bytes != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.failedCount != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.retriedCount != 0 {
		t.Errorf("flush is invalid")
	}
}

func TestWriterRunFlusherWhenSingleChunk(t *testing.T) {
	s := &stub{}

	resp := &bq.TableDataInsertAllResponse{}
	s.setResponse(resp, nil)

	w := NewWriter("p", "d", "t")
	w.SetService(s)

	for i := 0; i < 100; i++ {
		entity := map[string]interface{}{"foo": "bar", "baz": 123}
		w.queue.add("aaa", entity)
	}

	w.runFlusher()
	w.wg.Wait()

	if w.delay != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.count != 100 {
		t.Errorf("flush is invalid")
	}
	if w.stats.bytes <= 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.failedCount != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.retriedCount != 0 {
		t.Errorf("flush is invalid")
	}
}

func TestWriterRunFlusherWhenMultipleChunk(t *testing.T) {
	s := &stub{}

	resp := &bq.TableDataInsertAllResponse{}
	s.setResponse(resp, nil)

	w := NewWriter("p", "d", "t")
	w.SetService(s)

	for i := 0; i < 1001; i++ {
		entity := map[string]interface{}{"foo": "bar", "baz": 123}
		w.queue.add("aaa", entity)
	}

	w.runFlusher()
	w.wg.Wait()

	if w.delay != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.count != 1001 {
		t.Errorf("flush is invalid")
	}
	if w.stats.bytes <= 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.failedCount != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.retriedCount != 0 {
		t.Errorf("flush is invalid")
	}
}

func TestWriterAddMultipleChunk(t *testing.T) {
	s := &stub{}

	resp := &bq.TableDataInsertAllResponse{}
	s.setResponse(resp, nil)

	w := NewWriter("p", "d", "t")
	w.SetService(s)

	for i := 0; i < 1001; i++ {
		entity := map[string]interface{}{"foo": "bar", "baz": 123}
		err := w.Add("aaa", entity)
		if err != nil {
			t.Errorf("Add is invalid")
			return
		}
	}

	w.wg.Wait()

	if w.delay != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.count != 1001 {
		t.Errorf("flush is invalid")
	}
	if w.stats.bytes <= 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.failedCount != 0 {
		t.Errorf("flush is invalid")
	}
	if w.stats.retriedCount != 0 {
		t.Errorf("flush is invalid")
	}
}
