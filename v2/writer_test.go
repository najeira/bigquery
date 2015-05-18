package bigquery

import (
	bq "google.golang.org/api/bigquery/v2"
	"testing"
)

func TestChunkNewAndReady(t *testing.T) {
	c := &chunk{}
	if c.ready() {
		t.Errorf("chunk is ready")
	}
}

func TestChunkSingleAdd(t *testing.T) {
	c := &chunk{}
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
	c := &chunk{}
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
	c := &chunk{}
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
	q := &queue{}
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
	q := &queue{}
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
	q := &queue{}
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
	q := &queue{}
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
	q := &queue{}
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
	q := &queue{}
	c := q.popChunk()
	if c != nil {
		t.Errorf("queue.popChunk is invalid")
	}
}

func TestQueuePopChunkWhenNotReady(t *testing.T) {
	q := &queue{}
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
	q := &queue{}
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
