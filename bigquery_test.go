package bigquery

import "testing"

func Test_insertRows_key(t *testing.T) {
	rows := &insertRows{Project: "project", Dataset: "dataset", Table: "table", Body: nil}
	key := rows.key()
	if key != "project|dataset|table" {
		t.Error("invalid key")
	}
}

func Test_insertRows_decode_object(t *testing.T) {
	body := []byte(`{"name": "Alice", "city": "Tokyo"}`)
	rows := &insertRows{Project: "project", Dataset: "dataset", Table: "table", Body: body}
	arr, err := rows.decode()
	if err != nil {
		t.Error(err)
	}
	if arr == nil {
		t.Error("decode invalid return")
	}
	if len(arr) != 1 {
		t.Error("decode invalid length")
	}
}

func Test_insertRows_decode_array(t *testing.T) {
	body := []byte(`[
		{"name": "Alice", "city": "Tokyo"},
		{"name": "Bob", "city": "Osaka"}
	]`)
	rows := &insertRows{Project: "project", Dataset: "dataset", Table: "table", Body: body}
	arr, err := rows.decode()
	if err != nil {
		t.Error(err)
	}
	if arr == nil {
		t.Error("decode invalid return")
	}
	if len(arr) != 2 {
		t.Error("decode invalid length")
	}
}

func Test_newTableDataInsertAllRequest(t *testing.T) {
	body := []byte(`[
		{"name": "Alice", "city": "Tokyo"},
		{"name": "Bob", "city": "Osaka"}
	]`)
	rows := &insertRows{Project: "project", Dataset: "dataset", Table: "table", Body: body}

	r := newTableDataInsertAllRequest(rows.key())
	if r == nil {
		t.Error("newTableDataInsertAllRequest returns nil")
	}
	if r.project != "project" {
		t.Error("invalid project name")
	}
	if r.dataset != "dataset" {
		t.Error("invalid dataset name")
	}
	if r.table != "table" {
		t.Error("invalid table name")
	}
	if r.size != 0 {
		t.Errorf("invalid size", r.size)
	}
}

func Test_New(t *testing.T) {
	c := New("", nil)
	if c == nil {
		t.Error("New returns nil")
	}
}

func Test_Client_Add(t *testing.T) {
	c := New("", nil)

	if len(c.queues) != 0 {
		t.Error("queues has invalid length")
	}
	q, ok := c.queues["project|dataset|table"]
	if ok {
		t.Error("queues has key before adding")
	}
	if q != nil {
		t.Error("queues has key before adding")
	}

	c.Add("project", "dataset", "table", []byte(`[
		{"name": "Alice", "city": "Tokyo"},
		{"name": "Bob", "city": "Osaka"}
	]`))

	if c.queues == nil {
		t.Error("queues is nil")
	}
	if len(c.queues) != 1 {
		t.Error("queues has invalid length")
	}

	q, ok = c.queues["project|dataset|table"]
	if !ok {
		t.Error("queues does not have the key")
	}
	if q == nil {
		t.Error("queue is nil")
	}
	if q.Length() != 1 {
		t.Error("queue has invalid length")
	}

	c.Add("project", "dataset", "table", []byte(`[
		{"name": "Alice", "city": "Tokyo"},
		{"name": "Bob", "city": "Osaka"}
	]`))
	if q.Length() != 2 {
		t.Error("queue has invalid length")
	}
}

func Test_Client_getQueue(t *testing.T) {
	c := New("", nil)
	if len(c.queues) != 0 {
		t.Error("queues has invalid length")
	}

	q := c.getQueue("foo")
	if len(c.queues) != 1 {
		t.Error("queues has invalid length")
	}
	foo := q

	q = c.getQueue("bar")
	if len(c.queues) != 2 {
		t.Error("queues has invalid length")
	}

	q = c.getQueue("baz")
	if len(c.queues) != 3 {
		t.Error("queues has invalid length")
	}

	q = c.getQueue("foo")
	if len(c.queues) != 3 {
		t.Error("queues has invalid length")
	}
	if q != foo {
		t.Error("invalid queue")
	}
}

func Test_Client_putRowsToRequestFromQueue(t *testing.T) {
	rows := &insertRows{Project: "project", Dataset: "dataset", Table: "table", Body: nil}
	req := newTableDataInsertAllRequest(rows.key())

	c := New("", nil)
	c.Add("project", "dataset", "table", []byte(`[
		{"name": "Alice", "city": "Tokyo"},
		{"name": "Bob", "city": "Osaka"}
	]`))
	c.Add("project", "dataset", "table", []byte(`[
		{"name": "Taro", "city": "Sapporo"},
		{"name": "Jiro", "city": "Kyoto"}
	]`))
	c.putRowsToRequestFromQueue(req, c.queues[rows.key()])

	if req.project != "project" {
		t.Error("invalid project")
	}
	if req.dataset != "dataset" {
		t.Error("invalid dataset")
	}
	if req.table != "table" {
		t.Error("invalid table")
	}
	if req.rowsArray == nil {
		t.Error("invalid rowsArray")
	} else if len(req.rowsArray) != 2 {
		t.Error("invalid rowsArray length")
	}
	if req.request == nil {
		t.Error("invalid request")
	} else if len(req.request.Rows) != 4 {
		t.Error("invalid request length")
	} else {
		if req.request.Rows[0].Json["name"] != "Alice" {
			t.Error("invalid rows name")
		}
		if req.request.Rows[1].Json["name"] != "Bob" {
			t.Error("invalid rows name")
		}
		if req.request.Rows[2].Json["name"] != "Taro" {
			t.Error("invalid rows name")
		}
		if req.request.Rows[3].Json["name"] != "Jiro" {
			t.Error("invalid rows name")
		}
		if req.request.Rows[0].Json["city"] != "Tokyo" {
			t.Error("invalid rows city")
		}
		if req.request.Rows[1].Json["city"] != "Osaka" {
			t.Error("invalid rows city")
		}
		if req.request.Rows[2].Json["city"] != "Sapporo" {
			t.Error("invalid rows city")
		}
		if req.request.Rows[3].Json["city"] != "Kyoto" {
			t.Error("invalid rows city")
		}
	}
}
