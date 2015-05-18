package main

import (
	"bytes"
	"testing"
	"time"
)

type row struct {
	insertId string
	value    map[string]interface{}
}

type bufWriter struct {
	project string
	dataset string
	table   string
	rows    []*row
}

func (w *bufWriter) Add(insertId string, value map[string]interface{}) error {
	if w.rows == nil {
		w.rows = make([]*row, 0)
	}
	w.rows = append(w.rows, &row{insertId: insertId, value: value})
	return nil
}

func TestClientInsertId(t *testing.T) {
	w := &bufWriter{project: "p", dataset: "d", table: "t"}
	c := client{writer: w}
	s1 := c.insertId()
	s2 := c.insertId()
	if s1 == "" {
		t.Errorf("c.insertId is empty")
	}
	if s2 == "" {
		t.Errorf("c.insertId is empty")
	}
	if s1 == s2 {
		t.Errorf("c.insertId returns same values")
	}
}

func TestClientSendRecord(t *testing.T) {
	w := &bufWriter{project: "p", dataset: "d", table: "t"}
	c := client{writer: w}

	v := map[string]interface{}{"foo": "hoge", "bar": 123, "baz": true}
	err := c.sendRecord(v)
	if err != nil {
		t.Errorf("%v", err)
	}

	if len(w.rows) != 1 {
		t.Errorf("w.rows is invalid")
	} else {
		r := w.rows[0]
		if r == nil {
			t.Errorf("w.rows is invalid")
		} else if r.insertId == "" {
			t.Errorf("row.insertId is empty")
		} else if r.value == nil {
			t.Errorf("row.value is empty")
		} else if r.value["foo"] != "hoge" {
			t.Errorf("row.value is invalid")
		} else if r.value["bar"] != 123 {
			t.Errorf("row.value is invalid")
		} else if r.value["baz"] != true {
			t.Errorf("row.value is invalid")
		}
	}
}

func TestClientSendLine(t *testing.T) {
	w := &bufWriter{project: "p", dataset: "d", table: "t"}
	c := client{writer: w}
	err := c.sendLine(`{"foo": "hoge", "bar": 123, "baz": true}`)
	if err != nil {
		t.Errorf("%v", err)
	}

	if len(w.rows) != 1 {
		t.Errorf("w.rows is invalid")
	} else {
		r := w.rows[0]
		if r == nil {
			t.Errorf("w.rows is invalid")
		} else if r.insertId == "" {
			t.Errorf("row.insertId is empty")
		} else if r.value == nil {
			t.Errorf("row.value is empty")
		} else if r.value["foo"] != "hoge" {
			t.Errorf("row.value is invalid")
		} else if r.value["bar"] != 123.0 {
			t.Errorf("row.value is invalid")
		} else if r.value["baz"] != true {
			t.Errorf("row.value is invalid")
		}
	}
}

func TestClientSendLineInvalidLine(t *testing.T) {
	w := &bufWriter{project: "p", dataset: "d", table: "t"}
	c := client{writer: w}
	err := c.sendLine(`{"foo": "hoge", "bar": 123, "baz": true},`)
	if err == nil {
		t.Errorf("%v", err)
	}
	if len(w.rows) != 0 {
		t.Errorf("w.rows is invalid")
	}
}

func TestClientHandleLine(t *testing.T) {
	w := &bufWriter{project: "p", dataset: "d", table: "t"}
	c := client{writer: w}
	c.handleLine(`{"foo": "hoge", "bar": 123, "baz": true}`)

	if len(w.rows) != 1 {
		t.Errorf("w.rows is invalid")
	} else {
		r := w.rows[0]
		if r == nil {
			t.Errorf("w.rows is invalid")
		} else if r.insertId == "" {
			t.Errorf("row.insertId is empty")
		} else if r.value == nil {
			t.Errorf("row.value is empty")
		} else if r.value["foo"] != "hoge" {
			t.Errorf("row.value is invalid")
		} else if r.value["bar"] != 123.0 {
			t.Errorf("row.value is invalid")
		} else if r.value["baz"] != true {
			t.Errorf("row.value is invalid")
		}
	}
}

func TestClientHandlePipes(t *testing.T) {
	w := &bufWriter{project: "p", dataset: "d", table: "t"}
	c := client{writer: w}

	var sout bytes.Buffer
	var serr bytes.Buffer

	initLogger("DEBUG")

	c.handlePipes(&sout, &serr)
	sout.WriteString(`{"foo": "hoge", "bar": 123, "baz": true}` + "\n")
	time.Sleep(1 * time.Nanosecond) // to start goroutines in the client

	if len(w.rows) != 1 {
		t.Errorf("w.rows is invalid")
	} else {
		r := w.rows[0]
		if r == nil {
			t.Errorf("w.rows is invalid")
		} else if r.insertId == "" {
			t.Errorf("row.insertId is empty")
		} else if r.value == nil {
			t.Errorf("row.value is empty")
		} else if r.value["foo"] != "hoge" {
			t.Errorf("row.value is invalid")
		} else if r.value["bar"] != 123.0 {
			t.Errorf("row.value is invalid")
		} else if r.value["baz"] != true {
			t.Errorf("row.value is invalid")
		}
	}
}
