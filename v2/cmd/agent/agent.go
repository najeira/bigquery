package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	//_ "github.com/najeira/bigquery/v2"
	"bigquery/v2"
	"goutils/nlog"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"time"
)

var logger nlog.Logger = nil

type bigqueryWriter interface {
	Add(string, map[string]interface{}) error
}

type dummyWriter struct {
	project string
	dataset string
	table   string
}

func (w *dummyWriter) Add(insertId string, value map[string]interface{}) error {
	logger.Debugf("project=%s, dataset=%s, table=%s, insertId=%s, value=%v", w.project, w.dataset, w.table, insertId, value)
	return nil
}

type client struct {
	project string
	dataset string
	table   string
	writer  bigqueryWriter
	comment rune
	buf     bytes.Buffer
}

func (c *client) startTail(file string) error {
	cmd := exec.Command("tail", "-n", "0", "-F", file)

	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	errPipe, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	go scan(outPipe, func(line string) { c.handleLine(line) })
	go scan(errPipe, func(line string) { logger.Warnf(line) })

	if err := cmd.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *client) handleLine(line string) {
	err := c.sendLine(line)
	if err != nil {
		logger.Warnf("%v", err)
	}
}

func (c *client) sendLine(line string) error {
	var value interface{}
	err := json.Unmarshal([]byte(line), &value)
	if err != nil {
		return err
	}

	switch record := value.(type) {
	case map[string]interface{}:
		return c.sendRecord(record)
	}

	return fmt.Errorf("invalid line: %s", line)
}

func (c *client) sendRecord(record map[string]interface{}) error {
	return c.writer.Add(c.insertId(), record)
}

func (c *client) insertId() string {
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	c.buf.Reset()
	for i := 0; i < 10; i++ {
		ch := chars[rand.Int()%len(chars)]
		c.buf.WriteRune(rune(ch))
	}
	return c.buf.String()
}

func scan(reader io.Reader, handler func(line string)) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		handler(scanner.Text())
	}
}

func errorFlag() {
	fmt.Println("Usage:")
	fmt.Println("  agent [options] files")
	fmt.Println("Options:")
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	project := flag.String("project", "", "project")
	dataset := flag.String("dataset", "", "dataset")
	table := flag.String("table", "", "table")
	email := flag.String("email", "", "email")
	pem := flag.String("pem", "", "pem")
	echo := flag.Bool("echo", false, "writes to stdout")
	logging := flag.String("log", "info", "logging")
	flag.Parse()

	logger = nlog.NewLogger(&nlog.Config{Level: nlog.NameToLevel(*logging)})

	if project == nil || *project == "" {
		errorFlag()
	} else if dataset == nil || *dataset == "" {
		errorFlag()
	} else if table == nil || *table == "" {
		errorFlag()
	} else if email == nil || *email == "" {
		errorFlag()
	} else if pem == nil || *pem == "" {
		errorFlag()
	}

	files := flag.Args()
	if len(files) <= 0 {
		errorFlag()
	}

	rand.Seed(time.Now().UnixNano())

	var writer bigqueryWriter
	if echo != nil && *echo {
		writer = &dummyWriter{project: *project, dataset: *dataset, table: *table}
	} else {
		bq := bigquery.NewWriter(&bigquery.Config{
			Project: *project,
			Dataset: *dataset,
			Table:   *table,
		})
		bq.SetLogger(logger)
		writer = bq
	}

	client := &client{writer: writer}

	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		go func() {
			err := client.startTail(file)
			if err != nil {
				logger.Errorf("%v", err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println("done")
}
