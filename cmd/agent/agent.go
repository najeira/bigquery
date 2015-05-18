package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/najeira/bigquery/v2"
	"github.com/najeira/goutils/nlog"
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

type echoWriter struct {
	project string
	dataset string
	table   string
}

func (w *echoWriter) Add(insertId string, value map[string]interface{}) error {
	if logger != nil {
		logger.Debugf("project=%s, dataset=%s, table=%s, insertId=%s, value=%v", w.project, w.dataset, w.table, insertId, value)
	}
	return nil
}

type client struct {
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

	c.handlePipes(outPipe, errPipe)

	if err := cmd.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *client) handlePipes(outPipe, errPipe io.Reader) {
	go scan(outPipe, func(line string) { c.handleLine(line) })
	go scan(errPipe, func(line string) {
		if logger != nil {
			logger.Warnf(line)
		}
	})
}

func (c *client) handleLine(line string) {
	err := c.sendLine(line)
	if err != nil {
		if logger != nil {
			logger.Warnf("%v", err)
		}
	}
}

func trimCrLf(line string) string {
	for len(line) > 0 {
		ch := line[len(line)-1]
		if ch != '\n' && ch != '\r' {
			return line
		}
		line = line[:len(line)]
	}
	return line
}

func (c *client) sendLine(line string) error {
	line = trimCrLf(line)

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

func startClient(writer bigqueryWriter, files []string) {
	client := &client{writer: writer}
	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		go func() {
			err := client.startTail(file)
			if err != nil {
				if logger != nil {
					logger.Errorf("%v", err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func initLogger(level string) {
	logger = nlog.NewLogger(&nlog.Config{Level: nlog.NameToLevel(level)})
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

	initLogger(*logging)

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
		writer = &echoWriter{project: *project, dataset: *dataset, table: *table}
	} else {
		bq := bigquery.NewWriter(bigquery.Config{
			Project: *project,
			Dataset: *dataset,
			Table:   *table,
		})
		bq.SetLogger(logger)
		writer = bq
	}

	startClient(writer, files)

	fmt.Println("done")
}
