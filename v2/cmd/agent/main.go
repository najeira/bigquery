package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/najeira/bigquery/v2"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"time"
)

type bigqueryWriter interface {
	Add(string, string, string, string, map[string]interface{}) error
}

type dummyWriter struct {
}

func (w *dummyWriter) Add(project, dataset, table, insertId string, value map[string]interface{}) error {
	log.Printf("project=%s, dataset=%s, table=%s, insertId=%s, value=%v", project, dataset, table, insertId, value)
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
	go scan(errPipe, func(line string) { log.Println(line) })

	if err := cmd.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *client) handleLine(line string) {
	var value interface{}
	err := json.Unmarshal([]byte(line), &value)
	if err != nil {
		log.Println(err)
		return
	}

	switch record := value.(type) {
	case map[string]interface{}:
		err = c.send(record)
	default:
		err = fmt.Errorf("invalid line: %s", line)
	}

	if err != nil {
		log.Println(err)
	}
}

func (c *client) send(record map[string]interface{}) error {
	return c.writer.Add(c.project, c.dataset, c.table, c.insertId(), record)
}

func (c *client) insertId() string {
	chars := "abcdefghijklmnopqrstuvwxyz"
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

	flag.Parse()

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

	client := &client{
		project: *project,
		dataset: *dataset,
		table:   *table,
		writer:  &dummyWriter{},
	}

	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		go func() {
			err := client.startTail(file)
			if err != nil {
				log.Println(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println("done")
}
