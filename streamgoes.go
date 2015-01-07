package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	_index string
	_type  string
	_host  string
	_path  string
	_files []string
	_limit uint64
)

func init() {
	flag.StringVar(&_index, "index", "", "index")
	flag.StringVar(&_type, "type", "", "type of index")
	flag.StringVar(&_host, "host", "", "elasticserver host")
	flag.StringVar(&_path, "path", "", "input files path(also regex)")
	flag.Uint64Var(&_limit, "limit", 10*1024*1024, "http body limit size")
	flag.Parse()
	_files, _ = filepath.Glob(_path)
}

func main() {
	reader, writer := io.Pipe()
	var size uint64
	var line int64 = 0

	var wg sync.WaitGroup
	req, e := http.NewRequest("POST", "http://"+_host+":9200/_bulk", reader)
	if e != nil {
		fmt.Println("Error : ", e)
		return
	}
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		client := &http.Client{}
		resp, e := client.Do(req)
		if e != nil {
			fmt.Println("http error : ", e)
		}
		fmt.Println("from elasticsearch : ", resp)
	}(&wg)

	// read speed
	go func() {
		for {
			interval := time.Tick(1 * time.Second) // interval
			select {
			case <-interval:
				fmt.Printf("processed %d line per sec\n", line)
				line = 0
			}
		}
	}()
	for _, f := range _files {
		fmt.Println("f : ", f)
		fp, err := os.Open(f)
		if err != nil {
			panic(err)
		}
		scanner := bufio.NewScanner(fp)
		for scanner.Scan() {
			if size > _limit {
				writer.Close()
				//	reset
				reader, writer = io.Pipe()
				size = 0

				// new http request
				req, e = http.NewRequest("POST", "http://"+_host+":9200/_bulk", reader)
				if e != nil {
					fmt.Println("Error : ", e)
					return
				}
				wg.Add(1)
				go func(wg *sync.WaitGroup, req *http.Request) {
					defer wg.Done()
					client := &http.Client{}
					resp, e := client.Do(req)
					if e != nil {
						fmt.Println("http error : ", e)
					}
					fmt.Println("from elasticsearch : ", resp)
				}(&wg, req)
			}
			// use auto generated id for performance
			fmt.Fprintf(writer, "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\" } }\n", _index, _type)
			fmt.Fprintln(writer, scanner.Text())
			size += uint64(len(scanner.Bytes()))
			line++
		}
		fp.Close()
	}
	writer.Close()
	wg.Wait()

	fmt.Printf("processed %d line per sec\n", line)
	fmt.Println("Done")
}
