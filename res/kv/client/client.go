package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("usage: go run main.go <validation_file>\n")
		return
	}
	file := os.Args[1]
	baseURL := "http://127.0.0.1:8080"

	steps, err := readSteps(file)
	if err != nil {
		fmt.Printf("readSteps error: %v\n", err)
		return
	}

	transport := &http.Transport{
		MaxConnsPerHost:       100,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       30 * time.Second,
		ForceAttemptHTTP2:     false,
		DisableCompression:    true,
		ResponseHeaderTimeout: 15 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 0,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	client := &http.Client{
		Timeout:   20 * time.Second,
		Transport: transport,
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, 1)

	flush := func() { wg.Wait() }

	currKind := "" // "", "PUT", "GET", "DELETE"
	for _, st := range steps {
		if st.kind == "BARRIER" {
			flush()
			currKind = ""
			continue
		}
		// Don't mix kinds in the same in-flight batch
		if currKind != "" && currKind != st.kind {
			flush()
			currKind = ""
		}
		if currKind == "" {
			currKind = st.kind
		}

		wg.Add(1)
		sem <- struct{}{}
		switch st.kind {
		case "PUT":
			go func(st step) {
				defer wg.Done()
				defer func() { <-sem }()
				doPUT(client, baseURL, st)
			}(st)
		case "GET":
			go func(st step) {
				defer wg.Done()
				defer func() { <-sem }()
				doGET(client, baseURL, st)
			}(st)
		case "DELETE":
			go func(st step) {
				defer wg.Done()
				defer func() { <-sem }()
				doDELETE(client, baseURL, st)
			}(st)
		default:
			<-sem
			wg.Done()
			fmt.Printf("line %d: %s | unsupported instruction: %s\n", st.lineNo, st.raw, st.kind)
		}
	}
	flush()
}

type step struct {
	kind           string // "PUT", "GET", "DELETE", or "BARRIER"
	path           string
	payload        string   // for PUT
	expected       []string // for GET (any match passes)
	expectNotFound bool     // for GET: expect 404
	lineNo         int
	raw            string
}

func readSteps(path string) ([]step, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []step
	sc := bufio.NewScanner(f)
	lineno := 0
	for sc.Scan() {
		lineno++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		st, err := parseLine(line)
		if err != nil {
			fmt.Printf("line %d: %s | parse error: %v\n", lineno, line, err)
			continue
		}
		st.lineNo = lineno
		st.raw = line
		out = append(out, st)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Accepted lines:
//
//	PUT key value
//	GET key expected1 [expected2 ...]
//	GET key NOT_FOUND
//	DELETE /key
//	BARRIER
func parseLine(line string) (step, error) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return step{}, fmt.Errorf("empty line")
	}
	cmd := strings.ToUpper(parts[0])
	if cmd == "BARRIER" {
		return step{kind: "BARRIER"}, nil
	}
	if len(parts) < 2 {
		return step{}, fmt.Errorf("need METHOD and PATH")
	}

	// normalize path so that "k" becomes "/k"
	path := parts[1]
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	switch cmd {
	case "PUT":
		payload := ""
		if len(parts) > 2 {
			payload = strings.Join(parts[2:], " ")
		}
		return step{kind: "PUT", path: path, payload: payload}, nil
	case "GET":
		if len(parts) > 2 && strings.ToUpper(parts[2]) == "NOT_FOUND" {
			return step{kind: "GET", path: path, expectNotFound: true}, nil
		}
		var exp []string
		if len(parts) > 2 {
			exp = parts[2:]
		}
		return step{kind: "GET", path: path, expected: exp}, nil
	case "DELETE":
		return step{kind: "DELETE", path: path}, nil
	default:
		return step{}, fmt.Errorf("unknown method %q", cmd)
	}
}

func doPUT(client *http.Client, baseURL string, st step) {
	req, err := http.NewRequest(http.MethodPut, baseURL+st.path, strings.NewReader(st.payload))
	if err != nil {
		fmt.Printf("line %d: %s | error: %v\n", st.lineNo, st.raw, err)
		return
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp, err := doWithRetry(client, req)
	if err != nil {
		fmt.Printf("line %d: %s | error: %v\n", st.lineNo, st.raw, err)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("line %d: %s | status: %s\n", st.lineNo, st.raw, resp.Status)
		return
	}
	if string(body) != st.payload {
		fmt.Printf("line %d: %s | mismatch: echoed %q\n", st.lineNo, st.raw, string(body))
	}
}

func doGET(client *http.Client, baseURL string, st step) {
	req, err := http.NewRequest(http.MethodGet, baseURL+st.path, nil)
	if err != nil {
		fmt.Printf("line %d: %s | error: %v\n", st.lineNo, st.raw, err)
		return
	}
	resp, err := doWithRetry(client, req)
	if err != nil {
		fmt.Printf("line %d: %s | error: %v\n", st.lineNo, st.raw, err)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if st.expectNotFound {
		if resp.StatusCode != http.StatusNotFound {
			fmt.Printf("line %d: %s | expected 404, got %s\n", st.lineNo, st.raw, resp.Status)
		}
		return
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("line %d: %s | status: %s\n", st.lineNo, st.raw, resp.Status)
		return
	}
	if len(st.expected) == 0 {
		return
	}
	got := string(body)
	for _, exp := range st.expected {
		if got == exp {
			return
		}
	}
	fmt.Printf("line %d: %s | expected one of %q, got %q\n", st.lineNo, st.raw, st.expected, got)
}

func doDELETE(client *http.Client, baseURL string, st step) {
	req, err := http.NewRequest(http.MethodDelete, baseURL+st.path, nil)
	if err != nil {
		fmt.Printf("line %d: %s | error: %v\n", st.lineNo, st.raw, err)
		return
	}
	resp, err := doWithRetry(client, req)
	if err != nil {
		fmt.Printf("line %d: %s | error: %v\n", st.lineNo, st.raw, err)
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// If you want to allow DELETE on a non-existent key, treat 404 as OK here
		fmt.Printf("line %d: %s | status: %s\n", st.lineNo, st.raw, resp.Status)
	}
}

// Small retry loop for transient network/5xx errors
func doWithRetry(client *http.Client, req *http.Request) (*http.Response, error) {
	backoffs := []time.Duration{25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}
	var lastErr error
	for attempt := 0; attempt < 1+len(backoffs); attempt++ {
		resp, err := client.Do(req.Clone(req.Context()))
		if err == nil && (resp.StatusCode < 500 || resp.StatusCode >= 600) {
			return resp, nil
		}
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("server %s", resp.Status)
		} else {
			lastErr = err
		}
		if attempt == len(backoffs) {
			break
		}
		time.Sleep(backoffs[attempt])
	}
	return nil, lastErr
}
