package main

import (
	"bufio"
	_ "embed"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

//go:embed input.txt
var inputData string

func main() {
	baseURL := "http://127.0.0.1:8080"

	steps, err := readSteps(inputData)
	if err != nil {
		fmt.Printf("readSteps error: %v\n", err)
		return
	}

	transport := &http.Transport{
		// Keep-alives ON (default): reuse up to 100 conns to this host
		MaxConnsPerHost:       100,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       30 * time.Second,
		DisableCompression:    true,
		ResponseHeaderTimeout: 15 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 0,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
	}
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, 100) // cap to 100 in-flight requests

	flush := func() { wg.Wait() }

	currKind := "" // "", "PUT", "GET"
	for _, st := range steps {
		if st.kind == "BARRIER" {
			flush()
			currKind = ""
			continue
		}
		// Don’t mix GETs with later PUTs in the same in-flight batch
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
		default:
			<-sem
			wg.Done()
			fmt.Printf("line %d: %s | unsupported instruction: %s\n", st.lineNo, st.raw, st.kind)
		}
	}
	flush()
}

type step struct {
	kind     string // "PUT", "GET", or "BARRIER"
	path     string
	payload  string
	expected []string
	lineNo   int
	raw      string
}

func readSteps(data string) ([]step, error) {
	var out []step
	sc := bufio.NewScanner(strings.NewReader(data))
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

// Accepted lines: PUT /key value | GET /key exp1 [exp2 ...] | BARRIER
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
	path := parts[1]
	switch cmd {
	case "PUT":
		payload := ""
		if len(parts) > 2 {
			payload = strings.Join(parts[2:], " ")
		}
		return step{kind: "PUT", path: path, payload: payload}, nil
	case "GET":
		var exp []string
		if len(parts) > 2 {
			exp = parts[2:]
		}
		return step{kind: "GET", path: path, expected: exp}, nil
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

// Retry transient network/5xx errors (including "read: socket is not connected")
func doWithRetry(client *http.Client, req *http.Request) (*http.Response, error) {
	backoffs := []time.Duration{25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}
	var lastErr error
	for attempt := 0; attempt < 1+len(backoffs); attempt++ {
		// Clone the request each attempt
		resp, err := client.Do(req.Clone(req.Context()))
		if err == nil && (resp.StatusCode < 500 || resp.StatusCode >= 600) {
			return resp, nil
		}
		if err == nil {
			// 5xx: drain & close before retrying
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("server %s", resp.Status)
		} else {
			lastErr = err
			// Tiny sleep for transient network glitches
		}
		if attempt == len(backoffs) {
			break
		}
		time.Sleep(backoffs[attempt])
	}
	return nil, lastErr
}
