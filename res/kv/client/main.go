package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("usage: go run main.go <validation_file>\n")
		return
	}
	file := os.Args[1]
	baseURL := "http://localhost:8080"

	steps, err := readSteps(file)
	if err != nil {
		fmt.Printf("readSteps error: %v\n", err)
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}

	for _, st := range steps {
		switch st.method {
		case "PUT":
			req, err := http.NewRequest(http.MethodPut, baseURL+st.path, strings.NewReader(st.payload))
			if err != nil {
				fmt.Printf("line %d: %s | error: %v\n", st.lineNo, st.raw, err)
				continue
			}
			req.Header.Set("Content-Type", "text/plain; charset=utf-8")

			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("line %d: %s | error: %v\n", st.lineNo, st.raw, err)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				fmt.Printf("line %d: %s | status: %s\n", st.lineNo, st.raw, resp.Status)
				continue
			}
			if string(body) != st.payload {
				fmt.Printf("line %d: %s | mismatch: echoed %q\n", st.lineNo, st.raw, string(body))
			}

		case "GET":
			req, err := http.NewRequest(http.MethodGet, baseURL+st.path, nil)
			if err != nil {
				fmt.Printf("line %d: %s | error: %v\n", st.lineNo, st.raw, err)
				continue
			}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("line %d: %s | error: %v\n", st.lineNo, st.raw, err)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				fmt.Printf("line %d: %s | status: %s\n", st.lineNo, st.raw, resp.Status)
				continue
			}
			got := string(body)
			if st.payload != "" && got != st.payload {
				fmt.Printf("line %d: %s | expected %q, got %q\n", st.lineNo, st.raw, st.payload, got)
			}

		default:
			fmt.Printf("line %d: %s | unsupported method: %s\n", st.lineNo, st.raw, st.method)
		}
	}
}

type step struct {
	method  string
	path    string
	payload string
	lineNo  int
	raw     string
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

// Expects lines like:
//
//	PUT /key value
//	GET /key expected
func parseLine(line string) (step, error) {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return step{}, fmt.Errorf("need METHOD and PATH")
	}
	method := strings.ToUpper(parts[0])
	path := parts[1]

	payload := ""
	if len(parts) > 2 {
		payload = strings.Join(parts[2:], " ")
	}

	return step{method: method, path: path, payload: payload}, nil
}
