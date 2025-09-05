package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/go-chi/chi/v5"
)

type entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type server struct {
	mu       sync.Mutex
	mem      map[string]string
	putCount int
	sstFiles []string // order as listed in MANIFEST (oldest -> newest)
}

const manifestName = "MANIFEST"

func newServer() *server {
	s := &server{
		mem:      make(map[string]string),
		sstFiles: []string{},
	}
	s.loadManifest()
	return s
}

func (s *server) singleThread(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func main() {
	s := newServer()

	r := chi.NewRouter()
	r.Use(s.singleThread)

	r.Get("/{key}", s.getHandler)
	r.Put("/{key}", s.putHandler)

	_ = http.ListenAndServe(":8080", r)
}

func (s *server) getHandler(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")

	// 1) Memtable
	if v, ok := s.mem[key]; ok {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(v))
		return
	}

	// 2) SSTables via MANIFEST: newest -> oldest
	for i := len(s.sstFiles) - 1; i >= 0; i-- {
		fn := s.sstFiles[i]
		if v, found := lookupSST(fn, key); found {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(v))
			return
		}
	}

	http.Error(w, "key not found", http.StatusNotFound)
}

func (s *server) putHandler(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	defer r.Body.Close()

	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "unable to read body", http.StatusBadRequest)
		return
	}
	val := string(b)

	s.mem[key] = val
	s.putCount++

	if s.putCount >= 100 {
		if err := s.flush(); err != nil {
			http.Error(w, "flush error: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(val))
}

// flush: write memtable to new sst-###.json (keys sorted), append filename to MANIFEST.
func (s *server) flush() error {
	if len(s.mem) == 0 {
		s.putCount = 0
		return nil
	}

	// determine next index from last entry in MANIFEST
	nextIdx := 1
	if n := len(s.sstFiles); n > 0 {
		if idx, ok := parseSSTIndex(s.sstFiles[n-1]); ok {
			nextIdx = idx + 1
		}
	}
	filename := fmt.Sprintf("sst-%03d.json", nextIdx)

	// build sorted entries
	keys := make([]string, 0, len(s.mem))
	for k := range s.mem {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ents := make([]entry, 0, len(keys))
	for _, k := range keys {
		ents = append(ents, entry{Key: k, Value: s.mem[k]})
	}

	// write SST
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(ents); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// append to MANIFEST
	mf, err := os.OpenFile(manifestName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := mf.WriteString(filename + "\n"); err != nil {
		mf.Close()
		return err
	}
	if err := mf.Close(); err != nil {
		return err
	}

	// update in-memory state
	s.sstFiles = append(s.sstFiles, filename)
	s.mem = make(map[string]string)
	s.putCount = 0
	return nil
}

// loadManifest reads MANIFEST (if present) and records SST order.
func (s *server) loadManifest() {
	f, err := os.Open(manifestName)
	if err != nil {
		return // no manifest yet
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		name := strings.TrimSpace(line)
		if name == "" {
			continue
		}
		// keep only valid "sst-###.json" lines
		if _, ok := parseSSTIndex(name); ok {
			// optionally, ensure file exists
			if _, err := os.Stat(name); err == nil {
				s.sstFiles = append(s.sstFiles, name)
			}
		}
	}
}

func lookupSST(filename, key string) (string, bool) {
	f, err := os.Open(filename)
	if err != nil {
		return "", false
	}
	defer f.Close()

	var ents []entry
	if err := json.NewDecoder(f).Decode(&ents); err != nil {
		return "", false
	}
	for _, e := range ents {
		if e.Key == key {
			return e.Value, true
		}
	}
	return "", false
}

func parseSSTIndex(name string) (int, bool) {
	re := regexp.MustCompile(`^sst-(\d{3})\.json$`)
	m := re.FindStringSubmatch(strings.ToLower(strings.TrimSpace(name)))
	if len(m) != 2 {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	return n, true
}
