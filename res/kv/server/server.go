package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
)

type server struct {
	mu sync.Mutex
	kv map[string]string
}

func newServer() *server {
	return &server{kv: make(map[string]string)}
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
	r.Delete("/{key}", s.deleteHandler)

	_ = http.ListenAndServe(":8080", r)
}

func (s *server) getHandler(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	val, ok := s.kv[key]
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(val))
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
	if _, exists := s.kv[key]; exists {
		fmt.Println(len(s.kv))
	}
	s.kv[key] = val
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(val))
}

func (s *server) deleteHandler(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	if _, ok := s.kv[key]; !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	delete(s.kv, key)
	w.WriteHeader(http.StatusOK)
}
