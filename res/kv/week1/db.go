package main

import (
	"io"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	s := newServer()
	if err := http.ListenAndServe(":8080", s.router); err != nil {
		panic(err)
	}
}

type server struct {
	router chi.Router
	mu     sync.RWMutex
	db     map[string]string
}

func newServer() *server {
	s := &server{
		router: chi.NewRouter(),
	}
	s.routes()
	return s
}

func (s *server) routes() {
	s.router.Use(middleware.Logger)
	s.router.Use(middleware.Recoverer)

	s.router.Route("/{key}", func(r chi.Router) {
		r.Get("/", s.handleGetKey)
		r.Put("/", s.handlePutKey)
	})
}

func (s *server) handleGetKey(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	if key == "" {
		http.Error(w, "missing key in path", http.StatusBadRequest)
		return
	}

	s.mu.RLock()
	value, ok := s.db[key]
	s.mu.RUnlock()

	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(value))
}

func (s *server) handlePutKey(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	if key == "" {
		http.Error(w, "missing key in path", http.StatusBadRequest)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "unable to read body", http.StatusBadRequest)
		return
	}
	value := string(bodyBytes)

	s.mu.Lock()
	if s.db == nil {
		s.db = make(map[string]string)
	}
	s.db[key] = value
	s.mu.Unlock()

	w.WriteHeader(http.StatusOK)
}
