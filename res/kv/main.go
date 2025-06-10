package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
)

var (
	week    = flag.Int("week", 1, "Week number")
	clients = flag.Int("clients", 10, "Number of clients")
	url     = flag.String("url", "localhost:8080", "Server URL")
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func week1(ctx context.Context, client Client) error {
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < *clients; i++ {
		eg.Go(func() error {
			for j := 0; j < 1_000; j++ {
				key := fmt.Sprintf("key-%d-%d", i, j)

				res, code, err := client.get(key)
				if err := assertResponseStatus(res, code, err, "", http.StatusNotFound); err != nil {
					return fmt.Errorf("non-existing key: %w", err)
				}

				code, err = client.put(key, "foo")
				if err := assertStatus(code, err, http.StatusOK); err != nil {
					return fmt.Errorf("creating a new key: %w", err)
				}

				res, code, err = client.get(key)
				if err := assertResponseStatus(res, code, err, "foo", http.StatusOK); err != nil {
					return fmt.Errorf("existing key: %w", err)
				}
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed: %v", err)
	}
	return nil
}

func assertResponseStatus(gotResponse string, gotStatusCode int, gotErr error, expectedResponse string, expectedStatusCode int) error {
	if gotErr != nil {
		return gotErr
	}
	if gotStatusCode != expectedStatusCode {
		return fmt.Errorf("expect %d, got %d", expectedStatusCode, gotStatusCode)
	}
	if gotResponse != expectedResponse {
		return fmt.Errorf("expect %s, got %s", expectedResponse, gotResponse)
	}
	return nil
}

func assertStatus(gotStatusCode int, gotErr error, expectedStatusCode int) error {
	if gotErr != nil {
		return gotErr
	}
	if gotStatusCode != expectedStatusCode {
		return fmt.Errorf("expect %d, got %d", expectedStatusCode, gotStatusCode)
	}
	return nil
}

func run() error {
	flag.Parse()

	client := Client{
		client: &http.Client{},
		url:    fmt.Sprintf("http://%s/", *url),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch *week {
	default:
		return fmt.Errorf("unknown week: %d", *week)
	case 1:
		return week1(ctx, client)
	}
}
