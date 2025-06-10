package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

const maxRetries = 5

type Client struct {
	client *http.Client
	url    string
}

func (c *Client) get(path string) (string, int, error) {
	resp, err := c.request(http.MethodGet, path, "")
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode, nil
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("failed to read response body: %w", err)
	}

	return string(b), resp.StatusCode, nil
}

func (c *Client) post(path string, body any) (int, error) {
	res, err := c.request(http.MethodPost, path, "")
	return res.StatusCode, err
}

func (c *Client) put(path, body string) (int, error) {
	res, err := c.request(http.MethodPut, path, body)
	return res.StatusCode, err
}

func (c *Client) delete(path string) (int, error) {
	res, err := c.request(http.MethodDelete, path, "")
	return res.StatusCode, err
}

func (c *Client) request(method, path string, body string) (*http.Response, error) {
	for i := 0; ; i++ {
		buf := bytes.NewBufferString(body)

		req, err := http.NewRequest(method, c.url+path, buf)
		if err != nil {
			return nil, fmt.Errorf("request creation failed: %w", err)
		}

		req.Header.Set("Content-Type", "text/plain")
		resp, err := c.client.Do(req)
		if err != nil {
			if i >= maxRetries {
				return nil, err
			}
			continue
		}
		if resp.StatusCode >= 500 && resp.StatusCode < 600 {
			if i >= maxRetries {
				return nil, fmt.Errorf("wrong status code: %d", resp.StatusCode)
			}
			continue
		}

		return resp, nil
	}
}
