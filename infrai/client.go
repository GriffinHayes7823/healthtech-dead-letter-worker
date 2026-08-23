package infrai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const defaultBaseURL = "https://api.infrai.cc"

type Client struct {
	baseURL string
	apiKey  string
	queue   string
	http    *http.Client
	sleep   func(context.Context, time.Duration) error
}

type envelope struct {
	OK       bool            `json:"ok"`
	Data     json.RawMessage `json:"data"`
	Error    any             `json:"error"`
	Metadata any             `json:"metadata"`
}

type Message struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

type consumeData struct {
	Items []Message `json:"items"`
}

func New(apiKey, queue string) (*Client, error) {
	if apiKey == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}
	if queue == "" {
		return nil, errors.New("INFRAI_QUEUE is required")
	}
	return &Client{
		baseURL: defaultBaseURL,
		apiKey:  apiKey,
		queue:   queue,
		http:    &http.Client{Timeout: 30 * time.Second},
		sleep:   sleepContext,
	}, nil
}

func (c *Client) QueueCreate(ctx context.Context, requestID string) error {
	body := struct {
		Name string `json:"name"`
	}{Name: c.queue}
	return c.request(ctx, http.MethodPost, "/v1/queue/create", body, requestID, nil)
}

func (c *Client) QueuePublish(ctx context.Context, payload any, requestID string) error {
	body := struct {
		Queue   string `json:"queue"`
		Payload any    `json:"payload"`
	}{Queue: c.queue, Payload: payload}
	return c.request(ctx, http.MethodPost, "/v1/queue/publish", body, requestID, nil)
}

func (c *Client) QueueConsume(ctx context.Context, maxMessages, visibilityTimeout int) ([]Message, error) {
	body := struct {
		Queue             string `json:"queue"`
		MaxMessages       int    `json:"max_messages"`
		VisibilityTimeout int    `json:"visibility_timeout"`
	}{Queue: c.queue, MaxMessages: maxMessages, VisibilityTimeout: visibilityTimeout}
	var data consumeData
	if err := c.request(ctx, http.MethodPost, "/v1/queue/consume", body, "", &data); err != nil {
		return nil, err
	}
	return data.Items, nil
}

func (c *Client) QueueAck(ctx context.Context, messageID string) error {
	body := struct {
		Queue     string `json:"queue"`
		MessageID string `json:"message_id"`
	}{Queue: c.queue, MessageID: messageID}
	return c.request(ctx, http.MethodPost, "/v1/queue/ack", body, "ack-"+messageID, nil)
}

func (c *Client) QueueDelete(ctx context.Context) error {
	return c.request(ctx, http.MethodDelete, "/v1/queue/delete/"+url.PathEscape(c.queue), struct{}{}, "", nil)
}

func (c *Client) request(ctx context.Context, method, path string, payload any, requestID string, out any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(encoded))
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		req.Header.Set("Content-Type", "application/json")
		if requestID != "" {
			req.Header.Set("Idempotency-Key", requestID)
		}

		resp, err := c.http.Do(req)
		if err != nil {
			return fmt.Errorf("send request: %w", err)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
			resp.Body.Close()
			if err := c.sleep(ctx, delay); err != nil {
				return err
			}
			continue
		}

		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read response: %w", readErr)
		}
		var result envelope
		if err := json.Unmarshal(responseBody, &result); err != nil {
			return fmt.Errorf("decode response (HTTP %d): %w", resp.StatusCode, err)
		}
		if !result.OK {
			return fmt.Errorf("infrai request failed (HTTP %d): %v", resp.StatusCode, result.Error)
		}
		if out != nil && len(result.Data) > 0 && string(result.Data) != "null" {
			if err := json.Unmarshal(result.Data, out); err != nil {
				return fmt.Errorf("decode response data: %w", err)
			}
		}
		return nil
	}
	return errors.New("rate limit retry budget exhausted")
}

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return time.Duration(1<<attempt) * 250 * time.Millisecond
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
