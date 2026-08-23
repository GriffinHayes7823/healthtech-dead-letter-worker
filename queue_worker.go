package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"

	queueclient "github.com/infrai-examples/healthtech-dead-letter-worker/infrai"
)

const maxAttempts = 3

type job struct {
	Kind      string `json:"kind"`
	JobID     string `json:"job_id"`
	Operation string `json:"operation"`
	Attempts  int    `json:"attempts"`
	Reason    string `json:"reason,omitempty"`
}

type queueAPI interface {
	QueuePublish(context.Context, any, string) error
	QueueConsume(context.Context, int, int) ([]queueclient.Message, error)
	QueueAck(context.Context, string) error
}

func main() {
	queueName := os.Getenv("INFRAI_QUEUE")
	client, err := queueclient.New(os.Getenv("INFRAI_API_KEY"), queueName)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	switch command() {
	case "create":
		err = client.QueueCreate(ctx, queueName+"-create")
	case "publish":
		item := job{Kind: "health_job", JobID: "eligibility-1042", Operation: "unsupported", Attempts: 0}
		err = client.QueuePublish(ctx, item, queueName+"-eligibility-1042")
	case "work":
		err = runOnce(ctx, client)
	case "cleanup":
		err = client.QueueDelete(ctx)
	default:
		err = errors.New("command must be create, publish, work, or cleanup")
	}
	if err != nil {
		log.Fatal(err)
	}
}

func command() string {
	if len(os.Args) < 2 {
		return "work"
	}
	return os.Args[1]
}

func runOnce(ctx context.Context, queue queueAPI) error {
	messages, err := queue.QueueConsume(ctx, 10, 30)
	if err != nil {
		return err
	}
	for _, message := range messages {
		if err := processMessage(ctx, queue, message); err != nil {
			return fmt.Errorf("process %s: %w", message.MessageID, err)
		}
	}
	return nil
}

func processMessage(ctx context.Context, queue queueAPI, message queueclient.Message) error {
	var item job
	if err := json.Unmarshal(message.Payload, &item); err != nil {
		return moveToDeadLetter(ctx, queue, message.MessageID, job{Kind: "dead_letter", Reason: "invalid payload"})
	}
	if item.Kind == "dead_letter" {
		log.Printf("dead-letter observed job_id=%s reason=%q", item.JobID, item.Reason)
		return queue.QueueAck(ctx, message.MessageID)
	}
	if err := handleHealthJob(item); err == nil {
		log.Printf("completed job_id=%s", item.JobID)
		return queue.QueueAck(ctx, message.MessageID)
	} else if item.Attempts+1 >= maxAttempts {
		item.Kind = "dead_letter"
		item.Attempts++
		item.Reason = err.Error()
		return moveToDeadLetter(ctx, queue, message.MessageID, item)
	}

	item.Attempts++
	requestID := stableRequestID("retry", message.MessageID, item.Attempts)
	if err := queue.QueuePublish(ctx, item, requestID); err != nil {
		return err
	}
	return queue.QueueAck(ctx, message.MessageID)
}

func moveToDeadLetter(ctx context.Context, queue queueAPI, messageID string, item job) error {
	requestID := stableRequestID("dead-letter", messageID, item.Attempts)
	if err := queue.QueuePublish(ctx, item, requestID); err != nil {
		return err
	}
	return queue.QueueAck(ctx, messageID)
}

func handleHealthJob(item job) error {
	if item.JobID == "" {
		return errors.New("missing job identifier")
	}
	if item.Operation != "reconcile-eligibility" {
		return fmt.Errorf("unsupported health job operation %q", item.Operation)
	}
	return nil
}

func stableRequestID(prefix, messageID string, attempt int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", prefix, messageID, attempt)))
	return prefix + "-" + hex.EncodeToString(sum[:12])
}
