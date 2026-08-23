package main

import (
	"context"
	"encoding/json"
	"testing"

	queueclient "github.com/infrai-examples/healthtech-dead-letter-worker/infrai"
)

type fakeQueue struct {
	published []job
	acked     []string
}

func (f *fakeQueue) QueuePublish(_ context.Context, payload any, _ string) error {
	f.published = append(f.published, payload.(job))
	return nil
}

func (f *fakeQueue) QueueConsume(context.Context, int, int) ([]queueclient.Message, error) {
	return nil, nil
}

func (f *fakeQueue) QueueAck(_ context.Context, messageID string) error {
	f.acked = append(f.acked, messageID)
	return nil
}

func TestThirdFailureMovesJobToDeadLetterBeforeAck(t *testing.T) {
	payload, err := json.Marshal(job{
		Kind: "health_job", JobID: "eligibility-1042", Operation: "unsupported", Attempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	queue := &fakeQueue{}

	if err := processMessage(context.Background(), queue, queueclient.Message{MessageID: "msg-7", Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if len(queue.published) != 1 || queue.published[0].Kind != "dead_letter" {
		t.Fatalf("published = %#v, want one dead-letter job", queue.published)
	}
	if len(queue.acked) != 1 || queue.acked[0] != "msg-7" {
		t.Fatalf("acked = %#v, want msg-7", queue.acked)
	}
}
