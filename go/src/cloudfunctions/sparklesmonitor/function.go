package pubsublistener

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
)

func init() {
	functions.CloudEvent("HandlePubSub", handlePubSub)
}

// PubSubMessage is the payload of a Pub/Sub CloudEvent.
type PubSubMessage struct {
	Message struct {
		Attributes  map[string]string `json:"attributes"`
		Data        string            `json:"data"`
		MessageID   string            `json:"messageId"`
		PublishTime time.Time         `json:"publishTime"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

func handlePubSub(ctx context.Context, e event.Event) error {
	var msg PubSubMessage
	if err := e.DataAs(&msg); err != nil {
		return fmt.Errorf("event.DataAs: %w", err)
	}

	// Decode base64 data field
	decoded, err := base64.StdEncoding.DecodeString(msg.Message.Data)
	if err != nil {
		log.Printf("Warning: could not decode data as base64: %v", err)
		decoded = []byte(msg.Message.Data)
	}

	// Log structured event details for debugging
	log.Printf("=== Pub/Sub Event ===")
	log.Printf("Message ID:    %s", msg.Message.MessageID)
	log.Printf("Publish Time:  %s", msg.Message.PublishTime)
	log.Printf("Subscription:  %s", msg.Subscription)
	log.Printf("Data (raw):    %s", string(decoded))
	log.Printf("Attributes:    %v", msg.Message.Attributes)

	// Also log as JSON for easy parsing in Cloud Logging
	jsonBytes, _ := json.Marshal(map[string]interface{}{
		"messageId":    msg.Message.MessageID,
		"publishTime":  msg.Message.PublishTime,
		"subscription": msg.Subscription,
		"data":         string(decoded),
		"attributes":   msg.Message.Attributes,
	})
	log.Printf("event_json: %s", string(jsonBytes))

	return nil
}
