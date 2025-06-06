package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	projectID      = "myproject"
	consumeTopic   = "events"
	subscriptionID = consumeTopic + "-subid"
	publishTopic   = "events-processed"

	logger = watermill.NewStdLogger(
		true,  // debug
		false, // trace
	)
	// for ordering pubsub, use googlecloud.NewOrderingMarshaler and googlecloud.NewOrderingUnmarshaler
	marshaler = googlecloud.DefaultMarshalerUnmarshaler{}
)

type event struct {
	ID int `json:"id"`
}

type processedEvent struct {
	ProcessedID int       `json:"processed_id"`
	Time        time.Time `json:"time"`
}

func main() {
	publisher := createPublisher(projectID)

	subscriber := createSubscriber(projectID, subscriptionID)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(middleware.Recoverer)

	router.AddHandler(
		"handler_1",  // handler name, must be unique
		consumeTopic, // topic from which messages should be consumed
		subscriber,
		publishTopic, // topic to which messages should be published
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			consumedPayload := event{}
			err := json.Unmarshal(msg.Payload, &consumedPayload)
			if err != nil {
				return nil, err
			}

			log.Printf("received event %+v", consumedPayload)

			newPayload, err := json.Marshal(processedEvent{
				ProcessedID: consumedPayload.ID,
				Time:        time.Now(),
			})
			if err != nil {
				return nil, err
			}

			newMessage := message.NewMessage(watermill.NewUUID(), newPayload)

			return []*message.Message{newMessage}, nil
		},
	)

	// Simulate incoming events in the background
	go simulateEvents(publisher)

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}

/*
// definition of pubsub.DefaultPublishSettings
// Messages are batched and sent according to the topic's PublishSettings. Publish never blocks.
// if your publisher needs async batching, use the native Topic.Publish() instead of Watermill's publisher
// because Watermill's publisher is always synchronous and sends only one message at a time

var DefaultPublishSettings = PublishSettings{
	DelayThreshold: 10 * time.Millisecond,
	CountThreshold: 100,
	ByteThreshold:  1e6,
	Timeout:        60 * time.Second,

	BufferedByteLimit: 10 * MaxPublishRequestBytes,
	FlowControlSettings: FlowControlSettings{
		MaxOutstandingMessages: 1000,
		MaxOutstandingBytes:    -1,
		LimitExceededBehavior:  FlowControlIgnore,
	},

	EnableCompression:         false,
	CompressionBytesThreshold: 240,
}
*/
func createPublisher(projectID string) message.Publisher {
	publisher, err := googlecloud.NewPublisher(
		googlecloud.PublisherConfig{
			ProjectID:                 projectID,
			DoNotCreateTopicIfMissing: false,
			EnableMessageOrdering:     false,
			ConnectTimeout:            10 * time.Second,
			PublishTimeout:            5 * time.Second,
			PublishSettings:        &pubsub.DefaultPublishSettings,
			Marshaler: marshaler,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}
	return publisher
}

func createSubscriber(projectID, subscription string) message.Subscriber {
	subscriptionName := func(string) string {
		return subscription
	}
	ackDeadline := 20 * time.Second
	subscriber, err := googlecloud.NewSubscriber(
		googlecloud.SubscriberConfig{
			ProjectID:                        projectID,
			DoNotCreateTopicIfMissing:        false,
			DoNotCreateSubscriptionIfMissing: false,
			InitializeTimeout:                30 * time.Second,
			GenerateSubscriptionName:         subscriptionName,
			SubscriptionConfig: pubsub.SubscriptionConfig{
				RetainAckedMessages:   false,
				EnableMessageOrdering: false,
				AckDeadline:           ackDeadline,
				RetentionDuration:     24 * time.Hour,
				// expiration policy specifies the conditions for a subscription's expiration
				// use time.Duration(0) to indicate that the subscription should never expire
				ExpirationPolicy:      time.Duration(0),
				RetryPolicy: &pubsub.RetryPolicy{
					MinimumBackoff: 10 * time.Second,
					MaximumBackoff: 600 * time.Second,
				},
			},
			ReceiveSettings: pubsub.ReceiveSettings{
				// maximum number of unprocessed messages (unacked but not yet expired)
				// the server stops sending more messages when there are MaxOutstandingMessages unprocessed messages currently sent to the streaming pull client
				// it limits the number of concurrent callbacks of messages (each callback is executed in a new goroutine)
				// in this case, up to 10 unacked messages can be handled concurrently
				MaxOutstandingMessages: 10,
				// in the background, the client will automatically call modifyAckDeadline every AckDeadline seconds until MaxExtension has passed
				// after the time passes, the client assumes that you "lost" the message and stops extending
				MaxExtension:           ackDeadline,
				// when Synchronous is false, the more performant StreamingPull is used instead of Pull (unary grpc call)
				// must set ReceiveSettings.Synchronous to false to enable concurrency pulling of messages. Otherwise, NumGoroutines will be set to 1
				Synchronous:            false,
				// the number of goroutines spawned for pulling messages
				// in thise case, we have 16 goroutines for handling I/O work
				NumGoroutines:          16,
			},
			Unmarshaler: marshaler,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}
	return subscriber
}

// simulateEvents produces events that will be later consumed.
func simulateEvents(publisher message.Publisher) {
	i := 0
	for {
		e := event{
			ID: i,
		}

		payload, err := json.Marshal(e)
		if err != nil {
			panic(err)
		}

		err = publisher.Publish(consumeTopic, message.NewMessage(
			watermill.NewUUID(), // internal uuid of the message, useful for debugging
			payload,
		))
		if err != nil {
			panic(err)
		}

		i++

		time.Sleep(time.Second)
	}
}
