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

func createPublisher(projectID string) message.Publisher {
	publisher, err := googlecloud.NewPublisher(
		googlecloud.PublisherConfig{
			ProjectID:                 projectID,
			DoNotCreateTopicIfMissing: false,
			EnableMessageOrdering:     false,
			ConnectTimeout:            10 * time.Second,
			PublishTimeout:            5 * time.Second,
			// enable batching (which will increase latency)
			// PublishSettings:        &pubsub.DefaultPublishSettings,
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
				ExpirationPolicy:      time.Duration(0),
				RetryPolicy: &pubsub.RetryPolicy{
					MinimumBackoff: 10 * time.Second,
					MaximumBackoff: 600 * time.Second,
				},
			},
			ReceiveSettings: pubsub.ReceiveSettings{
				// maximum number of unprocessed message (unacked but not yet expired)
				MaxOutstandingMessages: 10,
				// in the background, the client will automatically call modifyAckDeadline until MaxExtension has passed
				// after the time passes, the client assumes that you "lost" the message and stops extending
				MaxExtension:           ackDeadline,
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
