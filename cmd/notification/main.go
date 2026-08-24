package main

import (
	"context"
	"encoding/json"
	"kafka-order-system/internal/models"
	"log"
	"os"

	KafkaGo "github.com/segmentio/kafka-go"
)

func main() {

	reader := KafkaGo.NewReader(KafkaGo.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders.created",
		GroupID: "notification-group",
	})

	defer reader.Close()

	log.Printf("Notification consumer started")

	for {
		message, err := reader.ReadMessage(context.Background())

		if err != nil {
			log.Printf("error reading message: %v", err)
			continue
		}

		var event models.OrderCreateEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Printf("invalid event: %v", err)
			continue
		}

		log.Printf(
			"Sending notification for order=%s, user=%s",
			event.OrderID,
			event.UserID,
		)

		err = writeNotificationLog(event)

		if err != nil {
			log.Printf("notification failed: %v", err)
			return
		}
	}
}

func writeNotificationLog(event models.OrderCreateEvent) error {
	file, err := os.OpenFile(
		"logs/notification.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)

	if err != nil {
		return err
	}

	defer file.Close()

	_, err = file.WriteString(
		"Notification sent for order " + event.OrderID + "\n",
	)

	return err
}
