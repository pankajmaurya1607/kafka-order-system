package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"kafka-order-system/internal/models"

	KafkaGo "github.com/segmentio/kafka-go"
)

func main() {
	
	reader := KafkaGo.NewReader(KafkaGo.ReaderConfig {
		Brokers: 	[]string{"localhost:9092"},
		Topic: 		"orders-created",
		GroupID: 	"inventory-group",
	})

	defer reader.Close()

	log.Printf("Inventory consumer started")

	for {
		message, err := reader.ReadMessage(context.Background())

		if err != nil {
			log.Printf("error reading message: %v", err)
			return 
		}

		var event models.OrderCreateEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Printf("invlid event: %v", err)
			continue
		}

		log.Printf(
			"Processing inventory for order=%s product=%s quantity=%d",
			event.OrderID,
			event.ProductID,
			event.Quantity,
		)

		err = writeInventoryLog(event)

		if err != nil {
			log.Printf("failed inventory processing: % v", err)
			continue
		}
	}
}


func writeInventoryLog(event models.OrderCreateEvent) error {
	file, err := os.OpenFile(
		"logs/inventory.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)

	if err != nil {
		return err
	}

	defer file.Close()

	_, err = file.WriteString(
		"Inventory reserved for order " + event.OrderID + "\n",
	)

	return err
}