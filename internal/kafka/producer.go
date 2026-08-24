package kafka

import (
	"context"
	"encoding/json"
	"kafka-order-system/internal/models"

	kafkaGo "github.com/segmentio/kafka-go"
)


type Producer struct {
	writer *kafkaGo.Writer
}

func NewProducer() *Producer {
	return &Producer{
		writer: &kafkaGo.Writer{
			Addr:		kafkaGo.TCP("localhost:9092"),
			Topic: 		"orders.created",
			Balancer: 	&kafkaGo.Hash{},
		},
	}
}

func (p *Producer) PublishOrder (ctx context.Context, order models.OrderCreateEvent) error {	
	data, err := json.Marshal(order)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafkaGo.Message{
		Key:	[]byte(order.OrderID),
		Value: 	data,
	})
}


func (p* Producer) Close() error {
	return p.writer.Close()
}