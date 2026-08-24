package main

import (
	"encoding/json"
	"kafka-order-system/internal/kafka"
	"kafka-order-system/internal/models"
	"log"
	"net/http"
	"time"
)

type CreateOrderRequest struct {
	UserId    string `json:"user_id"`
	ProductId string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

func main() {
	produce := kafka.NewProducer()
	defer produce.Close()

	http.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req CreateOrderRequest

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}

		order := models.OrderCreateEvent{
			Event:     "OrderCreated",
			OrderID:   generatedOrderID(),
			UserID:    req.UserId,
			ProductID: req.ProductId,
			Quantity:  req.Quantity,
		}

		if err := produce.PublishOrder(r.Context(), order); err != nil {
			log.Printf("Failed to publish event: %v", err)

			http.Error(w, "failed to create order", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		json.NewEncoder(w).Encode(order)
	})

	log.Println("Order API running on :8080")

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func generatedOrderID() string {
	return "ord-" + time.Now().Format("20060102150405.000000000")
}
