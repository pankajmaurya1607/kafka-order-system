package models

type OrderCreateEvent struct {
	Event 		string		`json:"event"`
	OrderID		string 		`json:"order_id"`
	UserID 		string		`json:"user_id"`
	ProductID   string 		`json:"product_id"`
	Quantity	int  		`json:"quantity"`
}