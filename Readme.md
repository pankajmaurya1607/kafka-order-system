## Project: Order Event Processing System


```text
Client
  |
  | POST /orders
  v
Order API
  |
  | publish OrderCreated
  v
Kafka
  |
  +--------------------+
  |                    |
  v                    v
Inventory Consumer    Notification Consumer
  |                    |
  v                    v
Inventory DB         Notification Log
```