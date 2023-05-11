package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

func main() {
	node := maelstrom.NewNode()
	storage := NewStorage()

	node.Handle("txn", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgId := body["msg_id"]
		ops := body["txn"].([]any)
		result := storage.Apply(ops)
		return node.Reply(msg, map[string]any{
			"type":        "txn_ok",
			"in_reply_to": msgId,
			"txn":         result,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
