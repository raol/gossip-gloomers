package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

func main() {
	node := maelstrom.NewNode()
	storage := NewStorage(node)

	node.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		value := int(body["msg"].(float64))
		offset := storage.Append(key, value)

		return node.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := body["offsets"].(map[string]any)
		messages := make(map[string][][]int)

		for k, v := range offsets {
			messages[k] = storage.GetFromOffset(k, int(v.(float64)))
		}

		return node.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": messages,
		})
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := body["offsets"].(map[string]any)

		for k, v := range offsets {
			storage.CommitOffset(k, int(v.(float64)))
		}

		return node.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		keys := body["keys"].([]any)

		response := map[string]int{}
		for _, v := range keys {
			key := v.(string)
			response[key] = storage.GetCommittedOffset(key)
		}

		return node.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": response,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
