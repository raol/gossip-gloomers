package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

type lState struct {
	leaderId string
}

func main() {
	node := maelstrom.NewNode()
	storage := NewStorage()
	kv := maelstrom.NewLinKV(node)
	leaderState := &lState{}

	node.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if isLeader(leaderState, node, kv) {
			key := body["key"].(string)
			value := int(body["msg"].(float64))
			offset := storage.Append(key, value)
			return node.Reply(msg, map[string]any{
				"type":   "send_ok",
				"offset": offset,
			})
		}

		return passThrough(node, leaderState.leaderId, msg, body)
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if isLeader(leaderState, node, kv) {
			offsets := body["offsets"].(map[string]any)
			messages := make(map[string][][]int)

			for k, v := range offsets {
				messages[k] = storage.GetFromOffset(k, int(v.(float64)))
			}

			return node.Reply(msg, map[string]any{
				"type": "poll_ok",
				"msgs": messages,
			})
		}

		return passThrough(node, leaderState.leaderId, msg, body)
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if isLeader(leaderState, node, kv) {
			offsets := body["offsets"].(map[string]any)

			for k, v := range offsets {
				storage.CommitOffset(k, int(v.(float64)))
			}

			return node.Reply(msg, map[string]any{
				"type": "commit_offsets_ok",
			})
		}

		return passThrough(node, leaderState.leaderId, msg, body)

	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if isLeader(leaderState, node, kv) {
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
		}

		return passThrough(node, leaderState.leaderId, msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func passThrough(node *maelstrom.Node, dest string, msg maelstrom.Message, body any) error {
	resp, err := node.SyncRPC(context.Background(), dest, body)
	if err != nil {
		return err
	}

	return node.Reply(msg, resp.Body)
}

func isLeader(leaderState *lState, node *maelstrom.Node, storage *maelstrom.KV) bool {
	if leaderState.leaderId != "" {
		return leaderState.leaderId == node.ID()
	}

	err := storage.CompareAndSwap(context.Background(), "leader", node.ID(), node.ID(), true)
	lid, rErr := storage.Read(context.Background(), "leader")
	if rErr != nil {
		log.Print("Failed to get leader state", rErr)
	}

	leaderState.leaderId = lid.(string)

	return err == nil
}
