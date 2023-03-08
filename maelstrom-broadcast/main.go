package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
	"time"
)

type state struct {
	mu     sync.Mutex
	values []any
}

type sendMessage struct {
	dest    string
	message any
}

func main() {
	topology := make([]string, 0)
	s := &state{values: make([]any, 0)}
	node := maelstrom.NewNode()
	
	node.Handle("broadcast", func(msg maelstrom.Message) error {
		s.mu.Lock()
		defer s.mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if len(topology) == 0 {
			topology = node.NodeIDs()
		}

		message := body["message"]
		if contains(s.values, message) {
			return nil
		}

		s.values = append(s.values, body["message"])

		for _, n := range topology {
			if msg.Src == n || n == node.ID() {
				continue
			}

			go sendWithRetry(node, sendMessage{n, body})
		}

		return node.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "read_ok"
		body["messages"] = s.values

		return node.Reply(msg, body)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if top, ok := body["topology"].(map[string]any); ok {
			if topology, ok = top[node.ID()].([]string); !ok {
				topology = node.NodeIDs()
			}
		}
		body["type"] = "topology_ok"
		delete(body, "topology")

		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func sendWithRetry(node *maelstrom.Node, message sendMessage) {
	acked := false
	node.RPC(message.dest, message.message, func(msg maelstrom.Message) error {
		acked = true
		return nil
	})
	time.Sleep(time.Millisecond * 50)
	if !acked {
		sendWithRetry(node, message)
	}
}

func contains[A comparable](collection []A, element A) bool {
	for _, e := range collection {
		if e == element {
			return true
		}
	}

	return false
}
