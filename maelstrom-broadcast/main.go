package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

func main() {
	observed := make(map[any]bool)
	topology := make([]string, 0)
	state := make([]any, 0)

	node := maelstrom.NewNode()
	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if len(topology) == 0 {
			topology = node.NodeIDs()
		}

		message := body["message"]
		if _, ok := observed[message]; ok {
			// Observed the message already
			return nil
		}

		observed[message] = true

		for _, n := range topology {
			if msg.Src == n {
				continue
			}

			node.Send(n, body)
		}

		state = append(state, body["message"])

		body["type"] = "broadcast_ok"
		delete(body, "message")

		return node.Reply(msg, body)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "read_ok"
		body["messages"] = state

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
