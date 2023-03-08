package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
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
	c := make(chan sendMessage)
	node := maelstrom.NewNode()

	defer close(c)

	go func() {
		for {
			select {
			case msg := <-c:
				err := node.Send(msg.dest, msg.message)
				if err != nil {
					go func() {
						c <- msg
					}()
				}
			}
		}
	}()

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
			if msg.Src == n {
				continue
			}

			c <- sendMessage{dest: n, message: body}
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

func contains[A comparable](collection []A, element A) bool {
	for _, e := range collection {
		if e == element {
			return true
		}
	}

	return false
}
