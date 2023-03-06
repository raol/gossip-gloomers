package main

import (
	"encoding/json"
	"github.com/antigloss/go/container/concurrent/queue"
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
	queue := queue.NewLockfreeQueue[sendMessage]()
	timer := time.NewTicker(1 * time.Millisecond)
	node := maelstrom.NewNode()

	go func() {
		for {
			<-timer.C
			for {
				value, ok := queue.Pop()
				if !ok {
					break
				}

				if err := node.Send(value.dest, value.message); err != nil {
					queue.Push(value)
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

			queue.Push(sendMessage{dest: n, message: body})
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

func send(node *maelstrom.Node, dest string, body any) {
	for {
		if err := node.Send(dest, body); err != nil {
			continue
		}

		break
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
