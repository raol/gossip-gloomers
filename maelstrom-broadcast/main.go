package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
	"time"
)

type state struct {
	mu       sync.Mutex
	values   []any
	topology []string
}

type gossipState struct {
	mu          sync.Mutex
	outstanding map[string][]any
}

type sendMessage struct {
	dest    string
	message any
}

func main() {
	s := &state{
		values:   make([]any, 0),
		topology: make([]string, 0),
	}

	gs := &gossipState{
		outstanding: make(map[string][]any),
	}

	node := maelstrom.NewNode()

	timer := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-timer.C:
				gs.mu.Lock()
				for k, _ := range gs.outstanding {
					go nodeGossip(node, k, gs)
				}
				gs.mu.Unlock()
			}
		}
	}()

	node.Handle("gossip", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		gs.mu.Lock()
		defer gs.mu.Unlock()

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if gossip, ok := body["message"].([]any); ok {
			for _, e := range gossip {
				if !contains(s.values, e) {
					log.Printf("Processing value %s", e)
					s.values = append(s.values, e)
					for k, v := range gs.outstanding {
						gs.outstanding[k] = append(v, e)
					}
				}

			}
		} else {
			log.Printf("Failed to parse gossip body")
		}

		return node.Reply(msg, map[string]any{
			"type": "gossip_ok",
		})
	})

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		s.mu.Lock()
		gs.mu.Lock()
		defer s.mu.Unlock()
		defer gs.mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if len(s.topology) == 0 {
			s.topology = node.NodeIDs()
		}

		message := body["message"]
		if contains(s.values, message) {
			return nil
		}

		s.values = append(s.values, body["message"])
		for k, v := range gs.outstanding {
			gs.outstanding[k] = append(v, body["message"])
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
		s.mu.Lock()
		defer s.mu.Unlock()
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var topology []any
		if top, ok := body["topology"].(map[string]any); ok {
			if topology, ok = top[node.ID()].([]any); ok {
				for _, t := range topology {
					s.topology = append(s.topology, t.(string))
				}
			} else {
				s.topology = node.NodeIDs()
			}
		}

		gs.mu.Lock()
		for _, n := range s.topology {
			gs.outstanding[n] = make([]any, 0)
		}
		defer gs.mu.Unlock()

		return node.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func nodeGossip(node *maelstrom.Node, dest string, state *gossipState) {
	state.mu.Lock()
	values := state.outstanding[dest]
	state.mu.Unlock()

	if len(values) == 0 {
		return
	}

	body := map[string]any{
		"type":    "gossip",
		"message": values,
	}

	reply := make(chan any)
	go func() {
		node.RPC(dest, body, func(msg maelstrom.Message) error {
			reply <- true
			return nil
		})
	}()

	select {
	case <-reply:
		state.mu.Lock()
		for _, e := range values {
			state.outstanding[dest] = remove(state.outstanding[dest], e)
		}
		state.mu.Unlock()
	case <-time.After(500 * time.Millisecond):
		// bail out
		return
	}
}

func sendWithRetry(node *maelstrom.Node, message sendMessage) {
	c := make(chan any)

	go func() {
		node.RPC(message.dest, message.message, func(msg maelstrom.Message) error {
			c <- true
			return nil
		})
	}()

	select {
	case <-c:
		return
	case <-time.After(1 * time.Second):
		sendWithRetry(node, message)
	}

}

func remove[A comparable](collection []A, element A) []A {
	index := -1
	for i, e := range collection {
		if e == element {
			index = i
		}
	}

	if index == -1 {
		return collection
	}

	return append(collection[:index], collection[index+1:]...)
}

func contains[A comparable](collection []A, element A) bool {
	for _, e := range collection {
		if e == element {
			return true
		}
	}

	return false
}
